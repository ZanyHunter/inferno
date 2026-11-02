use crate::{common::*, device_info::DeviceInfo};
use std::{
  collections::BTreeMap,
  error::Error,
  io,
  net::{Ipv4Addr, SocketAddr, SocketAddrV4},
  str::{self},
  sync::Arc,
  time::{Duration, Instant},
};

use searchfire::{
  discovery::DiscoveryBuilder,
  dns::{
    op::DnsResponse,
    rr::{Name, RecordType},
  },
  net::{IpVersion, TargetInterface},
};

pub type TxtEntries = BTreeMap<String, String>;

/// Total time a unicast-style lookup (`query`) waits for an answer, and how long each transmission of
/// the question gets before it is repeated. ONS fork (issue #131): a question sent once while the
/// link or the switch's multicast forwarding was not ready after a cable pull was simply lost, and
/// the caller then waited for its own retry timer (about 15 s in total).
const QUERY_TOTAL_TIMEOUT: Duration = Duration::from_secs(3);
const QUERY_RETRANSMIT_INTERVAL: Duration = Duration::from_secs(1);

/// Calls `attempt(timeout)` until it succeeds or `total` has passed. Each attempt gets at most
/// `per_try`; an attempt that fails sooner (for example because the send failed) is not repeated
/// before `per_try` has passed since it started, so a persistent error does not spin.
async fn retransmitting<T, E, F, Fut>(total: Duration, per_try: Duration, mut attempt: F) -> Result<T, E>
where
  F: FnMut(Duration) -> Fut,
  Fut: std::future::Future<Output = Result<T, E>>,
{
  let start = Instant::now();
  loop {
    let try_start = Instant::now();
    let remaining = total.saturating_sub(start.elapsed());
    let result = attempt(per_try.min(remaining)).await;
    if result.is_ok() || start.elapsed() >= total {
      return result;
    }
    let spent = try_start.elapsed();
    if spent < per_try {
      tokio::time::sleep((per_try - spent).min(total.saturating_sub(start.elapsed()))).await;
    }
    if start.elapsed() >= total {
      return result;
    }
  }
}

#[derive(Debug)]
pub struct AdvertisedService {
  pub addr: SocketAddr,
  pub properties: TxtEntries,
}

#[derive(Debug, Clone)]
pub struct PointerToMulticast {
  pub tx_hostname: String,
  pub bundle_id: usize,
  pub channel_in_bundle: usize,
}

#[derive(Debug, Clone)]
pub struct AdvertisedBundle {
  pub tx_channels_per_flow: usize,
  pub tx_bundle_id: u16,
  pub bits_per_sample: u32,
  pub fpp: u16,
  pub min_rx_latency_ns: usize,
  pub media_addr: SocketAddr,
}

#[derive(Debug, Clone)]
pub struct AdvertisedChannel {
  pub addr: SocketAddr,
  pub tx_channels_per_flow: usize,
  pub tx_channel_id: u16,
  pub bits_per_sample: u32,
  pub dbcp1: u16,
  /// minimum Frames Per Packet supported by transmitter (Frame = single samples of all channels)
  pub fpp_min: u16,
  /// maximum Frames Per Packet supported by transmitter (Frame = single samples of all channels)
  pub fpp_max: u16,
  pub min_rx_latency_ns: usize,
  pub multicast: Option<PointerToMulticast>,
}

pub fn self_origin_from_self_info(self_info: &DeviceInfo) -> Vec<u8> {
  (hex::encode(self_info.factory_device_id) + "," + &self_info.process_id.to_string()).into()
}

pub struct MdnsClient {
  listen_ip: Ipv4Addr,
  self_origin: Vec<u8>,
  self_info: Arc<DeviceInfo>,
}

impl MdnsClient {
  pub fn new(self_info: Arc<DeviceInfo>) -> Self {
    Self {
      listen_ip: self_info.ip_address,
      self_origin: self_origin_from_self_info(&self_info),
      self_info,
    }
  }
  async fn do_single_query(
    &self,
    types: &[RecordType],
    fqdn: &Name,
    timeout: Duration,
  ) -> Result<DnsResponse, Box<dyn Error>> {
    DiscoveryBuilder::new()
      .interface_v4(TargetInterface::Specific(self.listen_ip))
      .loopback()
      .build(IpVersion::V4)
      .map_err(|e| Box::new(e))?
      .single_query(types, fqdn, timeout, |origin_addr, response| {
        let is_local = match origin_addr.ip() {
          std::net::IpAddr::V4(addr) => addr.is_loopback() || addr == self.listen_ip,
          _ => false,
        };
        if is_local {
          let response_origin_fqdn = Name::from_labels(["_inferno-response-origin", "local"]).unwrap();
          for rr in response.additionals() {
            if rr.name() == &response_origin_fqdn {
              if let Some(data) = rr.data() {
                if let Some(txt) = data.as_txt() {
                  if let Some(data) = txt.txt_data().get(0) {
                    if data.eq_ignore_ascii_case(&self.self_origin) {
                      debug!("got answer from self, ignoring this response");
                      return false;
                    } else {
                      debug!("got local answer from other instance, considering");
                      return true;
                    }
                  }
                }
              }
            }
          }
          true
        } else {
          true
        }
      })
      .await
      .map_err(|e| Box::new(e).into())
  }
  /// [`do_single_query`](Self::do_single_query) with an error that can be held across an `await` in a
  /// spawned task (`Box<dyn Error>` cannot).
  async fn do_single_query_sendable(
    &self,
    types: &[RecordType],
    fqdn: &Name,
    timeout: Duration,
  ) -> Result<DnsResponse, String> {
    self.do_single_query(types, fqdn, timeout).await.map_err(|e| e.to_string())
  }
  async fn a_record_exists_single_check(
    &self,
    fqdn_parts: &[&str],
    timeout: Duration,
  ) -> Result<bool, Box<dyn Error>> {
    let fqdn = Name::from_labels(fqdn_parts.iter().map(|&s| s.as_bytes())).map_err(|e| Box::new(e))?;
    match self.do_single_query(&[RecordType::A], &fqdn, timeout).await {
      Ok(response) => {
        for record in response.answers() {
          if record.name().to_lowercase() == fqdn.to_lowercase() {
            return Ok(true);
          }
        }
        Ok(true)
      }
      Err(_e) => Ok(false), // TODO: sometimes error may be other than timeout, handle that
    }
  }
  pub async fn a_record_exists(&self, fqdn_parts: &[&str]) -> Result<bool, Box<dyn Error>> {
    debug!("checking if A record exists: {fqdn_parts:?}");
    for _ in 0..3 {
      let r = self.a_record_exists_single_check(fqdn_parts, Duration::from_millis(400)).await?;
      if r {
        return Ok(r);
      };
    }
    Ok(false)
  }
  pub async fn is_multicast_ip_already_used(&self, addr: Ipv4Addr) -> Result<bool, Box<dyn Error>> {
    let octets = addr.octets();
    let name = [
      &octets[3].to_string(),
      &octets[2].to_string(),
      &octets[1].to_string(),
      &octets[0].to_string(),
      "in-addr",
      "local",
    ];
    self.a_record_exists(&name).await
  }

  pub async fn query(&self, fqdn_parts: &[&str]) -> Result<AdvertisedService, Box<dyn Error>> {
    debug!("resolving {fqdn_parts:?}");
    let fqdn = Name::from_labels(fqdn_parts.iter().map(|&s| s.as_bytes())).map_err(|e| Box::new(e))?;
    let response = retransmitting(QUERY_TOTAL_TIMEOUT, QUERY_RETRANSMIT_INTERVAL, |timeout| {
      self.do_single_query_sendable(&[RecordType::SRV, RecordType::TXT], &fqdn, timeout)
    })
    .await?;
    let mut target = None;
    let mut properties = BTreeMap::new();
    for record in response.answers() {
      if record.name().to_lowercase() != fqdn.to_lowercase() {
        continue;
      }
      if let Some(rdata) = record.data() {
        if let Some(srv) = rdata.as_srv() {
          target = Some((srv.target(), srv.port()));
        } else if let Some(txt) = rdata.as_txt() {
          for txtbytes in txt.txt_data().iter() {
            let s = str::from_utf8(txtbytes).map_err(Box::new)?;
            if let Some((key, value)) = s.split_once("=") {
              properties.insert(key.to_owned(), value.to_owned());
            }
          }
        }
      }
    }
    if let Some((name, port)) = target {
      for record in response.additionals() {
        if record.name().to_lowercase() != name.to_lowercase() {
          continue;
        }
        if let Some(rdata) = record.data() {
          if let Some(a) = rdata.as_a() {
            return Ok(AdvertisedService {
              addr: SocketAddr::new(std::net::IpAddr::V4(*a), port),
              properties,
            });
          }
        }
      }
      // if at this point we haven't returned from this method,
      // it means that additional A record wasn't contained in the response
      let a_response = retransmitting(QUERY_TOTAL_TIMEOUT, QUERY_RETRANSMIT_INTERVAL, |timeout| {
        self.do_single_query_sendable(&[RecordType::A], &name, timeout)
      })
      .await?;
      for record in a_response.answers() {
        if record.name().to_lowercase() != name.to_lowercase() {
          continue;
        }
        if let Some(rdata) = record.data() {
          if let Some(a) = rdata.as_a() {
            return Ok(AdvertisedService {
              addr: SocketAddr::new(std::net::IpAddr::V4(*a), port),
              properties,
            });
          }
        }
      }
    }
    return Err(Box::new(io::Error::from(io::ErrorKind::NotFound)));
  }

  pub async fn query_chan(
    &self,
    tx_hostname: &str,
    tx_channel_name: &str,
  ) -> Result<AdvertisedChannel, Box<dyn Error>> {
    let full_name = format!("{}@{}", tx_channel_name, tx_hostname);
    let fqdn = [&full_name, "_netaudio-chan", "_udp", "local"];
    let result = self.query(&fqdn).await?;
    parse_channel_properties(result.addr, tx_hostname, &result.properties)
  }

  pub async fn query_bund(&self, full_name: &str) -> Result<AdvertisedBundle, Box<dyn Error>> {
    let fqdn = [full_name, "_netaudio-bund", "_udp", "local"];
    let result = self.query(&fqdn).await?;
    parse_bundle_properties(&result.properties)
  }
}

/// The most channels a flow may carry according to a transmitter's mDNS records. Dante flows carry at
/// most 8 channels (the largest unicast flow we ever request is 8); the margin keeps a future device
/// working while a hostile or broken record (`nchan=4000000000`) cannot make us allocate per-channel
/// state for billions of channels. ONS fork (issue #92).
pub const MAX_ADVERTISED_CHANNELS_PER_FLOW: usize = 64;

/// The longest minimum receive latency a transmitter may ask for. Anything longer is a broken record
/// (and would overflow the latency arithmetic).
const MAX_ADVERTISED_LATENCY_NS: usize = 10_000_000_000;

fn invalid_data(what: impl Into<String>) -> Box<dyn Error> {
  Box::new(io::Error::new(io::ErrorKind::InvalidData, what.into()))
}

/// Reads the number `key` from the TXT properties, decimal or `0x` hexadecimal.
fn parse_int_from_dict(dict: &TxtEntries, key: &str) -> Result<usize, Box<dyn Error>> {
  match dict.get(key) {
    Some(s) => {
      let result = match s.strip_prefix("0x") {
        Some(hex) => usize::from_str_radix(hex, 16),
        None => s.parse::<usize>(),
      };
      match result {
        Ok(v) => Ok(v),
        Err(e) => {
          error!("unable to parse {key}={s}");
          Err(Box::new(e))
        }
      }
    }
    None => {
      error!("{key} not found in dns response");
      Err(Box::new(io::Error::from(io::ErrorKind::NotFound)))
    }
  }
}

/// A number that must fit `u16` (the protocol's channel, bundle and port ids).
fn parse_u16_from_dict(dict: &TxtEntries, key: &str) -> Result<u16, Box<dyn Error>> {
  let v = parse_int_from_dict(dict, key)?;
  u16::try_from(v).map_err(|_| invalid_data(format!("{key}={v} does not fit 16 bits")))
}

/// The sample size in bits, which must be one the receiver can decode (16, 24 or 32).
fn parse_encoding(dict: &TxtEntries) -> Result<u32, Box<dyn Error>> {
  let bits = parse_int_from_dict(dict, "enc").or_else(|_| parse_int_from_dict(dict, "en"))?;
  match bits {
    16 | 24 | 32 => Ok(bits as u32),
    other => Err(invalid_data(format!("unsupported sample size of {other} bits"))),
  }
}

fn parse_channels_per_flow(dict: &TxtEntries) -> Result<usize, Box<dyn Error>> {
  let nchan = parse_int_from_dict(dict, "nchan")?;
  if (1..=MAX_ADVERTISED_CHANNELS_PER_FLOW).contains(&nchan) {
    Ok(nchan)
  } else {
    Err(invalid_data(format!("nchan={nchan} is outside 1..={MAX_ADVERTISED_CHANNELS_PER_FLOW}")))
  }
}

fn parse_latency_ns(dict: &TxtEntries) -> Result<usize, Box<dyn Error>> {
  let latency = parse_int_from_dict(dict, "latency_ns")?;
  if latency > MAX_ADVERTISED_LATENCY_NS {
    return Err(invalid_data(format!("latency_ns={latency} is implausibly long")));
  }
  Ok(latency)
}

/// Turns the TXT properties of a `_netaudio-chan` service into an [`AdvertisedChannel`]. The records come
/// from the network, so every value is range checked: an `Ok` result always has `1..=64` channels per
/// flow, 16, 24 or 32 bits per sample, a nonzero maximum frames per packet and a sane latency, which
/// the subscriber relies on (chunk sizes, divisions, sample decoding). Never panics.
pub(crate) fn parse_channel_properties(
  addr: SocketAddr,
  tx_hostname: &str,
  properties: &TxtEntries,
) -> Result<AdvertisedChannel, Box<dyn Error>> {
  let mut multicast = None;
  for (key, value) in properties {
    if let Some(bundle) = key.strip_prefix("b.") {
      multicast = Some(PointerToMulticast {
        tx_hostname: tx_hostname.to_owned(),
        bundle_id: match bundle.parse::<usize>() {
          Ok(v) => v,
          Err(_e) => {
            error!("Unable to parse multicast bundle key {key}");
            break;
          }
        },
        channel_in_bundle: match value.parse::<usize>() {
          Ok(v) if v > 0 => v - 1,
          _ => {
            error!("Unable to parse multicast bundle value {value}");
            break;
          }
        },
      });
      break;
    }
  }
  let (fpp1, fpp2) = properties
    .get("fpp")
    .ok_or(Box::new(io::Error::from(io::ErrorKind::NotFound)))?
    .split_once(",")
    .ok_or(Box::new(io::Error::from(io::ErrorKind::InvalidData)))?;
  let fpp_min: u16 = fpp2.parse()?;
  let fpp_max: u16 = fpp1.parse()?;
  if fpp_max == 0 {
    return Err(invalid_data("fpp maximum is 0"));
  }
  Ok(AdvertisedChannel {
    addr,
    tx_channels_per_flow: parse_channels_per_flow(properties)?,
    tx_channel_id: parse_u16_from_dict(properties, "id")?,
    bits_per_sample: parse_encoding(properties)?,
    dbcp1: parse_u16_from_dict(properties, "dbcp1")?,
    fpp_min,
    fpp_max,
    min_rx_latency_ns: parse_latency_ns(properties)?,
    multicast,
  })
}

/// Turns the TXT properties of a `_netaudio-bund` service into an [`AdvertisedBundle`], with the same
/// guarantees as [`parse_channel_properties`]; the media address is a multicast group and a nonzero port.
pub(crate) fn parse_bundle_properties(
  properties: &TxtEntries,
) -> Result<AdvertisedBundle, Box<dyn Error>> {
  let ip: Ipv4Addr =
    properties.get("a.0").ok_or(Box::new(io::Error::from(io::ErrorKind::NotFound)))?.parse()?;
  let port: u16 =
    properties.get("p.0").ok_or(Box::new(io::Error::from(io::ErrorKind::NotFound)))?.parse()?;
  if !ip.is_multicast() || port == 0 {
    return Err(invalid_data(format!("{ip}:{port} is not a multicast media address")));
  }
  let fpp = parse_u16_from_dict(properties, "fpp")?;
  if fpp == 0 {
    return Err(invalid_data("fpp is 0"));
  }
  Ok(AdvertisedBundle {
    tx_channels_per_flow: parse_channels_per_flow(properties)?,
    tx_bundle_id: parse_u16_from_dict(properties, "id")?,
    bits_per_sample: parse_encoding(properties)?,
    fpp,
    min_rx_latency_ns: parse_latency_ns(properties)?,
    media_addr: SocketAddr::V4(SocketAddrV4::new(ip, port)),
  })
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::sync::atomic::{AtomicUsize, Ordering};

  use rand::{rngs::SmallRng, Rng, SeedableRng};

  fn props(entries: &[(&str, &str)]) -> TxtEntries {
    entries.iter().map(|&(k, v)| (k.to_owned(), v.to_owned())).collect()
  }

  fn chan_props() -> TxtEntries {
    props(&[
      ("txtvers", "2"),
      ("dbcp1", "0x1102"),
      ("id", "3"),
      ("rate", "48000"),
      ("enc", "24"),
      ("en", "24"),
      ("latency_ns", "1000000"),
      ("fpp", "192,16"),
      ("nchan", "8"),
    ])
  }

  fn bund_props() -> TxtEntries {
    props(&[
      ("id", "1"),
      ("nchan", "2"),
      ("latency_ns", "1000000"),
      ("fpp", "48"),
      ("enc", "24"),
      ("a.0", "239.69.1.2"),
      ("p.0", "4321"),
    ])
  }

  fn addr() -> SocketAddr {
    SocketAddr::new(Ipv4Addr::new(192, 168, 1, 10).into(), 4440)
  }

  /// What the subscriber relies on after a record was accepted.
  fn assert_channel_sane(c: &AdvertisedChannel) {
    assert!((1..=MAX_ADVERTISED_CHANNELS_PER_FLOW).contains(&c.tx_channels_per_flow), "{c:?}");
    assert!([16, 24, 32].contains(&c.bits_per_sample), "{c:?}");
    assert!(c.fpp_max >= 1, "{c:?}");
    assert!(c.min_rx_latency_ns <= MAX_ADVERTISED_LATENCY_NS, "{c:?}");
  }

  fn assert_bundle_sane(b: &AdvertisedBundle) {
    assert!((1..=MAX_ADVERTISED_CHANNELS_PER_FLOW).contains(&b.tx_channels_per_flow), "{b:?}");
    assert!([16, 24, 32].contains(&b.bits_per_sample), "{b:?}");
    assert!(b.fpp >= 1, "{b:?}");
    assert!(b.min_rx_latency_ns <= MAX_ADVERTISED_LATENCY_NS, "{b:?}");
    assert!(b.media_addr.ip().is_multicast() && b.media_addr.port() != 0, "{b:?}");
  }

  #[test]
  fn a_good_channel_record_is_accepted() {
    let c = parse_channel_properties(addr(), "host", &chan_props()).unwrap();
    assert_eq!((c.tx_channels_per_flow, c.tx_channel_id, c.bits_per_sample), (8, 3, 24));
    assert_eq!((c.dbcp1, c.fpp_max, c.fpp_min, c.min_rx_latency_ns), (0x1102, 192, 16, 1_000_000));
    assert!(c.multicast.is_none());
    let mut p = chan_props();
    p.insert("b.7".into(), "2".into());
    let m = parse_channel_properties(addr(), "host", &p).unwrap().multicast.unwrap();
    assert_eq!((m.bundle_id, m.channel_in_bundle), (7, 1));
  }

  #[test]
  fn a_good_bundle_record_is_accepted() {
    let b = parse_bundle_properties(&bund_props()).unwrap();
    assert_eq!((b.tx_channels_per_flow, b.tx_bundle_id, b.bits_per_sample, b.fpp), (2, 1, 24, 48));
    assert_eq!(b.media_addr, "239.69.1.2:4321".parse().unwrap());
  }

  #[test]
  fn values_the_receiver_cannot_handle_are_refused() {
    // (key, value) pairs that used to reach the subscriber and panic it (chunks(0), division by zero,
    // assert on the sample size, unsupported bytes per sample) or allocate without bound.
    for (key, value) in [
      ("nchan", "0"),
      ("nchan", "65"),
      ("nchan", "4000000000"),
      ("enc", "0"),
      ("enc", "4"),
      ("enc", "12"),
      ("enc", "40"),
      ("id", "70000"),
      ("dbcp1", "0x10000"),
      ("fpp", "0,16"),
      ("fpp", "192"),
      ("fpp", "70000,16"),
      ("latency_ns", "18446744073709551615"),
    ] {
      let mut p = chan_props();
      p.insert(key.into(), value.into());
      if key == "enc" {
        p.insert("en".into(), value.into());
      }
      assert!(parse_channel_properties(addr(), "host", &p).is_err(), "{key}={value}");
    }
    for (key, value) in [
      ("nchan", "0"),
      ("nchan", "100000000000"),
      ("enc", "0"),
      ("fpp", "0"),
      ("a.0", "192.168.1.1"),
      ("a.0", "ff02::1"),
      ("p.0", "0"),
      ("p.0", "65536"),
      ("latency_ns", "99999999999999999999"),
    ] {
      let mut p = bund_props();
      p.insert(key.into(), value.into());
      assert!(parse_bundle_properties(&p).is_err(), "{key}={value}");
    }
  }

  #[test]
  fn a_record_missing_any_key_is_an_error_not_a_panic() {
    for key in chan_props().keys() {
      let mut p = chan_props();
      p.remove(key);
      if let Ok(c) = parse_channel_properties(addr(), "host", &p) {
        assert_channel_sane(&c); // optional keys (txtvers, rate, en) may be absent
      }
    }
    for key in bund_props().keys() {
      let mut p = bund_props();
      p.remove(key);
      assert!(parse_bundle_properties(&p).is_err(), "{key}");
    }
    assert!(parse_channel_properties(addr(), "host", &TxtEntries::new()).is_err());
    assert!(parse_bundle_properties(&TxtEntries::new()).is_err());
  }

  /// A random string: mostly digits and the separators the parsers split on, sometimes arbitrary
  /// (including multi-byte) characters.
  fn random_value(rng: &mut SmallRng) -> String {
    #[rustfmt::skip]
    const POOL: &[&str] = &[
      "0", "1", "2", "8", "16", "24", "32", "48", "192", "255", "65535", "65536", "0x", "0x1102", "0xzz", "-1",
      ",", "192,16", ",,", "239.1.2.3", "224.0.0.251", "::1", "é", "日本", "", " ", "99999999999999999999999",
      "4294967296",
    ];
    if rng.gen_bool(0.7) {
      POOL[rng.gen_range(0..POOL.len())].to_owned()
    } else {
      (0..rng.gen_range(0..12)).map(|_| char::from_u32(rng.gen_range(0..0x3000)).unwrap_or('?')).collect()
    }
  }

  #[test]
  fn random_mutations_of_a_record_never_panic_and_accepted_records_are_sane() {
    let mut rng = SmallRng::seed_from_u64(0x1337_0092);
    let (mut accepted_chan, mut accepted_bund) = (0, 0);
    for _ in 0..20_000 {
      let mut p = chan_props();
      let mut b = bund_props();
      p.insert("b.1".into(), "1".into());
      for _ in 0..rng.gen_range(1..4) {
        let target = if rng.gen_bool(0.5) { &mut p } else { &mut b };
        let keys: Vec<String> = target.keys().cloned().collect();
        let key = keys[rng.gen_range(0..keys.len())].clone();
        match rng.gen_range(0..4) {
          0 => {
            target.remove(&key);
          }
          1 => {
            target.insert(key, random_value(&mut rng));
          }
          2 => {
            target.insert(random_value(&mut rng), random_value(&mut rng));
          }
          _ => {
            // truncate the value
            let v = target[&key].clone();
            let cut = rng.gen_range(0..=v.chars().count());
            target.insert(key, v.chars().take(cut).collect());
          }
        }
      }
      if let Ok(c) = parse_channel_properties(addr(), "host", &p) {
        assert_channel_sane(&c);
        accepted_chan += 1;
      }
      if let Ok(bundle) = parse_bundle_properties(&b) {
        assert_bundle_sane(&bundle);
        accepted_bund += 1;
      }
    }
    // the fuzzer must exercise both outcomes
    assert!(accepted_chan > 100 && accepted_bund > 100, "{accepted_chan} {accepted_bund}");
  }

  #[tokio::test]
  async fn retransmits_until_an_attempt_succeeds() {
    let calls = AtomicUsize::new(0);
    let result: Result<usize, ()> =
      retransmitting(Duration::from_millis(600), Duration::from_millis(50), |_| async {
        let n = calls.fetch_add(1, Ordering::SeqCst);
        // the first two questions are lost (they time out), the third is answered
        if n < 2 {
          tokio::time::sleep(Duration::from_millis(50)).await;
          Err(())
        } else {
          Ok(n)
        }
      })
      .await;
    assert_eq!(result, Ok(2));
    assert_eq!(calls.load(Ordering::SeqCst), 3);
  }

  #[tokio::test]
  async fn a_failing_attempt_does_not_spin() {
    let calls = AtomicUsize::new(0);
    let start = Instant::now();
    let result: Result<(), &str> =
      retransmitting(Duration::from_millis(300), Duration::from_millis(100), |_| async {
        calls.fetch_add(1, Ordering::SeqCst);
        Err("send failed at once")
      })
      .await;
    assert_eq!(result, Err("send failed at once"));
    assert!(start.elapsed() >= Duration::from_millis(290), "gave up after {:?}", start.elapsed());
    // one attempt per 100 ms window, 3 windows
    assert!(calls.load(Ordering::SeqCst) <= 4, "{} attempts", calls.load(Ordering::SeqCst));
  }

  #[tokio::test]
  async fn the_last_attempt_gets_only_the_remaining_time() {
    let seen = std::sync::Mutex::new(vec![]);
    let _: Result<(), ()> = retransmitting(Duration::from_millis(250), Duration::from_millis(100), |t| {
      seen.lock().unwrap().push(t);
      async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        Err(())
      }
    })
    .await;
    let seen = seen.into_inner().unwrap();
    assert_eq!(seen[0], Duration::from_millis(100));
    assert!(seen.last().unwrap() <= &Duration::from_millis(100));
    assert!(seen.iter().all(|t| *t <= Duration::from_millis(100)));
  }
}
