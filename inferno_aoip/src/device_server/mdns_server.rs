use searchfire::{
  broadcast::{BroadcasterBuilder, BroadcasterHandle, Service, ServiceBuilder},
  dns::rr::Name,
  net::{IpVersion, TargetInterface},
};
use std::{
  collections::{BTreeMap, BTreeSet},
  net::{IpAddr, Ipv4Addr},
  sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard},
};

use super::flows_tx::{FPP_MAX_ADVERTISED, FPP_MIN, MAX_CHANNELS_IN_FLOW};
use crate::{
  device_info::DeviceInfo,
  mdns_client::{self_origin_from_self_info, MdnsClient, PointerToMulticast},
  utils::LogAndForget,
};

// ONS fork (issue #92): every service name, label and record below is built from names that a controller
// can change (device and channel renames), so building a record is fallible and a record that cannot be
// built is logged and skipped instead of panicking the ARC handler that asked for it. The locks guard
// plain tables, so a poisoned lock is used as it is.
fn lock_mutex<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
  m.lock().unwrap_or_else(|e| e.into_inner())
}
fn read_lock<T>(l: &RwLock<T>) -> RwLockReadGuard<'_, T> {
  l.read().unwrap_or_else(|e| e.into_inner())
}

/// Why a record could not be built or the responder could not start.
pub type MdnsError = String;

/// A multicast bundle that is currently advertised (kept so that a rename can advertise it again).
#[derive(Clone, Copy, Debug)]
struct Bundle {
  channels_per_flow: usize,
  fpp: usize,
  dst_addr: Ipv4Addr,
  dst_port: u16,
}

/// What is advertised besides the two device services. ONS fork: a rename removes the records under
/// the old device name and announces all of them again under the new one.
#[derive(Default)]
struct Announced {
  tx_channels: BTreeSet<usize>,
  bundles: BTreeMap<usize, Bundle>,
}

pub struct DeviceMDNSResponder {
  handle: RwLock<Option<BroadcasterHandle>>,
  self_info: Arc<DeviceInfo>,
  self_origin: Vec<u8>,
  multicasts_by_channel: Arc<RwLock<BTreeMap<usize, PointerToMulticast>>>,
  /// Also serializes every change of the advertised records against a rename.
  announced: Mutex<Announced>,
}

pub fn kv<T: std::fmt::Display>(key: &str, value: T) -> String {
  format!("{key}={value}")
}

/// A name made of labels that are fixed by the protocol or are decimal numbers, so building it cannot
/// fail; if it ever did, the root name is used (and the record is simply not found by anyone).
fn fixed_name<'a>(labels: impl IntoIterator<Item = &'a str>) -> Name {
  Name::from_labels(labels.into_iter().map(|s| s.as_bytes())).unwrap_or_else(|e| {
    log::error!("BUG: cannot build a fixed mDNS name: {e}");
    Name::root()
  })
}

pub fn service_type(st: &str) -> Name {
  fixed_name([st, "_udp", "local"])
}
pub fn in_addr_type() -> Name {
  fixed_name(["in-addr", "local"])
}
fn multicast_ip_to_name(addr: Ipv4Addr) -> Name {
  let octets = addr.octets();
  fixed_name([
    octets[3].to_string().as_str(),
    octets[2].to_string().as_str(),
    octets[1].to_string().as_str(),
    octets[0].to_string().as_str(),
  ])
}

/// A single-label service instance name. Fails for a name that is not a valid DNS label (longer than 63
/// bytes).
fn host_label(name: &str) -> Result<Name, MdnsError> {
  Name::from_labels([name.as_bytes()]).map_err(|e| format!("{name:?} is not a valid mDNS name: {e}"))
}

fn builder_error(what: &str, e: impl std::fmt::Debug) -> MdnsError {
  format!("cannot build the {what} record: {e:?}")
}

fn arc_service(self_info: &DeviceInfo, hostname: &str) -> Result<Service, MdnsError> {
  ServiceBuilder::new(service_type("_netaudio-arc"), host_label(hostname)?, self_info.arc_port)
    .map_err(|e| builder_error("_netaudio-arc", e))?
    .add_ip_address(IpAddr::V4(self_info.ip_address))
    .add_txt_truncated("arcp_vers=2.7.41")
    .add_txt_truncated("arcp_min=0.2.4")
    .add_txt_truncated("router_vers=4.0.2")
    .add_txt_truncated(kv("router_info", &self_info.board_name))
    .add_txt_truncated(kv("mf", &self_info.manufacturer))
    .add_txt_truncated(kv("model", &self_info.model_number))
    .ttl(4500)
    .build()
    .map_err(|e| builder_error("_netaudio-arc", e))
}

fn cmc_service(self_info: &DeviceInfo, hostname: &str) -> Result<Service, MdnsError> {
  ServiceBuilder::new(service_type("_netaudio-cmc"), host_label(hostname)?, self_info.cmc_port)
    .map_err(|e| builder_error("_netaudio-cmc", e))?
    .add_ip_address(IpAddr::V4(self_info.ip_address))
    .add_txt_truncated(kv("id", &hex::encode(self_info.factory_device_id)))
    .add_txt_truncated(kv("process", self_info.process_id))
    .add_txt_truncated("cmcp_vers=1.2.0")
    .add_txt_truncated("cmcp_min=1.0.0")
    .add_txt_truncated("server_vers=4.0.2")
    .add_txt_truncated("channels=0x6000004d") // ???
    .add_txt_truncated(kv("mf", &self_info.manufacturer))
    .add_txt_truncated(kv("model", &self_info.model_number))
    .add_txt_truncated("") // really needed?
    .add_txt_truncated("") // really needed?
    .build()
    .map_err(|e| builder_error("_netaudio-cmc", e))
}

fn bundle_service(
  self_info: &DeviceInfo,
  hostname: &str,
  bundle_id: usize,
  b: &Bundle,
) -> Result<Service, MdnsError> {
  ServiceBuilder::new(
    service_type("_netaudio-bund"),
    host_label(&format!("{bundle_id}@{hostname}"))?,
    self_info.flows_control_port,
  )
  .map_err(|e| builder_error("_netaudio-bund", e))?
  .add_ip_address(IpAddr::V4(self_info.ip_address))
  .add_txt_truncated("txtvers=1")
  .add_txt_truncated(kv("id", bundle_id))
  .add_txt_truncated(kv("nchan", b.channels_per_flow))
  .add_txt_truncated(kv("latency_ns", self_info.latency_ns /* FIXME should be tx latency */))
  .add_txt_truncated(kv("fpp", b.fpp))
  .add_txt_truncated(kv("rate", self_info.sample_rate))
  .add_txt_truncated(kv("enc", self_info.bits_per_sample))
  .add_txt_truncated(kv("a.0", b.dst_addr))
  .add_txt_truncated(kv("p.0", b.dst_port))
  .build()
  .map_err(|e| builder_error("_netaudio-bund", e))
}

/// The `_netaudio-chan` service of transmit channel `index` under the name `ch_name`. `multicast` is the
/// bundle the channel is also sent in, if any.
fn channel_service(
  self_info: &DeviceInfo,
  multicast: Option<&PointerToMulticast>,
  index: usize,
  ch_name: &str,
  default: bool,
  hostname: &str,
) -> Result<Service, MdnsError> {
  let bundle = multicast.map(|p| format!("b.{}={}", p.bundle_id, p.channel_in_bundle + 1));
  let name = host_label(&format!("{ch_name}@{hostname}"))?;
  let mut b = ServiceBuilder::new(service_type("_netaudio-chan"), name, self_info.flows_control_port)
    .map_err(|e| builder_error("_netaudio-chan", e))?
    .add_ip_address(IpAddr::V4(self_info.ip_address))
    .add_txt_truncated("txtvers=2")
    .add_txt_truncated("dbcp1=0x1102")
    .add_txt_truncated("dbcp=0x1004")
    .add_txt_truncated(kv("id", index + 1))
    .add_txt_truncated(kv("rate", self_info.sample_rate))
    .add_txt_truncated(format!("pcm={} {:x}", self_info.bits_per_sample / 8, self_info.pcm_type))
    .add_txt_truncated(kv("enc", self_info.bits_per_sample))
    .add_txt_truncated(kv("en", self_info.bits_per_sample))
    .add_txt_truncated(kv("latency_ns", self_info.latency_ns /* FIXME should be tx latency */))
    .add_txt_truncated(format!("fpp={},{}", FPP_MAX_ADVERTISED, FPP_MIN))
    .add_txt_truncated(kv("nchan", MAX_CHANNELS_IN_FLOW.min(self_info.tx_channels.len() as u16)));
  if default {
    b = b.add_txt_truncated("default");
  }
  if let Some(s) = &bundle {
    b = b.add_txt_truncated(s.clone());
  }
  b.build().map_err(|e| builder_error("_netaudio-chan", e))
}

/// Multicast the answers for `services` on mDNS from `ip`, now and once more after a second (RFC 6762
/// section 8.3 asks for at least two announcements). Runs on its own thread so that the caller, an
/// ARC request handler, is not held up; failures are only logged.
fn announce_unsolicited(ip: Ipv4Addr, services: Vec<Service>) {
  use searchfire::dns::serialize::binary::BinEncodable;
  let packets: Vec<Vec<u8>> =
    services.iter().filter_map(|s| s.dns_response().ok()).filter_map(|m| m.to_bytes().ok()).collect();
  if packets.is_empty() {
    return;
  }
  let spawned = std::thread::Builder::new().name("mdns announce".into()).spawn(move || {
    let send = || -> std::io::Result<()> {
      use socket2::{Domain, Protocol, SockAddr, Socket, Type};
      let sock = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
      sock.bind(&SockAddr::from(std::net::SocketAddrV4::new(ip, 0)))?;
      sock.set_multicast_if_v4(&ip)?;
      sock.set_multicast_loop_v4(true)?;
      sock.set_multicast_ttl_v4(255)?;
      let sock: std::net::UdpSocket = sock.into();
      for p in &packets {
        sock.send_to(p, (Ipv4Addr::new(224, 0, 0, 251), 5353))?;
      }
      Ok(())
    };
    for round in 0..2 {
      if round > 0 {
        std::thread::sleep(std::time::Duration::from_secs(1));
      }
      if let Err(e) = send() {
        log::warn!("cannot announce the renamed device on mDNS: {e}");
        return;
      }
    }
  });
  if let Err(e) = spawned {
    log::warn!("cannot start the mDNS announcement: {e}");
  }
}

impl DeviceMDNSResponder {
  /// Starts answering mDNS queries for the device on its NIC. Fails if the responder cannot be set up,
  /// typically because the address is not (or no longer) assigned to an interface.
  pub fn start(
    self_info: Arc<DeviceInfo>,
    multicasts_by_channel: Arc<RwLock<BTreeMap<usize, PointerToMulticast>>>,
  ) -> Result<Self, MdnsError> {
    let hostname = self_info.friendly_hostname();
    let bb = BroadcasterBuilder::new()
      .loopback()
      .interface_v4(TargetInterface::Specific(self_info.ip_address))
      .add_service(arc_service(&self_info, &hostname)?)
      .add_service(cmc_service(&self_info, &hostname)?);

    let handle = bb
      .build(IpVersion::V4)
      .map_err(|e| format!("cannot start the mDNS responder on {}: {e}", self_info.ip_address))?
      .run_in_background();

    Ok(Self {
      handle: RwLock::new(Some(handle)),
      self_origin: self_origin_from_self_info(&self_info),
      self_info,
      multicasts_by_channel,
      announced: Mutex::new(Announced::default()),
    })
  }

  /// The `_netaudio-chan` service of channel `index` under the name `ch_name`.
  fn channel_service(
    &self,
    index: usize,
    ch_name: &str,
    default: bool,
    hostname: &str,
  ) -> Result<Service, MdnsError> {
    let multicast = read_lock(&self.multicasts_by_channel).get(&index).cloned();
    channel_service(&self.self_info, multicast.as_ref(), index, ch_name, default, hostname)
  }

  /// The channel names a transmit channel is advertised under: the factory name, plus the friendly
  /// name when it differs. `None` for a channel that does not exist.
  fn channel_names(&self, index: usize) -> Option<(String, Option<String>)> {
    let txch = self.self_info.tx_channels.get(index)?;
    let friendly = read_lock(&txch.friendly_name).clone();
    let extra = (txch.factory_name != friendly).then_some(friendly);
    Some((txch.factory_name.clone(), extra))
  }

  fn add_tx_channel_services(&self, handle: &BroadcasterHandle, index: usize, hostname: &str) {
    let Some((factory, friendly)) = self.channel_names(index) else {
      log::error!("cannot advertise nonexistent transmit channel index {index}");
      return;
    };
    for (ch_name, default) in std::iter::once((factory, true)).chain(friendly.map(|f| (f, false))) {
      match self.channel_service(index, &ch_name, default, hostname) {
        Ok(service) => handle.add_service(service).log_and_forget(),
        Err(e) => log::warn!("channel {ch_name:?} is not advertised: {e}"),
      }
    }
  }

  fn remove_tx_channel_services(&self, handle: &BroadcasterHandle, index: usize, hostname: &str) {
    let Some((factory, friendly)) = self.channel_names(index) else {
      return;
    };
    for ch_name in std::iter::once(factory).chain(friendly) {
      // (a name that is not a valid label was never advertised, so there is nothing to remove)
      if let Ok(name) = host_label(&format!("{ch_name}@{hostname}")) {
        handle.remove_named_service(service_type("_netaudio-chan"), name).log_and_forget();
      }
    }
  }

  pub fn add_tx_channel(&self, index: usize) {
    let mut announced = lock_mutex(&self.announced);
    let hostname = self.self_info.friendly_hostname();
    let handle = read_lock(&self.handle);
    match handle.as_ref() {
      Some(handle) => {
        self.add_tx_channel_services(handle, index, &hostname);
        announced.tx_channels.insert(index);
      }
      None => {
        log::error!("BUG: trying to add channel using BroadcasterHandle after it was shut down");
      }
    };
  }

  pub fn remove_tx_channel(&self, index: usize) {
    let mut announced = lock_mutex(&self.announced);
    let hostname = self.self_info.friendly_hostname();
    match read_lock(&self.handle).as_ref() {
      Some(handle) => {
        self.remove_tx_channel_services(handle, index, &hostname);
        announced.tx_channels.remove(&index);
      }
      None => {
        log::error!("BUG: trying to remove channel using BroadcasterHandle after it was shut down");
      }
    }
  }

  pub fn add_multicast_bundle(
    &self,
    bundle_id: usize,
    channels_per_flow: usize,
    fpp: usize,
    dst_addr: Ipv4Addr,
    dst_port: u16,
  ) {
    let mut announced = lock_mutex(&self.announced);
    let hostname = self.self_info.friendly_hostname();
    let bundle = Bundle { channels_per_flow, fpp, dst_addr, dst_port };
    let service = match bundle_service(&self.self_info, &hostname, bundle_id, &bundle) {
      Ok(service) => service,
      Err(e) => {
        log::error!("multicast bundle {bundle_id} is not advertised: {e}");
        return;
      }
    };
    let handle = read_lock(&self.handle);
    match handle.as_ref() {
      Some(handle) => {
        handle.add_service(service).log_and_forget();
        announced.bundles.insert(bundle_id, bundle);
      }
      None => {
        log::error!("BUG: trying to add multicast bundle using BroadcasterHandle after it was shut down");
      }
    }
  }

  pub fn remove_multicast_bundle(&self, bundle_id: usize) {
    let mut announced = lock_mutex(&self.announced);
    let hostname = self.self_info.friendly_hostname();
    let handle = read_lock(&self.handle);
    match handle.as_ref() {
      Some(handle) => {
        match host_label(&format!("{bundle_id}@{hostname}")) {
          Ok(name) => handle.remove_named_service(service_type("_netaudio-bund"), name).log_and_forget(),
          Err(e) => log::warn!("multicast bundle {bundle_id}: {e}"),
        }
        announced.bundles.remove(&bundle_id);
      }
      None => {
        log::error!(
          "BUG: trying to remove multicast bundle using BroadcasterHandle after it was shut down"
        );
      }
    }
  }

  /// ONS fork: give the device a new name. Stops advertising the services under the old name (the
  /// host should send an mDNS goodbye for them, see `DeviceRename`), changes the shared name and
  /// advertises the device services, the transmit channels and the multicast bundles under the new
  /// name. The new name must have been validated.
  pub fn rename_device(&self, new_name: &str) {
    let announced = lock_mutex(&self.announced);
    let old_name = self.self_info.friendly_hostname();
    let handle = read_lock(&self.handle);
    let Some(handle) = handle.as_ref() else {
      // Already shut down: nothing is advertised, only the name changes.
      self.self_info.set_friendly_hostname(new_name);
      return;
    };
    if let Ok(name) = host_label(&old_name) {
      handle.remove_named_service(service_type("_netaudio-arc"), name.clone()).log_and_forget();
      handle.remove_named_service(service_type("_netaudio-cmc"), name).log_and_forget();
    }
    for index in &announced.tx_channels {
      self.remove_tx_channel_services(handle, *index, &old_name);
    }
    for bundle_id in announced.bundles.keys() {
      if let Ok(name) = host_label(&format!("{bundle_id}@{old_name}")) {
        handle.remove_named_service(service_type("_netaudio-bund"), name).log_and_forget();
      }
    }

    self.self_info.set_friendly_hostname(new_name);

    // The responder only answers queries, so tell the network about the new records unprompted: a
    // controller that has just dropped the old device (goodbye) would otherwise wait for its next browse.
    let mut announce = vec![];
    let mut keep = |what: &str, service: Result<Service, MdnsError>| match service {
      Ok(service) => announce.push(service),
      Err(e) => log::warn!("{what} is not advertised under the new name: {e}"),
    };
    keep("the ARC service", arc_service(&self.self_info, new_name));
    keep("the CMC service", cmc_service(&self.self_info, new_name));
    for index in &announced.tx_channels {
      let Some((factory, friendly)) = self.channel_names(*index) else {
        continue;
      };
      keep("a channel", self.channel_service(*index, &factory, true, new_name));
      if let Some(friendly) = friendly {
        keep("a channel", self.channel_service(*index, &friendly, false, new_name));
      }
    }
    for (bundle_id, bundle) in &announced.bundles {
      keep("a multicast bundle", bundle_service(&self.self_info, new_name, *bundle_id, bundle));
    }
    for service in &announce {
      handle.add_service(service.clone()).log_and_forget();
    }
    announce_unsolicited(self.self_info.ip_address, announce);
  }

  pub fn reserve_multicast_ip(&self, addr: Ipv4Addr) {
    let self_info = &*self.self_info;
    let service = ServiceBuilder::new(in_addr_type(), multicast_ip_to_name(addr), 0)
      .map_err(|e| builder_error("in-addr", e))
      .and_then(|b| {
        b.add_ip_address(IpAddr::V4(self_info.ip_address))
          .add_additional_txt(fixed_name(["_inferno-response-origin", "local"]), self.self_origin.clone())
          .build()
          .map_err(|e| builder_error("in-addr", e))
      });
    let service = match service {
      Ok(service) => service,
      Err(e) => {
        log::error!("multicast address {addr} is not reserved: {e}");
        return;
      }
    };
    let handle = read_lock(&self.handle);
    match handle.as_ref() {
      Some(handle) => {
        handle.add_service(service).log_and_forget();
      }
      None => {
        log::error!("BUG: trying to reserve multicast IP using BroadcasterHandle after it was shut down");
      }
    }
  }
  pub fn remove_multicast_ip(&self, addr: Ipv4Addr) {
    let handle = read_lock(&self.handle);
    match handle.as_ref() {
      Some(handle) => {
        handle.remove_named_service(in_addr_type(), multicast_ip_to_name(addr)).log_and_forget();
      }
      None => {
        log::error!(
          "BUG: trying to remove multicast IP reservation using BroadcasterHandle after it was shut down"
        );
      }
    }
  }

  pub fn shutdown_and_join(&self) {
    let handle = self.handle.write().unwrap_or_else(|e| e.into_inner()).take();
    match handle {
      Some(handle) => handle.shutdown().log_and_forget(),
      None => log::warn!("the mDNS responder was already shut down"),
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::device_info::Channel;
  use crate::protocol::proto_arc::test_device_info;
  use rand::{rngs::SmallRng, Rng, SeedableRng};

  fn info_with_channels(n: usize) -> DeviceInfo {
    let mut info = test_device_info();
    info.tx_channels = (0..n)
      .map(|i| Channel {
        factory_name: format!("{:02}", i + 1),
        friendly_name: Arc::new(RwLock::new(format!("TX {}", i + 1))),
      })
      .collect();
    info
  }

  fn bundle() -> Bundle {
    Bundle { channels_per_flow: 2, fpp: 48, dst_addr: Ipv4Addr::new(239, 69, 1, 2), dst_port: 4321 }
  }

  #[test]
  fn ordinary_names_build_every_record() {
    let info = info_with_channels(2);
    assert!(arc_service(&info, "My-Device").is_ok());
    assert!(cmc_service(&info, "My-Device").is_ok());
    assert!(bundle_service(&info, "My-Device", 1, &bundle()).is_ok());
    assert!(channel_service(&info, None, 0, "TX 1", true, "My-Device").is_ok());
    let pointer =
      PointerToMulticast { tx_hostname: "My-Device".into(), bundle_id: 1, channel_in_bundle: 0 };
    assert!(channel_service(&info, Some(&pointer), 1, "Left", false, "My-Device").is_ok());
  }

  #[test]
  fn a_name_that_is_not_a_dns_label_is_an_error_not_a_panic() {
    let info = info_with_channels(1);
    let long = "a".repeat(64);
    assert!(host_label(&long).is_err());
    assert!(arc_service(&info, &long).is_err());
    assert!(cmc_service(&info, &long).is_err());
    assert!(bundle_service(&info, &long, 1, &bundle()).is_err());
    // a channel name that is fine alone but makes "<channel>@<device>" longer than a label
    assert!(channel_service(&info, None, 0, &"c".repeat(40), true, &"d".repeat(30)).is_err());
    assert!(channel_service(&info, None, 0, &long, true, "dev").is_err());
  }

  #[test]
  fn random_names_never_panic_a_record_builder() {
    let mut rng = SmallRng::seed_from_u64(0x9292);
    let info = info_with_channels(1);
    let (mut built, mut refused) = (0, 0);
    for _ in 0..3000 {
      let len = rng.gen_range(0..90);
      let name: String = (0..len)
        .map(|_| match rng.gen_range(0..6) {
          0 => '.',
          1 => '@',
          2 => '\0',
          3 => char::from_u32(rng.gen_range(0x80..0x3000)).unwrap_or('?'),
          _ => rng.gen_range(b'a'..=b'z') as char,
        })
        .collect();
      let results = [
        arc_service(&info, &name).is_ok(),
        cmc_service(&info, &name).is_ok(),
        bundle_service(&info, &name, 7, &bundle()).is_ok(),
        channel_service(&info, None, 0, &name, true, "dev").is_ok(),
        channel_service(&info, None, 0, "ch", false, &name).is_ok(),
      ];
      for ok in results {
        if ok {
          built += 1
        } else {
          refused += 1
        }
      }
    }
    assert!(built > 0 && refused > 0, "built {built}, refused {refused}");
  }

  #[test]
  fn fixed_names_are_built_from_their_labels() {
    assert_eq!(service_type("_netaudio-arc").to_string(), "_netaudio-arc._udp.local.");
    assert_eq!(in_addr_type().to_string(), "in-addr.local.");
    assert_eq!(multicast_ip_to_name(Ipv4Addr::new(239, 69, 1, 2)).to_string(), "2.1.69.239.");
  }

  #[test]
  fn starting_on_an_address_that_no_interface_has_is_an_error() {
    // TEST-NET-1 (RFC 5737) is never assigned to a real interface.
    let mut info = info_with_channels(1);
    info.ip_address = Ipv4Addr::new(192, 0, 2, 77);
    let result = DeviceMDNSResponder::start(Arc::new(info), Default::default());
    assert!(result.is_err(), "started on an address that does not exist");
  }

  /// ONS fork (ZanyHunter/open-network-soundcard#15, control-plane.md section 7 finding 2): Dante
  /// Controller asks `<device>._netaudio-cmc._udp.local` for its SRV record and the lab capture of an old build
  /// shows no answer. The CMC record must exist, be found under that name, and carry the CMC port.
  #[test]
  fn the_cmc_service_record_is_built_under_the_instance_name_with_the_cmc_port() {
    use searchfire::dns::rr::{RData, RecordType};
    let info = info_with_channels(1);
    let service = cmc_service(&info, "ONS-TEST").unwrap();
    // a question for the instance name (what a controller asks) matches the record, whatever the case
    let asked = Name::from_ascii("ons-test._netaudio-cmc._udp.local.").unwrap();
    assert_eq!(service.service_id(), &asked);
    assert_eq!(service.service_type(), &service_type("_netaudio-cmc"));
    assert_eq!(service.port(), info.cmc_port);

    let response = service.dns_response().unwrap();
    let srv = response
      .answers()
      .iter()
      .find(|r| r.record_type() == RecordType::SRV)
      .expect("no SRV record in the CMC answer");
    assert_eq!(srv.name(), &asked);
    match srv.data() {
      Some(RData::SRV(srv)) => assert_eq!(srv.port(), 8800),
      other => panic!("SRV record without SRV data: {other:?}"),
    }
    assert!(response.answers().iter().any(|r| r.record_type() == RecordType::PTR));
    assert!(response.answers().iter().any(|r| r.record_type() == RecordType::TXT));
    assert!(response.additionals().iter().any(|r| r.record_type() == RecordType::A));
    // and it must encode, because the responder silently skips a record that does not
    use searchfire::dns::serialize::binary::BinEncodable;
    assert!(response.to_bytes().is_ok());
  }

  /// The same question over the wire (a QU question, so the answer comes back unicast to the asker). Skipped
  /// where the host cannot do mDNS on loopback.
  #[test]
  fn the_cmc_srv_question_is_answered_over_the_wire() {
    use searchfire::dns::op::{Message, MessageType, OpCode, Query};
    use searchfire::dns::rr::RecordType;
    use searchfire::dns::serialize::binary::{BinDecodable, BinEncodable};
    use socket2::{Domain, Protocol, SockAddr, Socket, Type};

    let mut info = info_with_channels(1);
    info.ip_address = Ipv4Addr::LOCALHOST;
    let info = Arc::new(info);
    let responder = match DeviceMDNSResponder::start(info.clone(), Default::default()) {
      Ok(r) => r,
      Err(e) => {
        eprintln!("skipped, no mDNS on loopback here: {e}");
        return;
      }
    };
    let hostname = info.friendly_hostname();
    let sock = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP)).unwrap();
    sock.bind(&SockAddr::from(std::net::SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))).unwrap();
    sock.set_multicast_if_v4(&Ipv4Addr::LOCALHOST).unwrap();
    sock.set_multicast_loop_v4(true).unwrap();
    let sock: std::net::UdpSocket = sock.into();
    sock.set_read_timeout(Some(std::time::Duration::from_millis(500))).unwrap();

    let ask = |name: &str, rtype: RecordType| -> Option<Message> {
      let mut query = Query::query(Name::from_ascii(name).unwrap(), rtype);
      query.set_mdns_unicast_response(true);
      let mut message = Message::new();
      message.set_message_type(MessageType::Query).set_op_code(OpCode::Query).add_query(query);
      let bytes = message.to_bytes().unwrap();
      let mut buf = [0u8; 2048];
      for _ in 0..6 {
        sock.send_to(&bytes, (Ipv4Addr::new(224, 0, 0, 251), 5353)).ok()?;
        if let Ok((n, _)) = sock.recv_from(&mut buf) {
          return Message::from_bytes(&buf[..n]).ok();
        }
      }
      None
    };

    // Control: the ARC record is found, so a missing CMC answer is not a host that cannot do mDNS.
    let arc_name = format!("{hostname}._netaudio-arc._udp.local.");
    if ask(&arc_name, RecordType::SRV).is_none() {
      eprintln!("skipped, the responder does not answer on loopback here");
      responder.shutdown_and_join();
      return;
    }
    let cmc_name = format!("{hostname}._netaudio-cmc._udp.local.");
    let answer = ask(&cmc_name, RecordType::SRV).expect("no answer to the _netaudio-cmc SRV question");
    eprintln!("cmc SRV question answered with {} records", answer.answers().len());
    assert!(answer.answers().iter().any(|r| r.record_type() == RecordType::SRV), "{answer:?}");
    let ptr = ask("_netaudio-cmc._udp.local.", RecordType::PTR).expect("no answer to the _netaudio-cmc PTR question");
    assert!(ptr.answers().iter().any(|r| r.record_type() == RecordType::PTR), "{ptr:?}");
    responder.shutdown_and_join();
  }

  /// A responder over the loopback interface (skipped where the host cannot do mDNS on loopback).
  /// The requests a controller can make about names and channels that do not exist or are not valid
  /// must not panic, and shutting down twice is harmless.
  #[test]
  fn requests_about_bad_names_and_channels_do_not_panic_the_responder() {
    let mut info = info_with_channels(2);
    info.ip_address = Ipv4Addr::LOCALHOST;
    let info = Arc::new(info);
    let responder = match DeviceMDNSResponder::start(info.clone(), Default::default()) {
      Ok(r) => r,
      Err(e) => {
        eprintln!("skipped, no mDNS on loopback here: {e}");
        return;
      }
    };
    responder.add_tx_channel(0);
    responder.add_tx_channel(99); // does not exist
    responder.remove_tx_channel(99);
    // a channel renamed to something that cannot be a DNS label
    *info.tx_channels[1].friendly_name.write().unwrap() = "x".repeat(80);
    responder.add_tx_channel(1);
    responder.remove_tx_channel(1);
    responder.add_multicast_bundle(1, 2, 48, Ipv4Addr::new(239, 69, 1, 2), 4321);
    responder.remove_multicast_bundle(1);
    responder.reserve_multicast_ip(Ipv4Addr::new(239, 69, 1, 2));
    responder.remove_multicast_ip(Ipv4Addr::new(239, 69, 1, 2));
    responder.rename_device("Renamed");
    assert_eq!(info.friendly_hostname(), "Renamed");
    responder.shutdown_and_join();
    responder.shutdown_and_join();
    // after the shutdown the requests are logged, not fatal
    responder.add_tx_channel(0);
    responder.rename_device("After-Shutdown");
  }
}
