use searchfire::{
  broadcast::{BroadcasterBuilder, BroadcasterHandle, Service, ServiceBuilder},
  dns::rr::Name,
  net::{IpVersion, TargetInterface},
};
use std::{
  collections::{BTreeMap, BTreeSet},
  net::{IpAddr, Ipv4Addr},
  sync::{Arc, Mutex, RwLock},
};

use super::flows_tx::{FPP_MAX_ADVERTISED, FPP_MIN, MAX_CHANNELS_IN_FLOW};
use crate::{
  device_info::DeviceInfo,
  mdns_client::{self_origin_from_self_info, MdnsClient, PointerToMulticast},
  utils::LogAndForget,
};

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

pub fn service_type(st: &str) -> Name {
  Name::from_labels([st, "_udp", "local"].iter().map(|&s| s.as_bytes())).unwrap()
}
pub fn in_addr_type() -> Name {
  Name::from_labels(["in-addr", "local"].iter().map(|&s| s.as_bytes())).unwrap()
}
fn multicast_ip_to_name(addr: Ipv4Addr) -> Name {
  let octets = addr.octets();
  Name::from_labels(
    [&octets[3].to_string(), &octets[2].to_string(), &octets[1].to_string(), &octets[0].to_string()]
      .iter()
      .map(|&s| s.as_bytes()),
  )
  .unwrap()
}

fn host_label(name: &str) -> Name {
  Name::from_labels([name.as_bytes()]).unwrap()
}

fn arc_service(self_info: &DeviceInfo, hostname: &str) -> Service {
  ServiceBuilder::new(service_type("_netaudio-arc"), host_label(hostname), self_info.arc_port)
    .unwrap()
    .add_ip_address(IpAddr::V4(self_info.ip_address))
    .add_txt_truncated("arcp_vers=2.7.41")
    .add_txt_truncated("arcp_min=0.2.4")
    .add_txt_truncated("router_vers=4.0.2")
    .add_txt_truncated(kv("router_info", &self_info.board_name))
    .add_txt_truncated(kv("mf", &self_info.manufacturer))
    .add_txt_truncated(kv("model", &self_info.model_number))
    .ttl(4500)
    .build()
    .unwrap()
}

fn cmc_service(self_info: &DeviceInfo, hostname: &str) -> Service {
  ServiceBuilder::new(service_type("_netaudio-cmc"), host_label(hostname), self_info.cmc_port)
    .unwrap()
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
    .unwrap()
}

fn bundle_service(self_info: &DeviceInfo, hostname: &str, bundle_id: usize, b: &Bundle) -> Service {
  ServiceBuilder::new(
    service_type("_netaudio-bund"),
    host_label(&format!("{bundle_id}@{hostname}")),
    self_info.flows_control_port,
  )
  .unwrap()
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
  .unwrap()
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
  pub fn start(
    self_info: Arc<DeviceInfo>,
    multicasts_by_channel: Arc<RwLock<BTreeMap<usize, PointerToMulticast>>>,
  ) -> Self {
    let hostname = self_info.friendly_hostname();
    let bb = BroadcasterBuilder::new()
      .loopback()
      .interface_v4(TargetInterface::Specific(self_info.ip_address))
      .add_service(arc_service(&self_info, &hostname))
      .add_service(cmc_service(&self_info, &hostname));

    let handle = bb.build(IpVersion::V4).unwrap().run_in_background();

    Self {
      handle: RwLock::new(Some(handle)),
      self_origin: self_origin_from_self_info(&self_info),
      self_info,
      multicasts_by_channel,
      announced: Mutex::new(Announced::default()),
    }
  }

  /// The `_netaudio-chan` service of channel `index` under the name `ch_name`.
  fn channel_service(&self, index: usize, ch_name: &str, default: bool, hostname: &str) -> Service {
    let self_info = &*self.self_info;
    let bundle = self
      .multicasts_by_channel
      .read()
      .unwrap()
      .get(&index)
      .map(|p| format!("b.{}={}", p.bundle_id, p.channel_in_bundle + 1));
    let name = host_label(&format!("{ch_name}@{hostname}"));
    let mut b = ServiceBuilder::new(service_type("_netaudio-chan"), name, self_info.flows_control_port)
      .unwrap()
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
    b.build().unwrap()
  }

  /// The channel names a transmit channel is advertised under: the factory name, plus the friendly
  /// name when it differs.
  fn channel_names(&self, index: usize) -> (String, Option<String>) {
    let txch = &self.self_info.tx_channels[index];
    let friendly = txch.friendly_name.read().unwrap().clone();
    let extra = (txch.factory_name != friendly).then_some(friendly);
    (txch.factory_name.clone(), extra)
  }

  fn add_tx_channel_services(&self, handle: &BroadcasterHandle, index: usize, hostname: &str) {
    let (factory, friendly) = self.channel_names(index);
    handle.add_service(self.channel_service(index, &factory, true, hostname)).log_and_forget();
    if let Some(friendly) = friendly {
      handle.add_service(self.channel_service(index, &friendly, false, hostname)).log_and_forget();
    }
  }

  fn remove_tx_channel_services(&self, handle: &BroadcasterHandle, index: usize, hostname: &str) {
    let (factory, friendly) = self.channel_names(index);
    for ch_name in std::iter::once(factory).chain(friendly) {
      handle
        .remove_named_service(service_type("_netaudio-chan"), host_label(&format!("{ch_name}@{hostname}")))
        .log_and_forget();
    }
  }

  pub fn add_tx_channel(&self, index: usize) {
    let mut announced = self.announced.lock().unwrap();
    let hostname = self.self_info.friendly_hostname();
    let handle = self.handle.read().unwrap();
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
    let mut announced = self.announced.lock().unwrap();
    let hostname = self.self_info.friendly_hostname();
    match self.handle.read().unwrap().as_ref() {
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
    let mut announced = self.announced.lock().unwrap();
    let hostname = self.self_info.friendly_hostname();
    let bundle = Bundle { channels_per_flow, fpp, dst_addr, dst_port };
    let service = bundle_service(&self.self_info, &hostname, bundle_id, &bundle);
    let handle = self.handle.read().unwrap();
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
    let mut announced = self.announced.lock().unwrap();
    let hostname = self.self_info.friendly_hostname();
    let name = host_label(&format!("{bundle_id}@{hostname}"));
    let handle = self.handle.read().unwrap();
    match handle.as_ref() {
      Some(handle) => {
        handle.remove_named_service(service_type("_netaudio-bund"), name).log_and_forget();
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
    let announced = self.announced.lock().unwrap();
    let old_name = self.self_info.friendly_hostname();
    let handle = self.handle.read().unwrap();
    let Some(handle) = handle.as_ref() else {
      // Already shut down: nothing is advertised, only the name changes.
      self.self_info.set_friendly_hostname(new_name);
      return;
    };
    handle.remove_named_service(service_type("_netaudio-arc"), host_label(&old_name)).log_and_forget();
    handle.remove_named_service(service_type("_netaudio-cmc"), host_label(&old_name)).log_and_forget();
    for index in &announced.tx_channels {
      self.remove_tx_channel_services(handle, *index, &old_name);
    }
    for bundle_id in announced.bundles.keys() {
      handle
        .remove_named_service(
          service_type("_netaudio-bund"),
          host_label(&format!("{bundle_id}@{old_name}")),
        )
        .log_and_forget();
    }

    self.self_info.set_friendly_hostname(new_name);

    // The responder only answers queries, so tell the network about the new records unprompted: a
    // controller that has just dropped the old device (goodbye) would otherwise wait for its next browse.
    let mut announce = vec![arc_service(&self.self_info, new_name), cmc_service(&self.self_info, new_name)];
    for index in &announced.tx_channels {
      let (factory, friendly) = self.channel_names(*index);
      announce.push(self.channel_service(*index, &factory, true, new_name));
      announce.extend(friendly.map(|f| self.channel_service(*index, &f, false, new_name)));
    }
    for (bundle_id, bundle) in &announced.bundles {
      announce.push(bundle_service(&self.self_info, new_name, *bundle_id, bundle));
    }
    for service in &announce {
      handle.add_service(service.clone()).log_and_forget();
    }
    announce_unsolicited(self.self_info.ip_address, announce);
  }

  pub fn reserve_multicast_ip(&self, addr: Ipv4Addr) {
    let self_info = &*self.self_info;
    let b = ServiceBuilder::new(in_addr_type(), multicast_ip_to_name(addr), 0)
      .unwrap()
      .add_ip_address(IpAddr::V4(self_info.ip_address))
      .add_additional_txt(
        Name::from_labels(["_inferno-response-origin", "local"]).unwrap(),
        self.self_origin.clone(),
      );
    let handle = self.handle.read().unwrap();
    match handle.as_ref() {
      Some(handle) => {
        handle.add_service(b.build().unwrap()).log_and_forget();
      }
      None => {
        log::error!("BUG: trying to reserve multicast IP using BroadcasterHandle after it was shut down");
      }
    }
  }
  pub fn remove_multicast_ip(&self, addr: Ipv4Addr) {
    let handle = self.handle.read().unwrap();
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
    self
      .handle
      .write()
      .unwrap()
      .take()
      .expect("shutting down more than once")
      .shutdown()
      .log_and_forget();
  }
}
