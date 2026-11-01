use std::{
  net::Ipv4Addr,
  sync::{Arc, RwLock},
};

use netdev::mac::MacAddr;

#[derive(Clone)]
pub struct Channel {
  pub factory_name: String,
  pub friendly_name: Arc<RwLock<String>>, // Arc is needed only because of Clone requirement, TODO: fix the ALSA plugin
}

pub type DeviceId = [u8; 8];

/// What the device tells controllers about itself beyond names and channels (ONS fork).
///
/// The values answer the settings queries of Dante Controller and `netaudio` (see
/// `protocol::proto_arc::device_settings`): the latency range and default, and the capability flags
/// of the conmon board-info reply.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeviceCapabilities {
  /// Smallest receive latency a controller may be told about, in nanoseconds.
  pub latency_min_ns: usize,
  /// Largest receive latency a controller may be told about, in nanoseconds.
  pub latency_max_ns: usize,
  /// The factory default receive latency, in nanoseconds.
  pub latency_default_ns: usize,
  /// Bytes `0x14..0x18` of the board-info reply (opcode `07 2a 00 60`): the feature flags that
  /// Dante Controller reads to decide which controls to enable. The default advertises only the
  /// manufacturer-name bit (`0x10` in the third byte), as upstream Inferno did.
  pub board_flags: [u8; 4],
}

impl DeviceCapabilities {
  /// A device with one fixed latency and no optional features.
  pub fn fixed_latency(latency_ns: usize) -> Self {
    Self {
      latency_min_ns: latency_ns,
      latency_max_ns: latency_ns,
      latency_default_ns: latency_ns,
      board_flags: [0, 0, 0x10, 0],
    }
  }
}

#[derive(Clone)] // TODO: this shouldn't need to be clonable, fix the ALSA plugin
pub struct DeviceInfo {
  pub ip_address: Ipv4Addr,
  pub netmask: Ipv4Addr,
  pub gateway: Ipv4Addr,
  pub mac_address: MacAddr,
  pub link_speed: u16,

  pub board_name: String,
  pub manufacturer: String,
  pub model_name: String,
  pub model_number: String, // _000000000000000b
  pub factory_device_id: DeviceId,
  pub process_id: u16,
  pub vendor_string: String,
  /// The user-visible device name. Shared and mutable because the ONS fork renames the device at
  /// run time (ARC opcode `0x1001`); read it with [`DeviceInfo::friendly_hostname`]. Clones of the
  /// `DeviceInfo` share the name.
  pub friendly_hostname: Arc<RwLock<String>>,
  pub factory_hostname: String, // TODO limit length to 31, otherwise DC ignores the device

  pub rx_channels: Vec<Channel>,
  pub tx_channels: Vec<Channel>,
  pub bits_per_sample: u8,
  pub pcm_type: u8, // usually 0xe, in older devices 4
  pub latency_ns: usize,
  pub sample_rate: u32,
  /// ONS fork: latency range and capability flags reported to controllers.
  pub capabilities: DeviceCapabilities,

  pub arc_port: u16,
  pub cmc_port: u16,
  pub flows_control_port: u16,
  pub info_request_port: u16,
}

impl DeviceInfo {
  /// The current device name (see the field of the same name).
  pub fn friendly_hostname(&self) -> String {
    self.friendly_hostname.read().unwrap_or_else(|e| e.into_inner()).clone()
  }

  /// Replace the device name. Only the rename path calls this; the mDNS records have to follow.
  pub(crate) fn set_friendly_hostname(&self, name: &str) {
    *self.friendly_hostname.write().unwrap_or_else(|e| e.into_inner()) = name.to_owned();
  }

  pub fn latency_samples(&self) -> usize {
    self.latency_ns * (self.sample_rate as usize) / 1_000_000_000
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn dummy_device_info(latency_ns: usize, sample_rate: u32) -> DeviceInfo {
    DeviceInfo {
      ip_address: Ipv4Addr::UNSPECIFIED,
      netmask: Ipv4Addr::UNSPECIFIED,
      gateway: Ipv4Addr::UNSPECIFIED,
      mac_address: MacAddr::zero(),
      link_speed: 0,
      board_name: String::new(),
      manufacturer: String::new(),
      model_name: String::new(),
      model_number: String::new(),
      factory_device_id: [0; 8],
      process_id: 0,
      vendor_string: String::new(),
      friendly_hostname: Default::default(),
      factory_hostname: String::new(),
      rx_channels: Vec::new(),
      tx_channels: Vec::new(),
      bits_per_sample: 0,
      pcm_type: 0,
      latency_ns,
      sample_rate,
      capabilities: DeviceCapabilities::fixed_latency(latency_ns),
      arc_port: 0,
      cmc_port: 0,
      flows_control_port: 0,
      info_request_port: 0,
    }
  }

  #[test]
  fn zero_latency_ns_returns_zero() {
    let device = dummy_device_info(0, 48_000);
    assert_eq!(device.latency_samples(), 0);
  }

  #[test]
  fn zero_sample_rate_returns_zero() {
    let device = dummy_device_info(1_000_000_000, 0);
    assert_eq!(device.latency_samples(), 0);
  }

  #[test]
  fn one_second_at_48000_hz() {
    let device = dummy_device_info(1_000_000_000, 48_000);
    assert_eq!(device.latency_samples(), 48_000);
  }

  #[test]
  fn one_second_at_44100_hz() {
    let device = dummy_device_info(1_000_000_000, 44_100);
    assert_eq!(device.latency_samples(), 44_100);
  }

  #[test]
  fn five_ms_at_48000_hz() {
    let device = dummy_device_info(5_000_000, 48_000);
    assert_eq!(device.latency_samples(), 240);
  }

  #[test]
  fn large_values_no_overflow() {
    let device = dummy_device_info(1_000_000_000, 44100 * 1024);
    let result = device.latency_samples();
    assert!(result <= usize::MAX);
  }
}
