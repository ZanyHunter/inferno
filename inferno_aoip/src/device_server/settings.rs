//! Device settings: a typed [`SettingsBuilder`] (ONS fork, issues W4 and W5) plus the upstream
//! env-var / key-value path, which now builds on the same builder.
//!
//! * [`SettingsBuilder::build`] never panics on bad input: every problem comes back as a
//!   [`SettingsError`].
//! * The network adapter is chosen with a [`NicSelector`] (GUID, IPv4 address or friendly name; plain
//!   data supplied by the host application). Tests and hosts that already know the adapter details
//!   can supply a [`NicInfo`] instead, which skips the operating system lookup entirely.
//! * The persistent state directory is injectable ([`SettingsBuilder::state_dir`]); see
//!   [`default_state_dir`] for the default.
//! * [`Settings::new`] / [`Settings::try_new`] keep the upstream `BTreeMap` + `INFERNO_*` environment
//!   variable interface working (Linux users, ALSA plugin, `inferno2pipe`).

use std::{
  collections::BTreeMap,
  env,
  ffi::OsString,
  net::{IpAddr, Ipv4Addr},
  path::PathBuf,
  str::FromStr,
  sync::{Arc, RwLock},
};

use netdev::mac::MacAddr;

use crate::common::warn;
use crate::device_info::{Channel, DeviceId, DeviceInfo};
use crate::protocol::flows_control::PORT as FLOWS_CONTROL_PORT;
use crate::protocol::mcast::INFO_REQUEST_PORT;
use crate::protocol::proto_arc::PORT as ARC_PORT;
use crate::protocol::proto_cmc::PORT as CMC_PORT;

/// Longest device name Dante Controller accepts (the upstream TODO notes that DC ignores devices
/// with longer names) **(verify exact limit against Dante Controller)**.
pub const MAX_DEVICE_NAME_LEN: usize = 31;

/// Folder name used under `%ProgramData%` on Windows (ONS ADR-006).
pub const WINDOWS_STATE_DIR_NAME: &str = "OpenNetworkSoundcard";

/// Everything that can go wrong while building [`Settings`].
#[derive(Debug, thiserror::Error)]
pub enum SettingsError {
  /// No usable local IPv4 address could be determined (automatic NIC selection).
  #[error("no usable network interface: {0}")]
  NoNetworkInterface(String),
  /// The selected adapter does not exist (by GUID, name or IPv4 address).
  #[error("network interface not found: {0}")]
  NicNotFound(String),
  /// The selected adapter has no IPv4 address.
  #[error("network interface {0} has no IPv4 address")]
  NicHasNoIpv4(String),
  /// The device name does not follow the Dante name rules (see [`validate_device_name`]).
  #[error("invalid device name {name:?}: {reason}")]
  InvalidDeviceName { name: String, reason: &'static str },
  /// A numeric/boolean/hex setting could not be parsed or is out of range.
  #[error("invalid setting {key}={value:?}: {expected}")]
  InvalidSetting { key: String, value: String, expected: &'static str },
  /// No persistent state directory could be determined (`%ProgramData%` unset on Windows, or no
  /// platform state directory elsewhere). Inject one with [`SettingsBuilder::state_dir`].
  #[error("cannot determine the state directory: {0}")]
  NoStateDir(&'static str),
}

fn invalid(key: &str, value: impl ToString, expected: &'static str) -> SettingsError {
  SettingsError::InvalidSetting { key: key.to_owned(), value: value.to_string(), expected }
}

// ---------------------------------------------------------------------------------------------
// Dante name rules
// ---------------------------------------------------------------------------------------------

/// Check a device name against the Dante name rules used by ONS (ADR-007): 1 to
/// [`MAX_DEVICE_NAME_LEN`] characters from `[A-Za-z0-9-]`, not starting or ending with `-`
/// **(verify against Dante Controller; the rules are not publicly documented)**.
pub fn validate_device_name(name: &str) -> Result<(), SettingsError> {
  let bad = |reason| SettingsError::InvalidDeviceName { name: name.to_owned(), reason };
  if name.is_empty() {
    return Err(bad("name is empty"));
  }
  if name.chars().count() > MAX_DEVICE_NAME_LEN {
    return Err(bad("name is longer than 31 characters"));
  }
  if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-') {
    return Err(bad("only ASCII letters, digits and '-' are allowed"));
  }
  if name.starts_with('-') || name.ends_with('-') {
    return Err(bad("name must not start or end with '-'"));
  }
  Ok(())
}

/// Reduce an arbitrary string to a valid Dante device name (possibly empty): ASCII letters and digits
/// are kept, each run of other characters becomes one `-`, edge hyphens are removed and the result
/// is cut to [`MAX_DEVICE_NAME_LEN`] characters. Same algorithm as `ons_dante::identity::sanitize_name`.
pub fn sanitize_device_name(input: &str) -> String {
  let mut out = String::new();
  let mut pending_hyphen = false;
  for c in input.chars() {
    if c.is_ascii_alphanumeric() {
      if pending_hyphen && !out.is_empty() {
        out.push('-');
      }
      pending_hyphen = false;
      out.push(c);
      if out.len() >= MAX_DEVICE_NAME_LEN {
        break;
      }
    } else {
      pending_hyphen = true;
    }
  }
  out.truncate(MAX_DEVICE_NAME_LEN);
  while out.ends_with('-') {
    out.pop();
  }
  out
}

/// Cut a string to at most `max` characters (never in the middle of a UTF-8 sequence).
fn truncate_chars(s: &str, max: usize) -> &str {
  match s.char_indices().nth(max) {
    Some((idx, _)) => &s[..idx],
    None => s,
  }
}

// ---------------------------------------------------------------------------------------------
// State directory
// ---------------------------------------------------------------------------------------------

/// `<ProgramData>\OpenNetworkSoundcard`, from the value of the `ProgramData` environment variable.
/// Pure function so it can be tested without touching the real environment.
pub fn program_data_state_dir(program_data: Option<OsString>) -> Result<PathBuf, SettingsError> {
  match program_data {
    Some(base) if !base.is_empty() => Ok(PathBuf::from(base).join(WINDOWS_STATE_DIR_NAME)),
    _ => Err(SettingsError::NoStateDir("the ProgramData environment variable is not set")),
  }
}

/// Default location of the persistent state.
///
/// * Windows: `%ProgramData%\OpenNetworkSoundcard` (ONS ADR-006). It is machine-wide and works for a
///   service running as `LocalSystem`, which has no useful user profile.
/// * Elsewhere: the `platform_dirs` state directory of `inferno_aoip`, as upstream does
///   (`~/.local/state/inferno_aoip` on Linux).
///
/// Inferno writes `<state dir>/<device id hex>/rx_channels.toml` etc. below this directory.
/// Hosts should normally call [`SettingsBuilder::state_dir`] instead of relying on the default.
pub fn default_state_dir() -> Result<PathBuf, SettingsError> {
  #[cfg(windows)]
  {
    program_data_state_dir(env::var_os("ProgramData"))
  }
  #[cfg(not(windows))]
  {
    platform_dirs::AppDirs::new(Some("inferno_aoip"), false)
      .map(|dirs| dirs.state_dir)
      .ok_or(SettingsError::NoStateDir("no platform state directory"))
  }
}

// ---------------------------------------------------------------------------------------------
// Network adapter selection
// ---------------------------------------------------------------------------------------------

/// Which network adapter to bind to. Plain data; the host application decides what to pass.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NicSelector {
  /// The adapter that owns the machine's primary local IPv4 address (upstream behaviour).
  Auto,
  /// Adapter GUID, e.g. `{3F2B1C0A-...}` (braces and case ignored). Windows adapter names are GUIDs.
  Guid(String),
  /// An IPv4 address assigned to the adapter.
  Ipv4(Ipv4Addr),
  /// Friendly name (e.g. `Ethernet 2`, case ignored) or the OS interface name (e.g. `eth0`).
  Name(String),
}

/// Resolved adapter details, as put into the device info that Dante Controller shows.
/// Hosts that already know the adapter can pass this to [`SettingsBuilder::nic_info`] to bypass the
/// operating system lookup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NicInfo {
  /// The address the device binds to and advertises.
  pub ipv4: Ipv4Addr,
  /// `0.0.0.0` when unknown.
  pub netmask: Ipv4Addr,
  /// `0.0.0.0` when unknown.
  pub gateway: Ipv4Addr,
  /// `None` when the adapter has no MAC.
  pub mac: Option<[u8; 6]>,
  /// Link speed in Mbit/s, 0 when unknown.
  pub link_speed_mbps: u32,
}

impl NicInfo {
  /// Only the address is known.
  pub fn new(ipv4: Ipv4Addr) -> Self {
    Self {
      ipv4,
      netmask: Ipv4Addr::UNSPECIFIED,
      gateway: Ipv4Addr::UNSPECIFIED,
      mac: None,
      link_speed_mbps: 0,
    }
  }
}

fn normalize_guid(s: &str) -> String {
  s.trim().trim_start_matches('{').trim_end_matches('}').to_ascii_lowercase()
}

fn iface_matches(iface: &netdev::Interface, selector: &NicSelector) -> bool {
  match selector {
    NicSelector::Auto => false,
    NicSelector::Guid(g) => normalize_guid(&iface.name) == normalize_guid(g),
    NicSelector::Ipv4(ip) => iface.ipv4.iter().any(|n| n.addr() == *ip),
    NicSelector::Name(n) => {
      let n = n.trim();
      iface.name == n || iface.friendly_name.as_deref().is_some_and(|f| f.eq_ignore_ascii_case(n))
    }
  }
}

fn nic_info_from_iface(iface: &netdev::Interface, ip: Ipv4Addr) -> NicInfo {
  let mut info = NicInfo::new(ip);
  if let Some(net) = iface.ipv4.iter().find(|n| n.addr() == ip) {
    info.netmask = net.netmask();
  }
  let speed_bps = iface.transmit_speed.unwrap_or(0).max(iface.receive_speed.unwrap_or(0));
  info.link_speed_mbps = u32::try_from(speed_bps / 1_000_000).unwrap_or(u32::MAX);
  if let Some(gws) = &iface.gateway {
    let mask = info.netmask.to_bits();
    if let Some(gw) = gws.ipv4.iter().find(|gw| (gw.to_bits() & mask) == (ip.to_bits() & mask)) {
      info.gateway = *gw;
    }
  }
  info.mac = iface.mac_addr.map(|m| [m.0, m.1, m.2, m.3, m.4, m.5]);
  info
}

/// Resolve `selector` against a list of adapters. `local_ip` is only consulted for
/// [`NicSelector::Auto`]. With `lenient`, an IPv4 address that matches no adapter is accepted as is
/// (upstream behaviour, used by the env-var path); otherwise it is an error.
fn resolve_nic(
  selector: &NicSelector,
  interfaces: &[netdev::Interface],
  local_ip: impl FnOnce() -> Option<Ipv4Addr>,
  lenient: bool,
) -> Result<NicInfo, SettingsError> {
  match selector {
    NicSelector::Auto => {
      let ip = local_ip().ok_or_else(|| {
        SettingsError::NoNetworkInterface(
          "unknown local IPv4 address, select a network interface explicitly".into(),
        )
      })?;
      Ok(match interfaces.iter().find(|i| i.ipv4.iter().any(|n| n.addr() == ip)) {
        Some(iface) => nic_info_from_iface(iface, ip),
        None => NicInfo::new(ip),
      })
    }
    NicSelector::Ipv4(ip) => match interfaces.iter().find(|i| iface_matches(i, selector)) {
      Some(iface) => Ok(nic_info_from_iface(iface, *ip)),
      None if lenient => Ok(NicInfo::new(*ip)),
      None => Err(SettingsError::NicNotFound(ip.to_string())),
    },
    NicSelector::Guid(s) | NicSelector::Name(s) => {
      let iface = interfaces
        .iter()
        .find(|i| iface_matches(i, selector))
        .ok_or_else(|| SettingsError::NicNotFound(s.clone()))?;
      let ip = iface.ipv4.first().ok_or_else(|| SettingsError::NicHasNoIpv4(s.clone()))?.addr();
      Ok(nic_info_from_iface(iface, ip))
    }
  }
}

fn system_local_ipv4() -> Option<Ipv4Addr> {
  match local_ip_address::local_ip() {
    Ok(IpAddr::V4(a)) => Some(a),
    _ => None,
  }
}

// ---------------------------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------------------------

/// Typed, fallible construction of [`Settings`]. All setters are infallible; everything is checked in
/// [`SettingsBuilder::build`].
///
/// ```no_run
/// use inferno_aoip::device_server::{NicSelector, SettingsBuilder};
/// let settings = SettingsBuilder::new("My Device", "MyDev")
///   .nic(NicSelector::Name("Ethernet 2".into()))
///   .device_name("My-Device")
///   .rx_channels(16)
///   .tx_channels(16)
///   .state_dir("C:\\ProgramData\\OpenNetworkSoundcard")
///   .build()?;
/// # Ok::<(), inferno_aoip::device_server::SettingsError>(())
/// ```
#[derive(Clone, Debug)]
pub struct SettingsBuilder {
  app_name: String,
  short_app_name: String,
  nic: NicSelector,
  nic_info: Option<NicInfo>,
  device_id: Option<DeviceId>,
  mac_for_device_id: Option<[u8; 6]>,
  process_id: u16,
  device_name: Option<String>,
  manufacturer: Option<String>,
  model_name: Option<String>,
  sample_rate: u32,
  rx_latency_ns: usize,
  tx_latency_ns: u32,
  alt_port: Option<u16>,
  rx_channels: u16,
  tx_channels: u16,
  clock_path: Option<PathBuf>,
  use_safe_clock: bool,
  tx_source_bit_depth: u8,
  state_dir: Option<PathBuf>,
  /// Upstream compatibility: names are not validated (only cut to 31 characters), the default name
  /// keeps its historic `<app> <hex ip>` form, and an IPv4 address that matches no adapter is accepted.
  legacy: bool,
}

impl SettingsBuilder {
  /// `app_name` is the model name shown in Dante Controller, `short_app_name` is used in the factory
  /// hostname (cut to 14 characters).
  pub fn new(app_name: &str, short_app_name: &str) -> Self {
    Self {
      app_name: app_name.to_owned(),
      short_app_name: short_app_name.to_owned(),
      nic: NicSelector::Auto,
      nic_info: None,
      device_id: None,
      mac_for_device_id: None,
      process_id: 0,
      device_name: None,
      manufacturer: None,
      model_name: None,
      sample_rate: 48000,
      rx_latency_ns: 10_000_000,
      tx_latency_ns: 10_000_000,
      alt_port: None,
      rx_channels: 2,
      tx_channels: 2,
      clock_path: None,
      use_safe_clock: false,
      tx_source_bit_depth: 32,
      state_dir: None,
      legacy: false,
    }
  }

  /// Select the adapter. Default: [`NicSelector::Auto`].
  pub fn nic(mut self, selector: NicSelector) -> Self {
    self.nic = selector;
    self
  }

  /// Use already-resolved adapter details. Takes precedence over [`SettingsBuilder::nic`] and skips
  /// every operating system lookup (used by tests and by hosts that enumerate adapters themselves).
  pub fn nic_info(mut self, info: NicInfo) -> Self {
    self.nic_info = Some(info);
    self
  }

  /// Set the 8-byte device ID explicitly.
  pub fn device_id(mut self, id: DeviceId) -> Self {
    self.device_id = Some(id);
    self
  }

  /// Derive the device ID from the NIC MAC: the six MAC bytes followed by the big-endian
  /// [`process_id`](SettingsBuilder::process_id) (0 by default, so `mac ++ 00 00`). Real Dante devices
  /// usually use the MAC padded with zeros **(verify the exact layout against a hardware device;
  /// some IDs may instead use an EUI-64 style `ff:fe` fill)**. An explicit
  /// [`device_id`](SettingsBuilder::device_id) wins.
  pub fn device_id_from_mac(mut self, mac: [u8; 6]) -> Self {
    self.mac_for_device_id = Some(mac);
    self
  }

  /// Process ID, to run several devices on one host. Default 0.
  pub fn process_id(mut self, id: u16) -> Self {
    self.process_id = id;
    self
  }

  /// Device name as shown in Dante Controller. Must follow the Dante name rules
  /// ([`validate_device_name`]); checked in `build`. Default: sanitized app name plus the hex IP.
  pub fn device_name(mut self, name: &str) -> Self {
    self.device_name = Some(name.to_owned());
    self
  }

  /// Manufacturer string (default `Inferno-AoIP`).
  pub fn manufacturer(mut self, manufacturer: &str) -> Self {
    self.manufacturer = Some(manufacturer.to_owned());
    self
  }

  /// Model name (default: the app name).
  pub fn model_name(mut self, model: &str) -> Self {
    self.model_name = Some(model.to_owned());
    self
  }

  /// Sample rate in Hz (default 48000).
  pub fn sample_rate(mut self, hz: u32) -> Self {
    self.sample_rate = hz;
    self
  }

  /// Receive latency in nanoseconds (default 10 ms).
  pub fn rx_latency_ns(mut self, ns: usize) -> Self {
    self.rx_latency_ns = ns;
    self
  }

  /// Transmit latency in nanoseconds (default 10 ms).
  pub fn tx_latency_ns(mut self, ns: u32) -> Self {
    self.tx_latency_ns = ns;
    self
  }

  /// Base port for ARC, CMC, flow control and info requests (`port`, `port+1`, `port+2`, `port+3`),
  /// instead of the standard Dante ports. Mainly for running several devices on one host.
  pub fn alt_port(mut self, port: u16) -> Self {
    self.alt_port = Some(port);
    self
  }

  /// Number of receive channels (default 2).
  pub fn rx_channels(mut self, count: u16) -> Self {
    self.rx_channels = count;
    self
  }

  /// Number of transmit channels (default 2).
  pub fn tx_channels(mut self, count: u16) -> Self {
    self.tx_channels = count;
    self
  }

  /// Path of the clock socket (Unix) passed to the clock receiver. Default: none.
  pub fn clock_path(mut self, path: impl Into<PathBuf>) -> Self {
    self.clock_path = Some(path.into());
    self
  }

  /// Use the safe clock mode. Default false.
  pub fn use_safe_clock(mut self, yes: bool) -> Self {
    self.use_safe_clock = yes;
    self
  }

  /// Bit depth of the TX source for dithering: 16, 24 or 32 (default 32).
  pub fn tx_source_bit_depth(mut self, bits: u8) -> Self {
    self.tx_source_bit_depth = bits;
    self
  }

  /// Directory for persistent state (channel names, subscriptions). Inferno creates one sub-folder
  /// per device ID inside it. Default: [`default_state_dir`]. Tests must always set this to a
  /// temporary directory.
  pub fn state_dir(mut self, dir: impl Into<PathBuf>) -> Self {
    self.state_dir = Some(dir.into());
    self
  }

  /// Build [`Settings`], looking up the adapter with the operating system if needed.
  pub fn build(self) -> Result<Settings, SettingsError> {
    // Only enumerate adapters when the host did not hand us the details.
    let interfaces = if self.nic_info.is_some() { Vec::new() } else { netdev::get_interfaces() };
    self.build_with(&interfaces, system_local_ipv4)
  }

  /// [`SettingsBuilder::build`] against an explicit adapter list (test seam, no OS access except
  /// for [`default_state_dir`] when no state dir was set).
  fn build_with(
    self,
    interfaces: &[netdev::Interface],
    local_ip: impl FnOnce() -> Option<Ipv4Addr>,
  ) -> Result<Settings, SettingsError> {
    if self.sample_rate == 0 {
      return Err(invalid("sample_rate", self.sample_rate, "must be greater than 0"));
    }
    if self.rx_latency_ns == 0 {
      return Err(invalid("rx_latency_ns", self.rx_latency_ns, "must be greater than 0"));
    }
    if self.tx_latency_ns == 0 {
      return Err(invalid("tx_latency_ns", self.tx_latency_ns, "must be greater than 0"));
    }
    if !matches!(self.tx_source_bit_depth, 16 | 24 | 32) {
      return Err(invalid("tx_source_bit_depth", self.tx_source_bit_depth, "must be one of 16, 24, 32"));
    }
    if let Some(port) = self.alt_port {
      if port < 1024 || port > u16::MAX - 3 {
        return Err(invalid("alt_port", port, "must be between 1024 and 65532"));
      }
    }

    let nic = match &self.nic_info {
      Some(info) => info.clone(),
      None => resolve_nic(&self.nic, interfaces, local_ip, self.legacy)?,
    };

    let device_id = match (self.device_id, self.mac_for_device_id) {
      (Some(id), _) => id,
      (None, Some(mac)) => {
        let mut id = [0u8; 8];
        id[..6].copy_from_slice(&mac);
        id[6..8].copy_from_slice(&self.process_id.to_be_bytes());
        id
      }
      (None, None) => {
        // Upstream default: 0000 <ip> <process id>.
        let mut id = [0u8; 8];
        id[2..6].copy_from_slice(&nic.ipv4.octets());
        id[6..8].copy_from_slice(&self.process_id.to_be_bytes());
        id
      }
    };

    let friendly_hostname = match (&self.device_name, self.legacy) {
      (Some(name), true) => truncate_chars(name, MAX_DEVICE_NAME_LEN).to_owned(),
      (Some(name), false) => {
        validate_device_name(name)?;
        name.clone()
      }
      (None, true) => {
        format!("{} {}", truncate_chars(&self.app_name, 22), hex::encode(nic.ipv4.octets()))
      }
      (None, false) => {
        let mut base = sanitize_device_name(truncate_chars(&self.app_name, 22));
        if base.is_empty() {
          base.push_str("Inferno");
        }
        format!("{base}-{}", hex::encode(nic.ipv4.octets()))
      }
    };

    let state_dir = match self.state_dir {
      Some(dir) => dir,
      None => default_state_dir()?,
    };

    let short_app_name = truncate_chars(&self.short_app_name, 14);

    let mut self_info = DeviceInfo {
      ip_address: nic.ipv4,
      netmask: nic.netmask,
      gateway: nic.gateway,
      mac_address: nic
        .mac
        .map(|m| MacAddr::new(m[0], m[1], m[2], m[3], m[4], m[5]))
        .unwrap_or_else(MacAddr::zero),
      link_speed: nic.link_speed_mbps.min(10000) as u16,

      board_name: "Inferno-AoIP".to_owned(),
      manufacturer: self.manufacturer.unwrap_or_else(|| "Inferno-AoIP".to_owned()),
      model_name: self.model_name.unwrap_or_else(|| self.app_name.clone()),
      factory_device_id: device_id,
      process_id: self.process_id,
      vendor_string: "Audinate Dante-compatible".to_owned(),
      factory_hostname: format!("{short_app_name}-{}", hex::encode(device_id)),
      friendly_hostname,
      model_number: "_000000000000000b".to_owned(),
      rx_channels: vec![],
      tx_channels: vec![],
      bits_per_sample: 24, // TODO make it configurable
      pcm_type: 0xe,
      latency_ns: self.rx_latency_ns,
      sample_rate: self.sample_rate,

      arc_port: ARC_PORT,
      cmc_port: CMC_PORT,
      flows_control_port: FLOWS_CONTROL_PORT,
      info_request_port: INFO_REQUEST_PORT,
    };

    if let Some(altport) = self.alt_port {
      self_info.arc_port = altport;
      self_info.cmc_port = altport + 1;
      self_info.flows_control_port = altport + 2;
      self_info.info_request_port = altport + 3;
    }

    let mut settings = Settings {
      self_info,
      tx_latency_ns: self.tx_latency_ns,
      clock_path: self.clock_path,
      use_safe_clock: self.use_safe_clock,
      tx_source_bit_depth: self.tx_source_bit_depth,
      state_dir,
    };
    settings.make_rx_channels(self.rx_channels as usize);
    settings.make_tx_channels(self.tx_channels as usize);
    Ok(settings)
  }

  /// Configure a builder from upstream-style key/value settings (`NAME`, `BIND_IP`, `PROCESS_ID`,
  /// `DEVICE_ID`, `SAMPLE_RATE`, `RX_LATENCY_NS`, `TX_LATENCY_NS`, `ALT_PORT`, `CLOCK_PATH`,
  /// `USE_SAFE_CLOCK`, `TX_SOURCE_BIT_DEPTH`, `RX_CHANNELS`, `TX_CHANNELS`, and the fork's
  /// `STATE_DIR`). Keys are case-insensitive. Does **not** read the environment; see
  /// [`Settings::try_new`]. The result is in upstream-compatibility mode (see the `legacy` field).
  pub fn from_config_map(
    app_name: &str,
    short_app_name: &str,
    my_ip: Option<Ipv4Addr>,
    config: &BTreeMap<String, String>,
  ) -> Result<Self, SettingsError> {
    let config: BTreeMap<String, String> =
      config.iter().map(|(k, v)| (k.to_ascii_uppercase(), v.clone())).collect();
    let mut b = Self::new(app_name, short_app_name);
    b.legacy = true;

    fn get<'a, T: FromStr>(
      config: &'a BTreeMap<String, String>,
      key: &str,
      expected: &'static str,
    ) -> Result<Option<T>, SettingsError> {
      config.get(key).map(|v| v.trim().parse().map_err(|_| invalid(key, v, expected))).transpose()
    }

    if let Some(ip) = my_ip {
      b.nic = NicSelector::Ipv4(ip);
    } else if let Some(v) = config.get("BIND_IP") {
      b.nic = match v.trim().parse::<Ipv4Addr>() {
        Ok(ip) => NicSelector::Ipv4(ip),
        Err(_) => NicSelector::Name(v.clone()),
      };
    }
    if let Some(v) = get::<u16>(&config, "PROCESS_ID", "must be u16")? {
      b.process_id = v;
    }
    if let Some(v) = config.get("DEVICE_ID") {
      let mut id = [0u8; 8];
      hex::decode_to_slice(v.trim(), &mut id)
        .map_err(|_| invalid("DEVICE_ID", v, "must be 16 hexadecimal digits"))?;
      b.device_id = Some(id);
    }
    b.device_name = config.get("NAME").cloned();
    if let Some(v) = get::<u32>(&config, "SAMPLE_RATE", "must be integer")? {
      b.sample_rate = v;
    }
    if let Some(v) = get::<usize>(&config, "RX_LATENCY_NS", "must be integer")? {
      b.rx_latency_ns = v;
    }
    if let Some(v) = get::<u32>(&config, "TX_LATENCY_NS", "must be integer")? {
      b.tx_latency_ns = v;
    }
    b.alt_port = get::<u16>(&config, "ALT_PORT", "must be u16")?;
    if let Some(v) = get::<u16>(&config, "RX_CHANNELS", "number of channels must be u16")? {
      b.rx_channels = v;
    }
    if let Some(v) = get::<u16>(&config, "TX_CHANNELS", "number of channels must be u16")? {
      b.tx_channels = v;
    }
    b.clock_path = config.get("CLOCK_PATH").map(PathBuf::from);
    if let Some(v) = get::<bool>(&config, "USE_SAFE_CLOCK", "must be boolean")? {
      b.use_safe_clock = v;
    }
    if let Some(v) = get::<u8>(&config, "TX_SOURCE_BIT_DEPTH", "must be one of: 16, 24, 32")? {
      b.tx_source_bit_depth = v;
    }
    b.state_dir = config.get("STATE_DIR").filter(|s| !s.is_empty()).map(PathBuf::from);
    Ok(b)
  }
}

/// Merge `INFERNO_*` variables into `config` (keys upper-cased, explicit config entries win).
fn merge_inferno_vars(
  config: &BTreeMap<String, String>,
  vars: impl Iterator<Item = (String, String)>,
) -> BTreeMap<String, String> {
  let mut merged: BTreeMap<String, String> =
    config.iter().map(|(k, v)| (k.to_ascii_uppercase(), v.clone())).collect();
  for (env_key, env_value) in vars {
    if let Some(key) = env_key.strip_prefix("INFERNO_") {
      merged.entry(key.to_ascii_uppercase()).or_insert(env_value);
    }
  }
  merged
}

// ---------------------------------------------------------------------------------------------
// Settings
// ---------------------------------------------------------------------------------------------

#[derive(Clone)] // TODO: this shouldn't need to be clonable, fix the ALSA plugin
pub struct Settings {
  pub self_info: DeviceInfo,
  pub tx_latency_ns: u32,
  pub clock_path: Option<PathBuf>,
  pub use_safe_clock: bool,
  pub tx_source_bit_depth: u8,
  /// Persistent state directory (see [`SettingsBuilder::state_dir`], [`default_state_dir`]).
  pub state_dir: PathBuf,
}

impl Settings {
  /// Start a [`SettingsBuilder`].
  pub fn builder(app_name: &str, short_app_name: &str) -> SettingsBuilder {
    SettingsBuilder::new(app_name, short_app_name)
  }

  /// Upstream-compatible constructor: key/value `config` merged with `INFERNO_*` environment
  /// variables (explicit `config` entries win). Returns an error instead of panicking.
  pub fn try_new(
    app_name: &str,
    short_app_name: &str,
    my_ip: Option<Ipv4Addr>,
    config: &BTreeMap<String, String>,
  ) -> Result<Self, SettingsError> {
    let merged = merge_inferno_vars(config, env::vars());
    SettingsBuilder::from_config_map(app_name, short_app_name, my_ip, &merged)?.build()
  }

  /// Upstream-compatible constructor that **panics** with the error message when the settings are
  /// invalid. Kept so existing callers (ALSA plugin, `inferno2pipe`) keep working; new code should use
  /// [`Settings::try_new`] or [`Settings::builder`].
  pub fn new(
    app_name: &str,
    short_app_name: &str,
    my_ip: Option<Ipv4Addr>,
    config: &BTreeMap<String, String>,
  ) -> Self {
    match Self::try_new(app_name, short_app_name, my_ip, config) {
      Ok(s) => s,
      Err(e) => {
        warn!("invalid Inferno settings: {e}");
        panic!("invalid Inferno settings: {e}")
      }
    }
  }

  pub fn make_rx_channels(&mut self, count: usize) {
    self.self_info.rx_channels = (1..=count)
      .map(|id| Channel {
        factory_name: format!("{id:02}"),
        friendly_name: Arc::new(RwLock::new(format!("RX {id}"))),
      })
      .collect();
  }
  pub fn make_tx_channels(&mut self, count: usize) {
    self.self_info.tx_channels = (1..=count)
      .map(|id| Channel {
        factory_name: format!("{id:02}"),
        friendly_name: Arc::new(RwLock::new(format!("TX {id}"))),
      })
      .collect();
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use netdev::ipnet::Ipv4Net;
  use netdev::NetworkDevice;

  const MAC: [u8; 6] = [0x00, 0x1d, 0xc1, 0xaa, 0xbb, 0xcc];

  fn test_state_dir() -> PathBuf {
    // Never the real ProgramData / platform state dir. Nothing is created by building Settings.
    std::env::temp_dir().join("inferno_aoip_settings_tests")
  }

  fn iface(name: &str, friendly: &str, ip: [u8; 4], prefix: u8) -> netdev::Interface {
    let mut i = netdev::Interface::dummy();
    i.name = name.to_owned();
    i.friendly_name = Some(friendly.to_owned());
    i.ipv4 = vec![Ipv4Net::new(Ipv4Addr::from(ip), prefix).unwrap()];
    i.mac_addr = Some(MacAddr::new(MAC[0], MAC[1], MAC[2], MAC[3], MAC[4], MAC[5]));
    i.transmit_speed = Some(1_000_000_000);
    i.receive_speed = Some(1_000_000_000);
    i.gateway = Some(NetworkDevice {
      mac_addr: MacAddr::zero(),
      ipv4: vec![Ipv4Addr::new(10, 0, 0, 1), Ipv4Addr::new(192, 168, 1, 1)],
      ipv6: vec![],
    });
    i
  }

  fn adapters() -> Vec<netdev::Interface> {
    vec![
      iface("{AAAAAAAA-0000-0000-0000-000000000001}", "Wi-Fi", [192, 168, 50, 7], 24),
      iface("{3F2B1C0A-1111-2222-3333-444455556666}", "Ethernet 2", [192, 168, 1, 20], 24),
    ]
  }

  fn base() -> SettingsBuilder {
    SettingsBuilder::new("Test App", "TestApp").state_dir(test_state_dir())
  }

  fn build(b: SettingsBuilder) -> Result<Settings, SettingsError> {
    b.build_with(&adapters(), || Some(Ipv4Addr::new(192, 168, 50, 7)))
  }

  #[test]
  fn select_by_guid_ignores_braces_and_case() {
    let s = build(base().nic(NicSelector::Guid("3f2b1c0a-1111-2222-3333-444455556666".into()))).unwrap();
    assert_eq!(s.self_info.ip_address, Ipv4Addr::new(192, 168, 1, 20));
    assert_eq!(s.self_info.netmask, Ipv4Addr::new(255, 255, 255, 0));
    assert_eq!(s.self_info.gateway, Ipv4Addr::new(192, 168, 1, 1));
    assert_eq!(s.self_info.link_speed, 1000);
    assert_eq!(s.self_info.mac_address, MacAddr::new(0x00, 0x1d, 0xc1, 0xaa, 0xbb, 0xcc));
  }

  #[test]
  fn select_by_friendly_name_and_ipv4() {
    let s = build(base().nic(NicSelector::Name("ethernet 2".into()))).unwrap();
    assert_eq!(s.self_info.ip_address, Ipv4Addr::new(192, 168, 1, 20));
    let s = build(base().nic(NicSelector::Ipv4(Ipv4Addr::new(192, 168, 50, 7)))).unwrap();
    assert_eq!(s.self_info.netmask, Ipv4Addr::new(255, 255, 255, 0));
    assert_eq!(s.self_info.gateway, Ipv4Addr::UNSPECIFIED); // 10.0.0.1 / 192.168.1.1 are not on 192.168.50/24
  }

  #[test]
  fn auto_uses_local_ip() {
    let s = build(base()).unwrap();
    assert_eq!(s.self_info.ip_address, Ipv4Addr::new(192, 168, 50, 7));
    let err = base().build_with(&adapters(), || None).err().unwrap();
    assert!(matches!(err, SettingsError::NoNetworkInterface(_)));
  }

  #[test]
  fn unknown_nic_is_an_error_not_a_panic() {
    let err = build(base().nic(NicSelector::Guid("{nope}".into()))).err().unwrap();
    assert!(matches!(err, SettingsError::NicNotFound(ref s) if s == "{nope}"));
    let err = build(base().nic(NicSelector::Name("Ethernet 9".into()))).err().unwrap();
    assert!(matches!(err, SettingsError::NicNotFound(_)));
    let err = build(base().nic(NicSelector::Ipv4(Ipv4Addr::new(1, 2, 3, 4)))).err().unwrap();
    assert!(matches!(err, SettingsError::NicNotFound(_)));
  }

  #[test]
  fn nic_without_ipv4_is_an_error() {
    let mut no_ip = iface("{BBBB}", "Empty", [0, 0, 0, 0], 8);
    no_ip.ipv4.clear();
    let err = base().nic(NicSelector::Name("Empty".into())).build_with(&[no_ip], || None).err().unwrap();
    assert!(matches!(err, SettingsError::NicHasNoIpv4(_)));
  }

  #[test]
  fn nic_info_override_needs_no_os_lookup() {
    let mut info = NicInfo::new(Ipv4Addr::new(10, 1, 2, 3));
    info.mac = Some(MAC);
    info.link_speed_mbps = 100;
    let s = base().nic_info(info).build_with(&[], || None).unwrap();
    assert_eq!(s.self_info.ip_address, Ipv4Addr::new(10, 1, 2, 3));
    assert_eq!(s.self_info.link_speed, 100);
    assert_eq!(s.self_info.mac_address, MacAddr::new(0x00, 0x1d, 0xc1, 0xaa, 0xbb, 0xcc));
  }

  #[test]
  fn device_id_layouts() {
    // Default (upstream): 0000 <ip> <process id>.
    let s = build(base().nic(NicSelector::Name("Ethernet 2".into())).process_id(0x0102)).unwrap();
    assert_eq!(s.self_info.factory_device_id, [0, 0, 192, 168, 1, 20, 0x01, 0x02]);
    // From MAC: mac ++ process id (0 => mac ++ 00 00).
    let s = build(base().device_id_from_mac(MAC)).unwrap();
    assert_eq!(s.self_info.factory_device_id, [0x00, 0x1d, 0xc1, 0xaa, 0xbb, 0xcc, 0, 0]);
    let s = build(base().device_id_from_mac(MAC).process_id(5)).unwrap();
    assert_eq!(s.self_info.factory_device_id, [0x00, 0x1d, 0xc1, 0xaa, 0xbb, 0xcc, 0, 5]);
    // Explicit wins.
    let s = build(base().device_id_from_mac(MAC).device_id([1, 2, 3, 4, 5, 6, 7, 8])).unwrap();
    assert_eq!(s.self_info.factory_device_id, [1, 2, 3, 4, 5, 6, 7, 8]);
    assert_eq!(s.self_info.factory_hostname, "TestApp-0102030405060708");
  }

  #[test]
  fn device_name_rules() {
    for ok in ["ONS-PC", "a", "A1-b2", &"a".repeat(31)] {
      assert!(validate_device_name(ok).is_ok(), "{ok}");
      assert_eq!(build(base().device_name(ok)).unwrap().self_info.friendly_hostname, ok);
    }
    for bad in ["", "-a", "a-", "has space", "under_score", "Büro", &"a".repeat(32)] {
      assert!(matches!(validate_device_name(bad), Err(SettingsError::InvalidDeviceName { .. })), "{bad}");
      assert!(matches!(
        build(base().device_name(bad)).err().unwrap(),
        SettingsError::InvalidDeviceName { .. }
      ));
    }
  }

  #[test]
  fn sanitize_matches_ons_identity_rules() {
    assert_eq!(sanitize_device_name("my  studio_pc.local"), "my-studio-pc-local");
    assert_eq!(sanitize_device_name("---abc---"), "abc");
    assert_eq!(sanitize_device_name("Studio 🎙 One"), "Studio-One");
    assert_eq!(sanitize_device_name("日本語"), "");
    let long = format!("{}-{}", "a".repeat(30), "bbbb");
    assert_eq!(sanitize_device_name(&long), "a".repeat(30));
    assert!(validate_device_name(&sanitize_device_name(&"x".repeat(100))).is_ok());
  }

  #[test]
  fn default_name_is_valid_even_for_odd_app_names() {
    for app in ["Test App", "", "日本語", &"long".repeat(20), "x-"] {
      let s = SettingsBuilder::new(app, app)
        .state_dir(test_state_dir())
        .build_with(&adapters(), || Some(Ipv4Addr::new(192, 168, 50, 7)))
        .unwrap();
      assert!(validate_device_name(&s.self_info.friendly_hostname).is_ok(), "{app:?}");
    }
    assert_eq!(build(base()).unwrap().self_info.friendly_hostname, "Test-App-c0a83207");
  }

  #[test]
  fn manufacturer_and_model_can_be_overridden() {
    let s = build(base().manufacturer("OpenNetworkSoundcard").model_name("ONS-VSC")).unwrap();
    assert_eq!(s.self_info.manufacturer, "OpenNetworkSoundcard");
    assert_eq!(s.self_info.model_name, "ONS-VSC");
    let s = build(base()).unwrap();
    assert_eq!(s.self_info.manufacturer, "Inferno-AoIP");
    assert_eq!(s.self_info.model_name, "Test App");
  }

  #[test]
  fn ports_latency_and_channels() {
    let s = build(base()).unwrap();
    assert_eq!(s.self_info.arc_port, ARC_PORT);
    assert_eq!(s.self_info.latency_ns, 10_000_000);
    assert_eq!(s.self_info.sample_rate, 48000);
    assert_eq!(s.self_info.rx_channels.len(), 2);

    let s = build(
      base()
        .alt_port(5000)
        .rx_latency_ns(4_000_000)
        .tx_latency_ns(6_000_000)
        .rx_channels(16)
        .tx_channels(8),
    )
    .unwrap();
    assert_eq!(
      (
        s.self_info.arc_port,
        s.self_info.cmc_port,
        s.self_info.flows_control_port,
        s.self_info.info_request_port
      ),
      (5000, 5001, 5002, 5003)
    );
    assert_eq!(s.self_info.latency_ns, 4_000_000);
    assert_eq!(s.tx_latency_ns, 6_000_000);
    assert_eq!(s.self_info.rx_channels.len(), 16);
    assert_eq!(s.self_info.tx_channels.len(), 8);
    assert_eq!(s.self_info.tx_channels[7].factory_name, "08");
  }

  #[test]
  fn out_of_range_values_are_errors() {
    for b in [
      base().sample_rate(0),
      base().rx_latency_ns(0),
      base().tx_latency_ns(0),
      base().alt_port(80),
      base().alt_port(65535),
      base().tx_source_bit_depth(20),
    ] {
      assert!(matches!(build(b).err().unwrap(), SettingsError::InvalidSetting { .. }));
    }
    assert!(build(base().alt_port(65532)).is_ok());
  }

  #[test]
  fn state_dir_is_injected_and_default_is_program_data() {
    let dir = test_state_dir().join("custom");
    let s = build(base().state_dir(&dir)).unwrap();
    assert_eq!(s.state_dir, dir);

    let pd = program_data_state_dir(Some(OsString::from("C:\\ProgramData"))).unwrap();
    assert_eq!(pd, PathBuf::from("C:\\ProgramData").join("OpenNetworkSoundcard"));
    assert!(matches!(program_data_state_dir(None), Err(SettingsError::NoStateDir(_))));
    assert!(matches!(program_data_state_dir(Some(OsString::new())), Err(SettingsError::NoStateDir(_))));
  }

  // ---- upstream key/value + env path ----

  fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
  }

  fn from_map(pairs: &[(&str, &str)], my_ip: Option<Ipv4Addr>) -> Result<Settings, SettingsError> {
    let mut b = SettingsBuilder::from_config_map("App", "App", my_ip, &map(pairs))?;
    if b.state_dir.is_none() {
      b = b.state_dir(test_state_dir());
    }
    b.build_with(&adapters(), || Some(Ipv4Addr::new(192, 168, 50, 7)))
  }

  #[test]
  fn config_map_basic() {
    let s = from_map(
      &[
        ("name", "my name with spaces"), // legacy path is lenient, as upstream
        ("PROCESS_ID", "3"),
        ("ALT_PORT", "6000"),
        ("RX_CHANNELS", "4"),
        ("TX_CHANNELS", "6"),
        ("SAMPLE_RATE", "44100"),
        ("CLOCK_PATH", "/tmp/clk"),
        ("BIND_IP", "Ethernet 2"),
        ("USE_SAFE_CLOCK", "true"),
        ("TX_SOURCE_BIT_DEPTH", "24"),
      ],
      None,
    )
    .unwrap();
    assert_eq!(s.self_info.friendly_hostname, "my name with spaces");
    assert_eq!(s.self_info.process_id, 3);
    assert_eq!(s.self_info.arc_port, 6000);
    assert_eq!(s.self_info.rx_channels.len(), 4);
    assert_eq!(s.self_info.tx_channels.len(), 6);
    assert_eq!(s.self_info.sample_rate, 44100);
    assert_eq!(s.clock_path, Some(PathBuf::from("/tmp/clk")));
    assert_eq!(s.self_info.ip_address, Ipv4Addr::new(192, 168, 1, 20));
    assert!(s.use_safe_clock);
    assert_eq!(s.tx_source_bit_depth, 24);
    assert_eq!(s.state_dir, test_state_dir());
  }

  #[test]
  fn config_map_keeps_upstream_defaults() {
    let s = from_map(&[], Some(Ipv4Addr::new(192, 168, 50, 7))).unwrap();
    assert_eq!(s.self_info.friendly_hostname, "App c0a83207");
    assert_eq!(s.self_info.factory_device_id, [0, 0, 192, 168, 50, 7, 0, 0]);
    // my_ip that matches no adapter is accepted (loopback tests rely on this).
    let s = from_map(&[("BIND_IP", "127.0.0.1")], Some(Ipv4Addr::new(127, 0, 0, 1))).unwrap();
    assert_eq!(s.self_info.ip_address, Ipv4Addr::LOCALHOST);
  }

  #[test]
  fn config_map_device_id_and_state_dir() {
    let s = from_map(&[("DEVICE_ID", "001dc1aabbcc0000"), ("STATE_DIR", "X:/state")], None).unwrap();
    assert_eq!(s.self_info.factory_device_id, [0x00, 0x1d, 0xc1, 0xaa, 0xbb, 0xcc, 0, 0]);
    assert_eq!(s.state_dir, PathBuf::from("X:/state"));
  }

  #[test]
  fn config_map_bad_values_are_errors() {
    for (k, v) in [
      ("PROCESS_ID", "70000"),
      ("DEVICE_ID", "xyz"),
      ("DEVICE_ID", "00"),
      ("SAMPLE_RATE", "fast"),
      ("RX_LATENCY_NS", "-1"),
      ("TX_LATENCY_NS", "x"),
      ("ALT_PORT", "99999"),
      ("RX_CHANNELS", "100000"),
      ("USE_SAFE_CLOCK", "maybe"),
      ("TX_SOURCE_BIT_DEPTH", "12"),
    ] {
      let err = from_map(&[(k, v)], None).err().unwrap_or_else(|| panic!("{k}={v} should fail"));
      assert!(matches!(err, SettingsError::InvalidSetting { .. }), "{k}={v}: {err}");
    }
    assert!(matches!(
      from_map(&[("BIND_IP", "No Such Adapter")], None).err().unwrap(),
      SettingsError::NicNotFound(_)
    ));
  }

  #[test]
  fn legacy_name_is_cut_on_a_char_boundary() {
    let name = "é".repeat(40);
    let s = from_map(&[("NAME", name.as_str())], None).unwrap();
    assert_eq!(s.self_info.friendly_hostname.chars().count(), 31);
  }

  #[test]
  fn env_vars_fill_in_but_do_not_override_config() {
    let vars = vec![
      ("INFERNO_NAME".to_string(), "FromEnv".to_string()),
      ("INFERNO_ALT_PORT".to_string(), "7000".to_string()),
      ("inferno_state_dir".to_string(), "ignored (prefix is case sensitive)".to_string()),
      ("OTHER".to_string(), "x".to_string()),
    ];
    let merged = merge_inferno_vars(&map(&[("alt_port", "8000")]), vars.into_iter());
    assert_eq!(merged["NAME"], "FromEnv");
    assert_eq!(merged["ALT_PORT"], "8000");
    assert!(!merged.contains_key("OTHER"));
    assert!(!merged.contains_key("STATE_DIR"));
  }

  #[test]
  fn try_new_reports_errors_without_panicking() {
    let err =
      Settings::try_new("App", "App", Some(Ipv4Addr::LOCALHOST), &map(&[("ALT_PORT", "nope")])).err();
    assert!(matches!(err, Some(SettingsError::InvalidSetting { .. })));
  }
}
