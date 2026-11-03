// SPDX-License-Identifier: GPL-3.0-or-later OR AGPL-3.0-or-later
//
// ONS fork addition (same dual licence as the rest of `inferno_aoip`). Written for the Open Network
// Soundcard fork; the wire formats were worked out from captures of Dante Controller and from reading the
// open-source `netaudio` package (MIT). No Audinate code or binaries were used.

//! ARC messages about the device itself: its name, its settings and the directory of settings
//! properties.
//!
//! All functions here are pure: they turn request bytes into values and values into response bytes, so
//! the formats are unit tested without a socket. `content` always means the bytes after the 10-byte ARC
//! header, as in the rest of the ARC server. Offsets that point into a response count from the start of
//! the packet, so they include [`HEADER_LENGTH`].
//!
//! | Opcode | Meaning | Source of the format |
//! |---|---|---|
//! | `0x1001` | Set the device name; an empty name resets it to the default | captured from `netaudio` (`28 09 00 1a 00 0a 10 01 00 00 <name>\0`) |
//! | `0x1100` | Read settings properties: sample rate and latencies | captured from Dante Controller and `netaudio`; reply layout from a hardware reply quoted in Inferno's source comments, checked against `netaudio`'s parser |
//! | `0x1101` | Change settings properties (latency, ...) | request seen from `netaudio`; not interpreted, rejected |
//! | `0x1102` | Property directory: which properties exist and whether they can be written | hardware reply quoted in Inferno's source comments |
//!
//! Anything marked **(verify)** was inferred and not confirmed against Dante Controller on hardware.

use crate::device_info::DeviceInfo;

use super::req_resp::HEADER_LENGTH;

/// Result code for "done" in the opcode2 field of a reply.
pub use super::req_resp::CODE_OK;

/// Result code for a request that is understood but refused (invalid value, not supported). Any code
/// other than [`CODE_OK`] counts as a rejection to `netaudio`; the exact value a hardware device sends
/// is not known **(verify)**, so this follows the `0xFFFF` Inferno already uses for failed requests.
pub const CODE_REJECTED: u16 = 0xFFFF;

pub mod set_device_name {
  //! Opcode `0x1001`.

  pub const OPCODE: u16 = 0x1001;

  /// What the request asks for.
  #[derive(Debug, PartialEq, Eq)]
  pub enum NameRequest {
    /// Use this name.
    Set(String),
    /// An empty name: go back to the default device name (what `netaudio device name --reset` sends).
    Reset,
  }

  /// Why a request could not be read.
  #[derive(Debug, PartialEq, Eq)]
  pub enum NameRequestError {
    /// The name is not valid UTF-8.
    NotUtf8,
  }

  /// Read the name from the request content: a zero-terminated string, or nothing at all for a reset.
  /// Bytes after the terminator (padding) are ignored.
  pub fn parse_request(content: &[u8]) -> Result<NameRequest, NameRequestError> {
    let end = content.iter().position(|b| *b == 0).unwrap_or(content.len());
    if end == 0 {
      return Ok(NameRequest::Reset);
    }
    let name = std::str::from_utf8(&content[..end]).map_err(|_| NameRequestError::NotUtf8)?;
    Ok(NameRequest::Set(name.to_owned()))
  }
}

pub mod device_settings {
  //! Opcodes `0x1100`, `0x1101` and `0x1102`.

  use super::*;

  /// Read properties.
  pub const QUERY_OPCODE: u16 = 0x1100;
  /// Change properties.
  pub const SET_OPCODE: u16 = 0x1101;
  /// List the properties the device has.
  pub const DIRECTORY_OPCODE: u16 = 0x1102;

  /// Property ids. The ones with the top bit set carry a pointer to a 32-bit value; the others carry
  /// a 16-bit value in place. The meaning of the first group was found by feeding replies to
  /// `netaudio`'s parser and watching which field each id fills.
  pub mod property {
    /// Sample rate in Hz.
    pub const SAMPLE_RATE: u16 = 0x8020;
    /// Default receive latency in nanoseconds.
    pub const DEFAULT_LATENCY: u16 = 0x8204;
    /// Configured receive latency in nanoseconds.
    pub const CONFIGURED_LATENCY: u16 = 0x8205;
    /// Latency in use right now, in nanoseconds.
    pub const ACTIVE_LATENCY: u16 = 0x8301;
    /// Largest latency the device offers, in nanoseconds.
    pub const MAX_LATENCY: u16 = 0x8302;
    /// Smallest latency the device offers, in nanoseconds.
    pub const MIN_LATENCY: u16 = 0x8306;
  }

  /// Properties answered when the request names none (`netaudio`'s plain "get settings" sends an
  /// empty request).
  pub const DEFAULT_PROPERTIES: [u16; 6] = [
    property::SAMPLE_RATE,
    property::DEFAULT_LATENCY,
    property::CONFIGURED_LATENCY,
    property::ACTIVE_LATENCY,
    property::MAX_LATENCY,
    property::MIN_LATENCY,
  ];

  /// Access flags of a property in the directory: bit 0 readable, bit 1 writable.
  pub const FLAG_READ: u16 = 1;
  /// See [`FLAG_READ`].
  pub const FLAG_WRITE: u16 = 2;

  /// The values the device reports. ONS fixes the sample rate and, while the device runs, the latency.
  #[derive(Debug, Clone, PartialEq, Eq)]
  pub struct Values {
    pub sample_rate: u32,
    pub default_latency_ns: u32,
    pub configured_latency_ns: u32,
    pub active_latency_ns: u32,
    pub max_latency_ns: u32,
    pub min_latency_ns: u32,
  }

  fn clamp_ns(ns: usize) -> u32 {
    u32::try_from(ns).unwrap_or(u32::MAX)
  }

  impl Values {
    /// Read the values from the device description.
    pub fn from_device(info: &DeviceInfo) -> Self {
      let caps = &info.capabilities;
      Self {
        sample_rate: info.sample_rate,
        default_latency_ns: clamp_ns(caps.latency_default_ns),
        configured_latency_ns: clamp_ns(info.latency_ns),
        active_latency_ns: clamp_ns(info.latency_ns),
        max_latency_ns: clamp_ns(caps.latency_max_ns),
        min_latency_ns: clamp_ns(caps.latency_min_ns),
      }
    }

    /// The 32-bit value of a property, `None` for a property this device does not have.
    pub fn get(&self, id: u16) -> Option<u32> {
      match id {
        property::SAMPLE_RATE => Some(self.sample_rate),
        property::DEFAULT_LATENCY => Some(self.default_latency_ns),
        property::CONFIGURED_LATENCY => Some(self.configured_latency_ns),
        property::ACTIVE_LATENCY => Some(self.active_latency_ns),
        property::MAX_LATENCY => Some(self.max_latency_ns),
        property::MIN_LATENCY => Some(self.min_latency_ns),
        _ => None,
      }
    }
  }

  /// The property ids a read request asks for: `u16` count, then that many `u16` ids (what Dante
  /// Controller and `netaudio`'s latency query send). An empty or unreadable request gives
  /// [`DEFAULT_PROPERTIES`].
  pub fn parse_query(content: &[u8]) -> Vec<u16> {
    if content.len() >= 2 {
      let count = usize::from(u16::from_be_bytes([content[0], content[1]]));
      let ids: Vec<u16> =
        content[2..].chunks_exact(2).take(count).map(|c| u16::from_be_bytes([c[0], c[1]])).collect();
      if !ids.is_empty() {
        return ids;
      }
    }
    DEFAULT_PROPERTIES.to_vec()
  }

  /// The content of the reply to a read request for `ids`.
  ///
  /// Layout (as in the hardware reply this was checked against): two bytes holding the number of
  /// entries (twice, as the hardware does **(verify)**), then one 4-byte entry per requested id, then
  /// the 32-bit values that the entries of the `0x8xxx` properties point to.
  ///
  /// * a property the device has: `id`, then the offset of its 32-bit value counted from the start of
  ///   the packet;
  /// * a property it does not have: `0x0000`, then the id (`netaudio` reports it as unavailable).
  pub fn build_query_response(ids: &[u16], values: &Values) -> Vec<u8> {
    let count = ids.len().min(0xff);
    let ids = &ids[..count];
    let entries_len = 4 * count;
    let mut data_offset = HEADER_LENGTH + 2 + entries_len;
    let mut out = Vec::with_capacity(2 + entries_len + 4 * count);
    out.push(count as u8);
    out.push(count as u8);
    let mut data = Vec::new();
    for id in ids {
      match values.get(*id) {
        Some(v) => {
          out.extend_from_slice(&id.to_be_bytes());
          out.extend_from_slice(&(data_offset as u16).to_be_bytes());
          data.extend_from_slice(&v.to_be_bytes());
          data_offset += 4;
        }
        None => {
          out.extend_from_slice(&0u16.to_be_bytes());
          out.extend_from_slice(&id.to_be_bytes());
        }
      }
    }
    out.extend_from_slice(&data);
    out
  }

  /// The properties of the directory and whether a controller may write them. ONS exposes the
  /// settings it can report, all read-only: the latency is changed in the ONS GUI while the device is
  /// stopped, and the sample rate is fixed.
  pub const DIRECTORY: [(u16, u16); 6] = [
    (property::SAMPLE_RATE, FLAG_READ),
    (property::DEFAULT_LATENCY, FLAG_READ),
    (property::CONFIGURED_LATENCY, FLAG_READ),
    (property::ACTIVE_LATENCY, FLAG_READ),
    (property::MAX_LATENCY, FLAG_READ),
    (property::MIN_LATENCY, FLAG_READ),
  ];

  /// The content of the reply to a property directory request (`0x1102`): `u16` entry count, then
  /// `(id, flags)` pairs.
  pub fn build_directory_response() -> Vec<u8> {
    let mut out = Vec::with_capacity(2 + 4 * DIRECTORY.len());
    out.extend_from_slice(&(DIRECTORY.len() as u16).to_be_bytes());
    for (id, flags) in DIRECTORY {
      out.extend_from_slice(&id.to_be_bytes());
      out.extend_from_slice(&flags.to_be_bytes());
    }
    out
  }

  /// A short description of a change request for the log: its length and leading bytes in hex. The
  /// body of `0x1101` is not decoded because every change is refused.
  pub fn describe_set_request(content: &[u8]) -> String {
    let shown = &content[..content.len().min(32)];
    format!("{} bytes: {}", content.len(), hex::encode(shown))
  }
}

#[cfg(test)]
mod tests {
  use super::device_settings::*;
  use super::set_device_name::*;
  use super::*;

  // ---- 0x1001 ----

  /// The request `netaudio device name ONS-RENAME-TEST` sent in the lab on 2026-11-01.
  #[test]
  fn set_name_request_from_the_lab_capture() {
    let content = hex::decode("4f4e532d52454e414d452d5445535400").unwrap();
    assert_eq!(parse_request(&content), Ok(NameRequest::Set("ONS-RENAME-TEST".into())));
  }

  #[test]
  fn empty_name_is_a_reset() {
    assert_eq!(parse_request(&[]), Ok(NameRequest::Reset));
    assert_eq!(parse_request(&[0]), Ok(NameRequest::Reset));
    assert_eq!(parse_request(&[0, 0, 0, 0]), Ok(NameRequest::Reset));
  }

  #[test]
  fn name_without_terminator_and_padding_after_it() {
    assert_eq!(parse_request(b"abc"), Ok(NameRequest::Set("abc".into())));
    assert_eq!(parse_request(b"abc\0\0\0\0"), Ok(NameRequest::Set("abc".into())));
    assert_eq!(parse_request(b"abc\0junk"), Ok(NameRequest::Set("abc".into())));
  }

  #[test]
  fn name_that_is_not_utf8_is_an_error() {
    assert_eq!(parse_request(&[0xff, 0xfe, 0]), Err(NameRequestError::NotUtf8));
  }

  // ---- 0x1100 / 0x1102 ----

  fn values() -> Values {
    Values {
      sample_rate: 48_000,
      default_latency_ns: 10_000_000,
      configured_latency_ns: 6_000_000,
      active_latency_ns: 6_000_000,
      max_latency_ns: 10_000_000,
      min_latency_ns: 4_000_000,
    }
  }

  /// The 18 properties Dante Controller asks for, from the capture of 2026-11-01.
  fn dante_controller_query() -> Vec<u8> {
    hex::decode("00120201820482050210021182188219830183028306031003110303802100f0806000220063").unwrap()
  }

  #[test]
  fn query_ids_are_read_after_the_count() {
    let ids = parse_query(&dante_controller_query());
    assert_eq!(ids.len(), 18);
    assert_eq!(ids[0], 0x0201);
    assert_eq!(ids[1], property::DEFAULT_LATENCY);
    assert_eq!(ids[17], 0x0063);
  }

  #[test]
  fn query_count_larger_than_the_data_is_tolerated() {
    assert_eq!(parse_query(&[0, 9, 0x80, 0x20]), vec![0x8020]);
  }

  #[test]
  fn an_empty_query_gets_the_default_properties() {
    assert_eq!(parse_query(&[]), DEFAULT_PROPERTIES.to_vec());
    assert_eq!(parse_query(&[0, 0]), DEFAULT_PROPERTIES.to_vec());
    assert_eq!(parse_query(&[7]), DEFAULT_PROPERTIES.to_vec());
  }

  /// Read the reply back the way a controller does: entries, then pointers counted from the packet start.
  fn read_reply(content: &[u8]) -> (Vec<(u16, u32)>, Vec<u16>) {
    let count = usize::from(content[1]);
    let mut present = vec![];
    let mut missing = vec![];
    for i in 0..count {
      let e = &content[2 + 4 * i..][..4];
      let (a, b) = (u16::from_be_bytes([e[0], e[1]]), u16::from_be_bytes([e[2], e[3]]));
      if a == 0 {
        missing.push(b);
      } else {
        let p = usize::from(b) - HEADER_LENGTH;
        present.push((a, u32::from_be_bytes(content[p..p + 4].try_into().unwrap())));
      }
    }
    (present, missing)
  }

  #[test]
  fn reply_to_dante_controller_has_the_latencies_and_marks_the_rest_unavailable() {
    let ids = parse_query(&dante_controller_query());
    let content = build_query_response(&ids, &values());
    assert_eq!(&content[..2], &[18, 18]);
    let (present, missing) = read_reply(&content);
    assert_eq!(
      present,
      vec![
        (property::DEFAULT_LATENCY, 10_000_000),
        (property::CONFIGURED_LATENCY, 6_000_000),
        (property::ACTIVE_LATENCY, 6_000_000),
        (property::MAX_LATENCY, 10_000_000),
        (property::MIN_LATENCY, 4_000_000),
      ]
    );
    // Everything else is reported as not present, in request order.
    assert_eq!(
      missing,
      vec![0x0201, 0x0210, 0x0211, 0x8218, 0x8219, 0x0310, 0x0311, 0x0303, 0x8021, 0x00f0, 0x8060, 0x0022, 0x0063]
    );
  }

  #[test]
  fn a_default_query_returns_the_sample_rate() {
    let content = build_query_response(&DEFAULT_PROPERTIES, &values());
    let (present, missing) = read_reply(&content);
    assert!(missing.is_empty());
    assert_eq!(present[0], (property::SAMPLE_RATE, 48_000));
    assert_eq!(present.len(), 6);
  }

  #[test]
  fn values_come_from_the_device_description() {
    use crate::device_info::DeviceCapabilities;
    let mut info = crate::protocol::proto_arc::test_device_info();
    info.latency_ns = 4_000_000;
    info.capabilities = DeviceCapabilities {
      latency_min_ns: 4_000_000,
      latency_max_ns: 10_000_000,
      latency_default_ns: 10_000_000,
      board_flags: [0; 4],
      lab: Default::default(),
    };
    let v = Values::from_device(&info);
    assert_eq!(v.sample_rate, 48_000);
    assert_eq!(v.configured_latency_ns, 4_000_000);
    assert_eq!(v.active_latency_ns, 4_000_000);
    assert_eq!(v.default_latency_ns, 10_000_000);
    assert_eq!((v.min_latency_ns, v.max_latency_ns), (4_000_000, 10_000_000));
  }

  #[test]
  fn huge_latencies_saturate_instead_of_wrapping() {
    use crate::device_info::DeviceCapabilities;
    let mut info = crate::protocol::proto_arc::test_device_info();
    info.latency_ns = usize::MAX;
    info.capabilities = DeviceCapabilities::fixed_latency(usize::MAX);
    assert_eq!(Values::from_device(&info).configured_latency_ns, u32::MAX);
  }

  #[test]
  fn too_many_ids_are_cut_to_what_one_byte_can_count() {
    let ids = vec![property::SAMPLE_RATE; 300];
    let content = build_query_response(&ids, &values());
    assert_eq!(content[1], 255);
    assert_eq!(content.len(), 2 + 255 * 4 + 255 * 4);
  }

  #[test]
  fn directory_lists_the_settings_read_only() {
    let content = build_directory_response();
    assert_eq!(&content[..2], &[0, 6]);
    assert_eq!(content.len(), 2 + 6 * 4);
    for chunk in content[2..].chunks_exact(4) {
      let flags = u16::from_be_bytes([chunk[2], chunk[3]]);
      assert_eq!(flags & FLAG_WRITE, 0, "no property may be advertised as writable");
      assert_eq!(flags & FLAG_READ, FLAG_READ);
    }
    assert_eq!(&content[2..6], &[0x80, 0x20, 0, 1]);
  }

  #[test]
  fn every_directory_property_can_be_answered() {
    let v = values();
    for (id, _) in DIRECTORY {
      assert!(v.get(id).is_some(), "{id:#06x}");
    }
  }

  #[test]
  fn set_request_description_is_short() {
    let long = vec![0xabu8; 200];
    let d = describe_set_request(&long);
    assert!(d.starts_with("200 bytes: abab"), "{d}");
    assert!(d.len() < 100);
  }

  #[test]
  fn rejected_is_never_the_ok_code() {
    assert_ne!(CODE_REJECTED, CODE_OK);
  }
}
