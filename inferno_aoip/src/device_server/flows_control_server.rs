use crate::common::*;
use crate::device_server::flows_tx::FlowInfo;
use itertools::Itertools;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tokio::sync::Mutex;

use super::flows_tx::{FlowsTransmitter, FPP_MAX, MAX_FLOWS};
use crate::protocol::flows_control::{FlowControlError, FlowHandle};
use crate::{
  byte_utils::{make_u16, read_0term_str_from_buffer},
  device_info::DeviceInfo,
  net_utils::UdpSocketWrapper,
  protocol::req_resp::{self, HEADER_LENGTH},
};
use tokio::sync::broadcast::Receiver as BroadcastReceiver;

pub async fn run_server(
  self_info: Arc<DeviceInfo>,
  flows_tx: Arc<Mutex<Option<FlowsTransmitter>>>,
  shutdown: BroadcastReceiver<()>,
) {
  let server =
    UdpSocketWrapper::new(Some(self_info.ip_address), self_info.flows_control_port, shutdown).await;
  let mut conn = req_resp::Connection::new(server);
  let mut recv_buff = crate::net_utils::ReceiveBuffer::new();
  while conn.should_work() {
    let request = match conn.recv(&mut recv_buff).await {
      Some(v) => v,
      None => continue,
    };

    if request.opcode2().read() == 0 {
      match request.opcode1().read() {
        0x0100 => {
          // request flow
          let packet = request.into_storage();
          let req = match parse_flow_request(packet) {
            Ok(req) => req,
            Err(e) => {
              error!("invalid flow request ({e}): {}", hex::encode(packet));
              continue;
            }
          };
          let FlowRequest {
            hostname,
            rx_flow_name,
            sample_rate,
            bits_per_sample,
            fpp,
            channel_indices,
            rx_ip,
            rx_port,
          } = req;

          info!("{hostname} requesting flow {rx_flow_name} of channel indices {channel_indices:?} at {sample_rate}Hz {bits_per_sample}bit {fpp} fpp to {rx_ip}:{rx_port}");
          if channel_indices.iter().flatten().find(|&&chi| chi >= self_info.tx_channels.len()).is_some() {
            error!("too large channel number, returning error");
            conn.respond_with_code(0x0302u16 /* ??? TODO */, &[]).await;
            // ONS fork: upstream fell through here, answered a second time and created the flow, and the
            // TX thread then indexed a channel that does not exist.
            continue;
          }
          if sample_rate != self_info.sample_rate {
            error!("sample rate mismatch, returning error");
            conn.respond_with_code(FlowControlError::SampleRateMismatch as u16, &[]).await;
            continue;
          }
          if fpp > FPP_MAX {
            error!("too large fpp, returning error");
            conn.respond_with_code(0x0302u16 /* TODO */, &[]).await;
            continue;
          }
          let flow_info = FlowInfo {
            rx_hostname: hostname.into(),
            rx_flow_name: rx_flow_name.into(),
            dst_addr: rx_ip,
            dst_port: rx_port,
            local_channel_indices: channel_indices,
          };
          let result = match flows_tx.lock().await.as_mut() {
            Some(flows_tx) => {
              flows_tx.add_flow(flow_info, fpp as usize, (bits_per_sample / 8) as usize, None, false).await
            }
            None => {
              error!("flow request while the transmitter is not running");
              conn.respond_with_code(FlowControlError::TooManyTXFlows as u16, &[]).await;
              continue;
            }
          };
          match result {
            Ok((flow_index, handle)) => {
              conn.respond(&handle).await;
            }
            Err(e) => {
              error!("adding flow failed: {e:?}");
              conn.respond_with_code(FlowControlError::TooManyTXFlows as u16, &[]).await;
            }
          }
        }
        0x0101 => {
          // stop flow
          let handle = if let Ok(handle) = request.content().try_into() {
            handle
          } else {
            error!("packet too short: {}", hex::encode(request.content()));
            continue;
          };
          let removed = match flows_tx.lock().await.as_mut() {
            Some(flows_tx) => flows_tx.remove_flow(handle).await.is_ok(),
            None => false,
          };
          if removed {
            info!("stopped flow {handle:?}");
            conn.respond(&[]).await;
          } else {
            warn!("received stop flow request for unknown handle {handle:?}");
            conn.respond_with_code(FlowControlError::FlowNotFound as u16, &[]).await;
          }
        }
        0x0102 => {
          // update flow
          let (handle, channel_indices) = match parse_update_flow(request.content()) {
            Ok(v) => v,
            Err(e) => {
              error!("invalid update flow request ({e}): {}", hex::encode(request.content()));
              continue;
            }
          };
          if channel_indices.iter().flatten().any(|&chi| chi >= self_info.tx_channels.len()) {
            error!("too large channel number in update flow request, returning error");
            conn.respond_with_code(0x0302u16 /* ??? TODO */, &[]).await;
            continue;
          }

          let updated = match flows_tx.lock().await.as_mut() {
            Some(flows_tx) => flows_tx.set_channels(handle, channel_indices.clone()).await.is_ok(),
            None => false,
          };
          if updated {
            info!("set channels {channel_indices:?} in flow {handle:?}");
            conn.respond(&[]).await;
          } else {
            warn!("received update flow request for unknown handle {handle:?}");
            conn.respond_with_code(FlowControlError::FlowNotFound as u16, &[]).await;
          }
        }

        x => {
          error!("received unknown opcode1 {x:#04x}, content {}", hex::encode(request.content()));
          error!("whole packet: {:?}", hex::encode(request.into_storage()));
        }
      }
    } else {
      error!(
        "received unknown opcode2 {:#04x}, content {}",
        request.opcode2().read(),
        hex::encode(request.content())
      );
      error!("whole packet: {:?}", hex::encode(request.into_storage()));
    }
  }
  if let Some(flows_tx) = flows_tx.lock().await.as_mut() {
    flows_tx.shutdown().await;
  }
}

/// A flow request (opcode `0x0100`) from a receiving device, checked and decoded.
#[derive(Debug, PartialEq, Eq)]
struct FlowRequest {
  hostname: String,
  rx_flow_name: String,
  sample_rate: u32,
  bits_per_sample: u32,
  fpp: u16,
  /// One entry per channel of the flow: the transmit channel index, or `None` for an unused slot.
  channel_indices: Vec<Option<usize>>,
  rx_ip: Ipv4Addr,
  rx_port: u16,
}

fn be16(bytes: &[u8], at: usize) -> Result<u16, &'static str> {
  let end = at.checked_add(2).ok_or("truncated")?;
  match bytes.get(at..end) {
    Some(&[a, b]) => Ok(make_u16(a, b)),
    _ => Err("truncated"),
  }
}

fn be32(bytes: &[u8], at: usize) -> Result<u32, &'static str> {
  let end = at.checked_add(4).ok_or("truncated")?;
  match bytes.get(at..end) {
    Some(&[a, b, c, d]) => Ok(u32::from_be_bytes([a, b, c, d])),
    _ => Err("truncated"),
  }
}

/// The channel list of a flow: `count` big-endian `u16` ids at `at`, 0 meaning unused and `n` channel `n - 1`.
fn channel_list(bytes: &[u8], at: usize, count: usize) -> Result<Vec<Option<usize>>, &'static str> {
  // The list must fit in the packet; this also bounds the allocation by the packet size.
  let end = count.checked_mul(2).and_then(|n| n.checked_add(at)).ok_or("truncated")?;
  let list = bytes.get(at..end).ok_or("truncated channel list")?;
  Ok(
    list
      .chunks_exact(2)
      .map(|id| match make_u16(id[0], id[1]) {
        0 => None,
        x => Some(usize::from(x) - 1),
      })
      .collect(),
  )
}

/// Decode a whole flow request packet (header included). Offsets inside the request count from the start
/// of the packet. Never panics, whatever the bytes.
fn parse_flow_request(packet: &[u8]) -> Result<FlowRequest, &'static str> {
  let c = packet.get(HEADER_LENGTH..).ok_or("too short packet")?;
  if c.len() <= 16 {
    return Err("too short packet");
  }
  let hostname_offset = usize::from(be16(c, 0)?);
  let sample_rate = be32(c, 2)?;
  let bits_per_sample = be32(c, 6)?;
  let one = be16(c, 10)?;
  if one != 1 {
    warn!("expecting 1, received {one:#x}");
  }
  let num_channels = usize::from(be16(c, 12)?);
  let remote_descr_offset = usize::from(be16(c, 14)?);
  let channel_indices = channel_list(c, 16, num_channels)?;
  let offset = 16 + num_channels * 2 + 6;
  let fpp = be16(c, offset)?;
  let rx_flow_name_offset = usize::from(be16(c, offset + 2)?);

  let hostname = read_0term_str_from_buffer(packet, hostname_offset).map_err(|_| "bad hostname")?.to_owned();
  let rx_flow_name =
    read_0term_str_from_buffer(packet, rx_flow_name_offset).map_err(|_| "bad flow name")?.to_owned();

  let descr = packet.get(remote_descr_offset..remote_descr_offset + 8).ok_or("remote descriptor out of range")?;
  if descr[0] != 0x08 || descr[1] != 0x02 {
    warn!("expected 0x0802, got 0x{:02x}{:02x}", descr[0], descr[1]);
  }
  let rx_port = make_u16(descr[2], descr[3]);
  let rx_ip = Ipv4Addr::new(descr[4], descr[5], descr[6], descr[7]);
  // A flow without samples or without a frame size cannot be transmitted.
  if fpp == 0 || bits_per_sample < 8 {
    return Err("zero frame size or sample size");
  }
  Ok(FlowRequest { hostname, rx_flow_name, sample_rate, bits_per_sample, fpp, channel_indices, rx_ip, rx_port })
}

/// Decode the content of an update flow request (opcode `0x0102`): the flow handle and its new channel list.
fn parse_update_flow(c: &[u8]) -> Result<(FlowHandle, Vec<Option<usize>>), &'static str> {
  let handle: FlowHandle = c.get(0..6).and_then(|h| h.try_into().ok()).ok_or("truncated")?;
  let num_channels = usize::from(be16(c, 6)?);
  Ok((handle, channel_list(c, 8, num_channels)?))
}


#[cfg(test)]
mod tests {
  use super::*;

  /// A well-formed flow request packet for the given channel ids.
  fn flow_packet(channels: &[u16]) -> Vec<u8> {
    let n = channels.len();
    let content_len = 16 + n * 2 + 6 + 4;
    let strings = HEADER_LENGTH + content_len;
    let hostname_offset = strings;
    let flow_name_offset = strings + 5;
    let descr_offset = strings + 5 + 7;
    let mut p = vec![0u8; HEADER_LENGTH];
    p.extend_from_slice(&(hostname_offset as u16).to_be_bytes());
    p.extend_from_slice(&48_000u32.to_be_bytes());
    p.extend_from_slice(&24u32.to_be_bytes());
    p.extend_from_slice(&1u16.to_be_bytes());
    p.extend_from_slice(&(n as u16).to_be_bytes());
    p.extend_from_slice(&(descr_offset as u16).to_be_bytes());
    for ch in channels {
      p.extend_from_slice(&ch.to_be_bytes());
    }
    p.extend_from_slice(&[0u8; 6]);
    p.extend_from_slice(&192u16.to_be_bytes());
    p.extend_from_slice(&(flow_name_offset as u16).to_be_bytes());
    p.extend_from_slice(b"host\0");
    p.extend_from_slice(b"flow-a\0");
    p.extend_from_slice(&[0x08, 0x02, 0x1f, 0x90, 192, 168, 1, 7]);
    p
  }

  #[test]
  fn a_well_formed_flow_request_is_decoded() {
    let req = parse_flow_request(&flow_packet(&[1, 0, 3])).unwrap();
    assert_eq!(req.hostname, "host");
    assert_eq!(req.rx_flow_name, "flow-a");
    assert_eq!(req.sample_rate, 48_000);
    assert_eq!(req.bits_per_sample, 24);
    assert_eq!(req.fpp, 192);
    assert_eq!(req.channel_indices, vec![Some(0), None, Some(2)]);
    assert_eq!(req.rx_ip, Ipv4Addr::new(192, 168, 1, 7));
    assert_eq!(req.rx_port, 8080);
  }

  #[test]
  fn every_truncation_of_a_flow_request_is_an_error_not_a_panic() {
    let full = flow_packet(&[1, 2]);
    for len in 0..full.len() {
      assert!(parse_flow_request(&full[..len]).is_err(), "len {len}");
    }
  }

  #[test]
  fn an_absurd_channel_count_is_an_error() {
    let mut p = flow_packet(&[1]);
    p[HEADER_LENGTH + 12] = 0xff;
    p[HEADER_LENGTH + 13] = 0xff;
    assert!(parse_flow_request(&p).is_err());
  }

  #[test]
  fn zero_frame_size_is_refused() {
    let mut p = flow_packet(&[1]);
    let fpp_at = HEADER_LENGTH + 16 + 2 + 6;
    p[fpp_at] = 0;
    p[fpp_at + 1] = 0;
    assert_eq!(parse_flow_request(&p), Err("zero frame size or sample size"));
  }

  #[test]
  fn update_flow_is_decoded_and_checked() {
    let mut c = vec![1, 2, 3, 4, 5, 6, 0, 2, 0, 1, 0, 0];
    assert_eq!(parse_update_flow(&c).unwrap(), ([1, 2, 3, 4, 5, 6], vec![Some(0), None]));
    c[7] = 9;
    assert!(parse_update_flow(&c).is_err());
    assert!(parse_update_flow(&c[..5]).is_err());
    assert!(parse_update_flow(&[]).is_err());
  }

  /// Random and mutated packets never panic (a stand-in for a fuzzer; the generator is deterministic).
  #[test]
  fn random_and_mutated_packets_never_panic() {
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    let mut next = move || {
      state ^= state << 13;
      state ^= state >> 7;
      state ^= state << 17;
      state
    };
    let seed = flow_packet(&[1, 2, 3, 4]);
    for _ in 0..20_000 {
      let mut p = seed.clone();
      for _ in 0..(next() % 6) {
        let i = (next() as usize) % p.len();
        p[i] = next() as u8;
      }
      p.truncate((next() as usize) % (p.len() + 1));
      let _ = parse_flow_request(&p);
      let _ = parse_update_flow(&p);
      let random: Vec<u8> = (0..(next() % 80)).map(|_| next() as u8).collect();
      let _ = parse_flow_request(&random);
      let _ = parse_update_flow(&random);
    }
  }
}
