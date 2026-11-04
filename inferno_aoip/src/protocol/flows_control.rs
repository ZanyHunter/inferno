/*
start_code = 0x1102 = dbcp1

error responses:
opcode2 = 0103 = stream expired (i.e. no keepalives)
opcode2 = 0315 = too many tx flows
opcode2 = 0301 = sample rate mismatch
*/

use log::{debug, warn};
use std::{
  error::Error,
  io::ErrorKind,
  net::{IpAddr, SocketAddr},
  sync::{
    atomic::{AtomicU16, Ordering},
    Arc,
  },
  time::Duration,
};

use crate::device_info::DeviceInfo;
use crate::net_utils::MTU;
use bytebuffer::ByteBuffer;
use thiserror::Error;
use tokio::{
  net::UdpSocket,
  time::{timeout_at, Instant},
};

use super::req_resp::{make_packet, req_resp_packet, HEADER_LENGTH};

pub const PORT: u16 = 4455;

#[derive(Error, Debug)]
pub enum FlowControlError {
  #[error("flow not found")]
  FlowNotFound = 0x0103,
  #[error("too many TX flows")]
  TooManyTXFlows = 0x0315,
  #[error("sample rate mismatch")]
  SampleRateMismatch = 0x0301,
}

/// The transmitter answered a request with a result code other than OK.
#[derive(Error, Debug)]
#[error("transmitter returned error code {code:#06x}: {packet_hex}")]
pub struct RemoteError {
  pub code: u16,
  packet_hex: String,
}

impl RemoteError {
  /// True if the transmitter says the flow does not exist (any more): it expired the flow, or never had it.
  pub fn is_flow_not_found(&self) -> bool {
    self.code == FlowControlError::FlowNotFound as u16
  }
}

pub struct FlowsControlClient {
  seqnum: AtomicU16,
  self_info: Arc<DeviceInfo>,
}

pub type FlowHandle = [u8; 6];

impl FlowsControlClient {
  pub fn new(self_info: Arc<DeviceInfo>) -> Self {
    Self { seqnum: AtomicU16::new(1), self_info }
  }
  async fn connect(&self, rem_addr: &SocketAddr) -> tokio::io::Result<UdpSocket> {
    let socket = UdpSocket::bind(SocketAddr::new(IpAddr::V4(self.self_info.ip_address), 0)).await?;
    socket.connect(rem_addr).await?;
    return Ok(socket);
  }
  fn write_channels(buffer: &mut ByteBuffer, channels: &[Option<u16>]) {
    for ch in channels {
      buffer.write_u16(ch.unwrap_or(0));
    }
  }
  async fn send_and_wait_for_reply<'a>(
    &self,
    recvbuf: &'a mut [u8],
    socket: &UdpSocket,
    _start_code: u16, // looks like version number, so we don't use it, forcing our version instead
    opcode1: u16,
    content: &[u8],
  ) -> Result<req_resp_packet::View<&'a [u8]>, Box<dyn Error>> {
    let send_seqnum = self.seqnum.fetch_add(1, Ordering::AcqRel);
    let pkt_to_send = make_packet(recvbuf, /*start_code*/ 0x1102, send_seqnum, opcode1, 0, content);
    socket.send(pkt_to_send).await?;
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
      let recv_size = match timeout_at(deadline, socket.recv(recvbuf)).await {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => {
          return Err(Box::new(e));
        }
        Err(_) => {
          return Err(Box::new(std::io::Error::from(ErrorKind::TimedOut)));
        }
      };
      if recv_size < HEADER_LENGTH {
        return Err(Box::new(std::io::Error::from(ErrorKind::UnexpectedEof)));
      }
      {
        let resp = req_resp_packet::View::new(&recvbuf[..recv_size]);
        if resp.opcode1().read() == opcode1 && resp.seqnum().read() == send_seqnum {
          if resp.opcode2().read() == 1 {
            return Ok(req_resp_packet::View::new(&recvbuf[..recv_size]));
            // creating another View is necessary because https://users.rust-lang.org/t/solved-borrow-doesnt-drop-returning-this-value-requires-that/24182
          } else {
            // (not logged here: what an error means depends on the request, the caller decides)
            let code = resp.opcode2().read();
            return Err(Box::new(RemoteError { code, packet_hex: hex::encode(&resp.into_storage()) }));
          }
        } else {
          warn!("received spurious packet: {}", hex::encode(&resp.into_storage()));
        }
      }
    }
  }
  pub async fn request_flow(
    &self,
    rem_addr: &SocketAddr,
    dbcp1: u16,
    sample_rate: u32,
    bits_per_sample: u32,
    fpp: u16,
    channels: &[Option<u16>],
    rx_port: u16,
    rx_flow_name: &str,
  ) -> Result<FlowHandle, Box<dyn Error>> {
    let socket = self.connect(rem_addr).await?;
    let mut body = ByteBuffer::new();
    let mut strings = ByteBuffer::new();
    let strings_offset = 0x26 + channels.len() * 2 + HEADER_LENGTH;
    body.write_u16(strings_offset as u16);
    strings.write_bytes(self.self_info.friendly_hostname().as_bytes());
    strings.write_u8(0);
    let rx_flow_name_offset = (strings.get_wpos() + strings_offset) as u16;
    strings.write_bytes(rx_flow_name.as_bytes());
    strings.write_u8(0);
    body.write_u32(sample_rate);
    body.write_u32(bits_per_sample);
    body.write_u16(1);
    body.write_u16(channels.len() as u16);
    while (strings.get_wpos() + strings_offset) % 8 != 0 {
      strings.write_u8(0);
    }
    body.write_u16((strings.get_wpos() + strings_offset) as u16);
    strings.write_u16(0x0802);
    strings.write_u16(rx_port);
    strings.write_bytes(&self.self_info.ip_address.octets());
    Self::write_channels(&mut body, channels);
    body.write_u16((0x1c + 2 * channels.len()) as u16);
    body.write_u16(0x0a00);
    body.write_u16(0x0002);
    body.write_u16(fpp);
    body.write_u16(rx_flow_name_offset);
    /* for _ in 0..6 {
      body.write_u16(self.self_info.process_id); // XXX testing
    } */
    body.write_bytes(&[0; 12]); // XXX testing
    assert_eq!(body.get_wpos(), strings_offset - HEADER_LENGTH);
    body.write_bytes(strings.as_bytes());

    let mut recvbuf = [0; MTU];
    let resp =
      self.send_and_wait_for_reply(&mut recvbuf, &socket, dbcp1, 0x0100, body.as_bytes()).await?;
    if resp.content().len() < 6 {
      return Err(Box::new(std::io::Error::from(ErrorKind::UnexpectedEof)));
    }
    let mut handle = [0u8; 6];
    handle.copy_from_slice(&resp.content()[0..6]);
    return Ok(handle);
  }

  pub async fn update_flow(
    &self,
    rem_addr: &SocketAddr,
    dbcp1: u16,
    handle: FlowHandle,
    channels: &[Option<u16>],
  ) -> Result<(), Box<dyn Error>> {
    let socket = self.connect(rem_addr).await?;
    let mut body = ByteBuffer::new();
    body.write_bytes(&handle);
    body.write_u16(channels.len() as u16);
    Self::write_channels(&mut body, channels);
    let mut recvbuf = [0; MTU];
    return self
      .send_and_wait_for_reply(&mut recvbuf, &socket, dbcp1, 0x0102, body.as_bytes())
      .await
      .map(|_| ());
  }

  pub async fn stop_flow(
    &self,
    rem_addr: &SocketAddr,
    dbcp1: u16,
    handle: FlowHandle,
  ) -> Result<(), Box<dyn Error>> {
    let socket = self.connect(rem_addr).await?;
    let mut body = ByteBuffer::new();
    body.write_bytes(&handle);
    let mut recvbuf = [0; MTU];
    // One request, never repeated. ONS fork (ZanyHunter/open-network-soundcard#15, control-plane.md section 7
    // finding 3): a lab capture showed 358 of 358 stops answered "flow not found". That answer means the
    // transmitter has already dropped the flow (it expires flows that get no keepalives), which is what the
    // stop wanted, so it is a success and nothing is sent again.
    match self.send_and_wait_for_reply(&mut recvbuf, &socket, dbcp1, 0x0101, body.as_bytes()).await {
      Ok(_) => Ok(()),
      Err(e) if e.downcast_ref::<RemoteError>().is_some_and(RemoteError::is_flow_not_found) => {
        debug!("flow {handle:?} at {rem_addr} is already gone (transmitter answered 'flow not found')");
        Ok(())
      }
      Err(e) => Err(e),
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use netdev::mac::MacAddr;
  use std::net::Ipv4Addr;

  fn minimal_device_info() -> DeviceInfo {
    DeviceInfo {
      ip_address: Ipv4Addr::new(127, 0, 0, 1),
      netmask: Ipv4Addr::new(0, 0, 0, 0),
      gateway: Ipv4Addr::new(0, 0, 0, 0),
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
      latency_ns: 0,
      sample_rate: 0,
      capabilities: crate::device_info::DeviceCapabilities::fixed_latency(0),
      arc_port: 0,
      cmc_port: 0,
      flows_control_port: 0,
      info_request_port: 0,
    }
  }

  /// A transmitter on loopback that answers every request with `code` and counts the requests it gets.
  async fn fake_transmitter(code: u16) -> (SocketAddr, Arc<std::sync::atomic::AtomicUsize>) {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let addr = socket.local_addr().unwrap();
    let requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = requests.clone();
    tokio::spawn(async move {
      let mut buf = [0u8; MTU];
      let mut out = [0u8; MTU];
      loop {
        let Ok((n, from)) = socket.recv_from(&mut buf).await else { return };
        counter.fetch_add(1, Ordering::SeqCst);
        let req = req_resp_packet::View::new(&buf[..n]);
        let reply = make_packet(&mut out, 0x1102, req.seqnum().read(), req.opcode1().read(), code, &[]);
        let _ = socket.send_to(reply, from).await;
      }
    });
    (addr, requests)
  }

  #[tokio::test]
  async fn a_stop_answered_flow_not_found_is_a_success_and_is_sent_once() {
    let (addr, requests) = fake_transmitter(FlowControlError::FlowNotFound as u16).await;
    let client = FlowsControlClient::new(Arc::new(minimal_device_info()));
    client.stop_flow(&addr, 0x1102, [1, 2, 3, 4, 5, 6]).await.expect("flow not found counts as stopped");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(requests.load(Ordering::SeqCst), 1, "the stop must not be repeated");
  }

  #[tokio::test]
  async fn a_stop_answered_ok_is_a_success_and_other_errors_still_fail() {
    let (addr, requests) = fake_transmitter(1).await;
    let client = FlowsControlClient::new(Arc::new(minimal_device_info()));
    client.stop_flow(&addr, 0x1102, [0; 6]).await.unwrap();
    assert_eq!(requests.load(Ordering::SeqCst), 1);

    let (addr, requests) = fake_transmitter(FlowControlError::TooManyTXFlows as u16).await;
    let err = client.stop_flow(&addr, 0x1102, [0; 6]).await.unwrap_err();
    let remote = err.downcast_ref::<RemoteError>().expect("a remote error");
    assert_eq!(remote.code, 0x0315);
    assert!(!remote.is_flow_not_found());
    assert_eq!(requests.load(Ordering::SeqCst), 1);
  }

  #[tokio::test]
  async fn a_flow_request_answered_flow_not_found_is_still_an_error() {
    // only the stop treats "flow not found" as done; an update of a flow the transmitter dropped must fail
    let (addr, _requests) = fake_transmitter(FlowControlError::FlowNotFound as u16).await;
    let client = FlowsControlClient::new(Arc::new(minimal_device_info()));
    let err = client.update_flow(&addr, 0x1102, [0; 6], &[Some(1)]).await.unwrap_err();
    assert!(err.downcast_ref::<RemoteError>().unwrap().is_flow_not_found());
  }

  #[test]
  fn write_channels_empty_writes_nothing() {
    let _client = FlowsControlClient::new(Arc::new(minimal_device_info()));
    let mut buffer = ByteBuffer::new();
    FlowsControlClient::write_channels(&mut buffer, &[]);
    assert_eq!(buffer.as_bytes(), &[]);
  }

  #[test]
  fn write_channels_some_none() {
    let _client = FlowsControlClient::new(Arc::new(minimal_device_info()));
    let mut buffer = ByteBuffer::new();
    FlowsControlClient::write_channels(&mut buffer, &[Some(1), Some(2), None, Some(4)]);
    assert_eq!(buffer.as_bytes(), &[0x00, 0x01, 0x00, 0x02, 0x00, 0x00, 0x00, 0x04]);
  }

  #[test]
  fn write_channels_many() {
    let _client = FlowsControlClient::new(Arc::new(minimal_device_info()));
    let mut buffer = ByteBuffer::new();
    let channels: Vec<Option<u16>> = (0..100).map(|i| Some(i)).collect();
    FlowsControlClient::write_channels(&mut buffer, &channels);
    assert_eq!(buffer.get_wpos(), 200);
  }
}
