use super::channels_subscriber::ChannelsSubscriber;
use crate::byte_utils::*;
use crate::common::*;
use crate::media_clock::{ClockStatusHandle, MediaClock};
use crate::net_utils::UdpSocketWrapper;
use crate::protocol::mcast::{make_packet, MulticastMessage};
use crate::{byte_utils::write_str_to_buffer, device_info::DeviceInfo};
use bytebuffer::ByteBuffer;
use std::sync::atomic::Ordering;
use std::sync::RwLock;
use std::{
  net::{IpAddr, Ipv4Addr, SocketAddr},
  sync::Arc,
  time::Duration,
};
use tokio::sync::watch;
use tokio::time::interval;
use tokio::{
  select,
  sync::{broadcast::Receiver as BroadcastReceiver, mpsc},
  time::MissedTickBehavior,
};

const SEND_BUFFER_SIZE: usize = 1500;
const DST_PORT_HEARTBEAT: u16 = 8708;
const DST_PORT_DEVICE_INFO: u16 = 8702;

pub type PeaksCallback = Box<dyn FnMut() -> (Vec<u8>, Vec<u8>) + Send + Sync>;

/// First word of the clock-stats reply with the "PLL not locked" flag (bit 0x01 of the last byte) set.
/// This is the value upstream Inferno always sent. The meaning of the bit comes from an upstream source
/// comment (`0x01 = PLL not locked`) **(verify against Dante Controller on a real network)**.
const CLOCK_STATS_FLAGS_PLL_NOT_LOCKED: [u8; 4] = [0x00, 0x03, 0x00, 0x03];
/// The same word with the "PLL not locked" bit clear **(verify)**.
const CLOCK_STATS_FLAGS_PLL_LOCKED: [u8; 4] = [0x00, 0x03, 0x00, 0x02];

/// Flags word of the clock-stats reply for a host clock that is or is not locked.
fn clock_stats_flags(locked: bool) -> [u8; 4] {
  if locked {
    CLOCK_STATS_FLAGS_PLL_LOCKED
  } else {
    CLOCK_STATS_FLAGS_PLL_NOT_LOCKED
  }
}

/// Content of the clock-stats reply (opcode `07 2a 00 20`), same layout as upstream Inferno:
/// flags word, `00 00 00 9f`, frequency offset in ppb (i32), our MAC, two zero bytes, the grandmaster id
/// twice, 76 zero bytes.
fn clock_stats_content(flags: [u8; 4], freq_offset_ppb: i32, mac: [u8; 6], master_clock: [u8; 8]) -> Vec<u8> {
  let mut bytes = ByteBuffer::new();
  bytes.set_endian(bytebuffer::Endian::BigEndian);
  bytes.write_bytes(&flags);
  bytes.write_bytes(&[0x00, 0x00, 0x00, 0x9f /* was 0xff */]);
  bytes.write_i32(freq_offset_ppb);
  bytes.write_bytes(&mac);
  bytes.write_u16(0);
  bytes.write_bytes(&master_clock);
  bytes.write_bytes(&master_clock);
  bytes.write_bytes(&[0u8; 76]);
  bytes.as_bytes().to_vec()
}

/// Upstream's source of the grandmaster id: the first 16 hex digits of `/tmp/clock-stats.<mac>0000*`,
/// written by a Linux PTP helper. Never present on Windows.
fn clock_stats_master_from_tmp_file(mac: &[u8; 6]) -> Option<[u8; 8]> {
  let required_prefix = format!("clock-stats.{}0000", hex::encode(mac));
  for entry in std::fs::read_dir("/tmp").ok()?.flatten() {
    if entry.file_name().to_string_lossy().starts_with(&required_prefix) {
      if let Ok(content) = std::fs::read_to_string(entry.path()) {
        let id = content.trim_ascii().get(0..16).and_then(|h| hex::decode(h).ok());
        if let Some(id) = id.and_then(|v| <[u8; 8]>::try_from(v).ok()) {
          return Some(id);
        }
      }
    }
  }
  None
}

struct Multicaster<'s> {
  self_info: &'s DeviceInfo,
  pub server: UdpSocketWrapper,
  seqnum: u16,
  vendor: [u8; 8],
  firmware_version_bytes: [u8; 4],
  product_version_bytes: [u8; 4],
  device_info_destination: SocketAddr,
  heartbeat_destination: SocketAddr,
  send_buffer: [u8; SEND_BUFFER_SIZE],
  clock: Arc<RwLock<MediaClock>>,
  channels_subscriber: Option<Arc<ChannelsSubscriber>>,
  get_peaks: PeaksCallback,
  had_clock: bool,
  host_clock_status: Option<ClockStatusHandle>,
}

impl<'s> Multicaster<'s> {
  pub fn new(
    self_info: &'s DeviceInfo,
    server: UdpSocketWrapper,
    clock: Arc<RwLock<MediaClock>>,
    host_clock_status: Option<ClockStatusHandle>,
    get_peaks: PeaksCallback,
  ) -> Multicaster {
    let patch_version = env!("CARGO_PKG_VERSION_PATCH").parse::<u16>().unwrap();
    let mut r = Multicaster {
      self_info,
      server,
      seqnum: 1,
      vendor: [32; 8],
      firmware_version_bytes: [4, 1, 6, 2],
      product_version_bytes: [
        env!("CARGO_PKG_VERSION_MAJOR").parse::<u8>().unwrap(),
        env!("CARGO_PKG_VERSION_MINOR").parse::<u8>().unwrap(),
        H(patch_version),
        L(patch_version),
      ],
      device_info_destination: SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(224, 0, 0, 231)),
        DST_PORT_DEVICE_INFO,
      ),
      heartbeat_destination: SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(224, 0, 0, 233)),
        DST_PORT_HEARTBEAT,
      ),
      send_buffer: [0; SEND_BUFFER_SIZE],
      clock,
      channels_subscriber: None,
      get_peaks,
      had_clock: false,
      host_clock_status,
    };
    write_str_to_buffer(&mut r.vendor, 0, 8, &self_info.vendor_string);
    return r;
  }

  pub fn should_work(&self) -> bool {
    return self.server.should_work();
  }

  async fn send(&mut self, dst: SocketAddr, start_code: u16, opcode: [u8; 8], content: &[u8]) {
    let pkt = make_packet(
      &mut self.send_buffer,
      start_code,
      self.seqnum,
      self.self_info.process_id,
      self.self_info.factory_device_id,
      self.vendor,
      opcode,
      content,
    );
    self.seqnum = self.seqnum.wrapping_add(1);
    self.server.send(&dst, pkt).await;
  }

  async fn send_board_info(&mut self) {
    let mut content = [0u8; 200];
    // Firmware version:
    content[0..4].copy_from_slice(&[4, 1, 0, 6]);
    content[0x23] = 2;
    // Hardware version:
    content[4..8].copy_from_slice(&[4, 1, 0, 3]);
    content[0x27] = 1;
    // Boot version:
    content[0x28..0x2c].copy_from_slice(&[1, 0, 0, 0]);

    // flags of supported features:
    // 0x14: AES67, Device Lock
    //       0x01 - ??? (was 1, od 0xd)
    //       0x04 - supports AES67
    //       0x08 - is lockable
    // 0x15: ??? (was 0x50)
    // 0x16:
    //       0x10 - has Manufacturer name
    //       0x40 - Network is configurable (supports static addressing)
    // 0x17: Identify device, Sample rate & encoding configuration, Reboot, Factory reset (was 0xdb)
    content[0x14] = 0;
    content[0x15] = 0;
    content[0x16] = 0x10;
    content[0x17] = 0;

    content[0xbb] = 0x1f; // if 0, device is flooded with info multicast requests around 1 per second
                          /* content[0xbf] = 5;
                          content[0xc3] = 3;
                          content[0xc7] = 3; */
    write_str_to_buffer(&mut content, 12, 8, &self.self_info.board_name);
    write_str_to_buffer(&mut content, 0x38, 16, &self.self_info.board_name);

    self.send(self.device_info_destination, 0xffff, [0x07, 0x2a, 0x00, 0x60, 0, 0, 0, 0], &content).await;
  }

  async fn send_product_info(&mut self) {
    let mut content = [0; 336];
    write_str_to_buffer(&mut content, 0, 8, &self.self_info.manufacturer);
    write_str_to_buffer(&mut content, 8, 8, &self.self_info.board_name);
    write_str_to_buffer(&mut content, 0x2c, 16, &self.self_info.manufacturer);
    write_str_to_buffer(&mut content, 0xac, 16, &self.self_info.model_name);
    // product version:
    //content[0x12c..0x130].copy_from_slice(&self.product_version_bytes);

    // firmware version:
    content[0x1c..0x20].copy_from_slice(&self.product_version_bytes);

    // 0x18..0x1b - software version
    // 0x24..0x26 - software patch version, u32
    // 0x28..0x2b - firmware patch version, u32

    self.send(self.device_info_destination, 0xffff, [0x07, 0x2a, 0x00, 0xc0, 0, 0, 0, 0], &content).await;
  }

  fn get_freq_offset_ppb(&self) -> Option<i32> {
    self
      .clock
      .read()
      .unwrap()
      .get_overlay()
      .as_ref()
      .map(|clkovl| {
        // freq_scale_including_hw() exists only in the Linux build of usrvclock (clock_adjtime).
        #[cfg(target_os = "linux")]
        let freq_scale = clkovl.freq_scale_including_hw();
        #[cfg(not(target_os = "linux"))]
        let freq_scale = clkovl.freq_scale;
        let freq_offset_f = (freq_scale * 1_000_000_000f64).round();
        if i32::MIN as f64 <= freq_offset_f && freq_offset_f <= i32::MAX as f64 {
          Some(freq_offset_f as i32)
        } else {
          None
        }
      })
      .flatten()
  }

  async fn send_heartbeat(&mut self) {
    let ctr = self.seqnum;
    let mut bytes = ByteBuffer::new();
    bytes.set_endian(bytebuffer::Endian::BigEndian);

    let freq_offset_opt = self.get_freq_offset_ppb();

    if let Some(freq_offset) = freq_offset_opt {
      bytes.write_u16(16); // length of this part
      bytes.write_u16(0x8001); // type
      bytes.write_u16(4); // ???
      bytes.write_u16(4); // maybe content length???
      bytes.write_u16(ctr);
      bytes.write_u16(0);
      bytes.write_i32(freq_offset);
      trace!("freq offset {freq_offset}/1000 ppm");

      /* bytes.write_bytes(&[
        0x00, 0x24, 0x80, 0x00,
        0x00, 0x04, 0x00, 0x04, H(ctr), L(ctr), 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x01, 0x00, 0x10,
        /* TX Bps, 4B: */ 0x00, 0x00, 0x01, 0xde, /* RX Bps, 4B: */ 0x00, 0x07, 0xdf, 0x17,
        /* TX errors, 4B: */ 0x00, 0x00, 0x00, 0x02, /* RX errors, 4B: */ 0x00, 0x00, 0x00, 0x07,
      ]); // network statistics */

      let (rx_peaks, tx_peaks) = (self.get_peaks)();
      let total_peaks = rx_peaks.len() + tx_peaks.len();
      if total_peaks > 0 {
        let mut total_len = 24 + total_peaks;
        while (total_len % 4) != 0 {
          total_len += 1;
        }
        let end_pos = bytes.get_wpos() + total_len;
        bytes.write_u16((24 + total_peaks).try_into().unwrap());
        bytes.write_u16(0x8002);
        bytes.write_u16(4);
        bytes.write_u16((12 + total_peaks).try_into().unwrap());
        bytes.write_u16(ctr);
        bytes.write_u16(0);
        bytes.write_u16(tx_peaks.len().try_into().unwrap());
        bytes.write_u16(0);
        bytes.write_u16(rx_peaks.len().try_into().unwrap());
        bytes.write_u16(0);
        bytes.write_u16(24);
        bytes.write_u16(0);
        for peak in tx_peaks.into_iter().chain(rx_peaks.into_iter()) {
          bytes.write_u8(peak);
        }
        while bytes.get_wpos() < end_pos {
          bytes.write_u8(0);
        }
      }

      // rx latency:
      if let Some(chsub) = self.channels_subscriber.as_ref() {
        let flows_info = chsub.flows_info();
        let flows_info = flows_info.read().unwrap();
        let flows_count = flows_info.len() as u16;
        bytes.write_u16(24 + flows_count * 4);
        bytes.write_u16(0x8003);
        bytes.write_u16(4);
        bytes.write_u16(12 + flows_count * 4); // content length
        bytes.write_u16(ctr);
        bytes.write_u16(0);
        bytes.write_u16(flows_count); // number of flows
        bytes.write_u16(0);
        bytes.write_u16(24);
        bytes.write_u16(0);
        bytes.write_u32(self.self_info.sample_rate);

        for opt in flows_info.iter() {
          let latency =
            opt.as_ref().map(|fi| fi.actual_latency_samples.swap(0, Ordering::Relaxed)).unwrap_or(0);
          bytes.write_u32(latency.clamp(0, i32::MAX) as u32);
        }
      }

      /* bytes.write_bytes(&[
        0x00, 0x1c, 0x80, 0x04,
        0x00, 0x04, 0x00, 0x10,  H(ctr), L(ctr), 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
      ]); */
      /*
      0x00, 0x1c, 0x80, 0x04, 0x00, 0x04, 0x00, 0x10, 0x17, 0x0f, 0x00, 0x00,
      0x00, 0x02, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00, missed packets, 4B: 0x00, 0x03, 0x90, 0x1e, 0x00, 0x00, 0x00, 0x00
       */
      if !self.had_clock {
        debug!("clock appeared");
      }
    } else {
      debug!("no clock available");
    }
    self.had_clock = freq_offset_opt.is_some();

    let content = bytes.as_bytes();
    self.send(self.heartbeat_destination, 0xfffe, [0, 8, 0, 1, 0x10, 0, 0, 0], &content).await;

    // this is probably response to 0738008100000064
    /* self.send(
      self.device_info_destination, 0xffff, [0x07, 0x2a, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00],
      &[0x00, 0x18, 0x00, 0x04, 0x00, 0x00, 0xbb, 0x80, 0x00, 0x00, 0xbb, 0x80, 0x00, 0x02, 0x00, 0x00,
      // supported sample rates:
      0x00, 0x00, /* 44100: */ 0xac, 0x44, 0x00, 0x00, 0xbb, 0x80, 0x00, 0x01, 0x58, 0x88, 0x00, 0x01, 0x77, 0x00]
    ).await; */

    /* self.send(
      self.device_info_destination, 0xffff, [0x07, 0x2a, 0x10, 0x07, 0, 0, 0, 0],
      &[0, 0, 0, 0]
    ).await; */

    // this is probably response to 0738008300000064
    /* self.send(
      self.device_info_destination, 0xffff, [0x07, 0x2a, 0x00, 0x82, 0x00, 0x00, 0x00, 0x00],
    &[
      0x00, 0x18, 0x00, 0x03, 0x00, 0x00, 0x00, 0x18, 0x00, 0x00, 0x00, 0x18, 0x00, 0x02, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x18, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x20
    ]).await; */

    /* self.send(
    self.device_info_destination, 0xffff, [0x07, 0x2a, 0x10, 0x09, 0x00, 0x00, 0x00, 0x00],
    &[
      0x00, 0x00, 0x00, 0x04, 0x00, 0x02, 0x00, 0x08, 0x00, 0x18, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      /* clock source: */0x00, 0x1d, 0xc1, 0xff, 0xfe, 0x11, 0x11, 0x33,
      /* transmitting to us??? : */ 0x00, 0x1d, 0xc1, 0xff, 0xfe, 0x11, 0x66, 0x33,
    ]).await; */
  }

  async fn send_clock_stats(&mut self) {
    let freq_offset = if let Some(f) = self.get_freq_offset_ppb() {
      f
    } else {
      return;
    };
    let mac = self.self_info.mac_address.octets();
    // ONS fork: the host-supplied status wins when there is one; the Linux file is the fallback.
    let (mc, flags) = match &self.host_clock_status {
      Some(handle) => {
        let status = handle.get();
        match status.master_clock_id {
          Some(id) => (id, clock_stats_flags(status.locked)),
          None => return, // no leader followed: stay silent, Dante Controller shows the clock red
        }
      }
      None => match clock_stats_master_from_tmp_file(&mac) {
        // The file carries no lock information: send the historic flags word unchanged.
        Some(id) => (id, CLOCK_STATS_FLAGS_PLL_NOT_LOCKED),
        None => return,
      },
    };
    let content = clock_stats_content(flags, freq_offset, mac, mc);
    self
      .send(
        self.device_info_destination,
        0xffff,
        [0x07, 0x2a, 0x00, 0x20, 0x00, 0x00, 0x00, 0x00],
        &content,
      )
      .await;
  }

  async fn send_network_info(&mut self) {
    let mut bytes = ByteBuffer::new();
    bytes.set_endian(bytebuffer::Endian::BigEndian);
    bytes.write_bytes(&[0x00, 0x01, 0x00, 0x00, 0x00, 0x00]);
    bytes.write_u16(self.self_info.link_speed);
    bytes.write_u16(1);
    bytes.write_bytes(&self.self_info.mac_address.octets());
    bytes.write_bytes(&self.self_info.ip_address.octets());
    bytes.write_bytes(&self.self_info.netmask.octets());
    bytes.write_bytes(&self.self_info.gateway.octets());
    bytes.write_bytes(&self.self_info.gateway.octets()); // DNS? doesn't really matter.
    bytes.write_bytes(&[
      0x00, 0x18, 0x00, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ]);

    self
      .send(
        self.device_info_destination,
        0xffff,
        [0x07, 0x2a, 0x00, 0x11, 0x00, 0x00, 0x00, 0x00],
        bytes.as_bytes(),
      )
      .await;
  }
}

pub async fn run_server(
  self_info: Arc<DeviceInfo>,
  mut rx: mpsc::Receiver<MulticastMessage>,
  clock: Arc<RwLock<MediaClock>>,
  host_clock_status: Option<ClockStatusHandle>,
  mut channels_sub_rx: watch::Receiver<Option<Arc<ChannelsSubscriber>>>,
  get_peaks: PeaksCallback,
  shutdown: BroadcastReceiver<()>,
) {
  let server =
    UdpSocketWrapper::new(Some(self_info.ip_address), self_info.info_request_port, shutdown).await;
  let mut recv_buff = crate::net_utils::ReceiveBuffer::new();
  let mut mcaster = Multicaster::new(self_info.as_ref(), server, clock, host_clock_status, get_peaks);
  mcaster.send_board_info().await;
  mcaster.send_product_info().await;
  let mut heartbeat_interval = interval(Duration::from_secs(1));
  heartbeat_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
  while mcaster.should_work() {
    select! {
      r = mcaster.server.recv(&mut recv_buff) => {
        let (_src, request_buf) = match r {
          Some(v) => v,
          None => continue
        };
        if request_buf.len() < 32 {
          error!("too short packet received: {}", hex::encode(request_buf));
          continue;
        }
        let opcode = &request_buf[24..32];
        match opcode {
          [0x07, _, 0, 0x61, 0, 0, 0, 0] => {
            mcaster.send_board_info().await;
          },
          [0x07, _, 0, 0xc1, 0, 0, 0, 0] => {
            mcaster.send_product_info().await;
          },
          [0x07, _ /* was 0x38 */, 0, 0x21, 0, 0, 0, _ /* was 0x64 */] => {
            mcaster.send_clock_stats().await;
          },
          [0x07, _, 0, 0x13, 0, 0, 0, _] => {
            mcaster.send_network_info().await;
          }
          [0x07, _, 0, 0x77, 0, 0, 0, _]=> {
            mcaster.send(
              mcaster.device_info_destination, 0xffff, [0x07, 0x2a, 0x00, 0x78, 0, 0, 0, 0],
              &[0, 0, 0, 3, 0, 0, 0, 0]
            ).await;
          }
          _ => {
            warn!("unknown request to multicast port: opcode: {}", hex::encode(opcode));
            warn!("raw udp payload: {}", hex::encode(request_buf));
          }
        };
      },
      m = rx.recv() => {
        // TODO we could also make seqnum atomic and simply share socket with anyone that wants it
        if let Some(msg) = m {
          mcaster.send(mcaster.device_info_destination, msg.start_code, msg.opcode, &msg.content).await;
        } else {
          break;
        }
      },
      _ = heartbeat_interval.tick() => {
        mcaster.send_heartbeat().await;
      },
      _ = channels_sub_rx.changed() => {
        mcaster.channels_subscriber = channels_sub_rx.borrow_and_update().clone();
      }
      // TODO receive shutdown properly, currently Ctrl-C doesn't work if there is error binding to socket
    };
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::media_clock::{ClockOverlay, HostClockStatus};
  use netdev::mac::MacAddr;
  use tokio::net::UdpSocket;
  use tokio::sync::broadcast;

  const MAC: [u8; 6] = [0x02, 0x4f, 0x4e, 0x53, 0x13, 0x36];
  const MASTER: [u8; 8] = [0x00, 0x1d, 0xc1, 0x20, 0x51, 0xf0, 0x00, 0x00];

  fn test_info() -> DeviceInfo {
    DeviceInfo {
      ip_address: Ipv4Addr::LOCALHOST,
      netmask: Ipv4Addr::UNSPECIFIED,
      gateway: Ipv4Addr::UNSPECIFIED,
      mac_address: MacAddr::new(MAC[0], MAC[1], MAC[2], MAC[3], MAC[4], MAC[5]),
      link_speed: 1000,
      board_name: String::new(),
      manufacturer: String::new(),
      model_name: String::new(),
      model_number: String::new(),
      factory_device_id: [1, 2, 3, 4, 5, 6, 7, 8],
      process_id: 9,
      vendor_string: String::new(),
      friendly_hostname: String::new(),
      factory_hostname: String::new(),
      rx_channels: Vec::new(),
      tx_channels: Vec::new(),
      bits_per_sample: 24,
      pcm_type: 0xe,
      latency_ns: 0,
      sample_rate: 48_000,
      arc_port: 0,
      cmc_port: 0,
      flows_control_port: 0,
      info_request_port: 0,
    }
  }

  fn running_clock() -> Arc<RwLock<MediaClock>> {
    let mut clock = MediaClock::new(false);
    clock.update_overlay(ClockOverlay { clock_id: 1, last_sync: 0, shift: 0, freq_scale: 12.5e-6 });
    Arc::new(RwLock::new(clock))
  }

  /// Runs `send_clock_stats` on a `Multicaster` whose "device info group" is a loopback socket (nothing
  /// leaves the machine) and returns the datagram it produced, if any.
  async fn clock_stats_reply(
    host: Option<ClockStatusHandle>,
    clock: Arc<RwLock<MediaClock>>,
  ) -> (Option<Vec<u8>>, Option<i32>) {
    let info = test_info();
    let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);
    let server = UdpSocketWrapper::new(Some(Ipv4Addr::LOCALHOST), 0, shutdown_rx).await;
    let mut mcaster = Multicaster::new(&info, server, clock, host, Box::new(|| (vec![], vec![])));
    let sink = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    mcaster.device_info_destination = sink.local_addr().unwrap();
    let freq_offset = mcaster.get_freq_offset_ppb();
    mcaster.send_clock_stats().await;
    let mut buf = [0u8; 1500];
    let got = tokio::time::timeout(Duration::from_millis(300), sink.recv_from(&mut buf)).await;
    (got.ok().map(|r| buf[..r.unwrap().0].to_vec()), freq_offset)
  }

  #[tokio::test]
  async fn host_status_gives_clock_stats_with_master_mac_offset_and_locked_flag() {
    let handle = ClockStatusHandle::new();
    handle.set(HostClockStatus { master_clock_id: Some(MASTER), locked: true });
    let (pkt, freq_offset) = clock_stats_reply(Some(handle), running_clock()).await;
    let pkt = pkt.expect("a clock-stats reply");
    let freq_offset = freq_offset.expect("clock has an overlay");
    #[cfg(not(target_os = "linux"))]
    assert_eq!(freq_offset, 12_500);

    assert_eq!(pkt.len(), 32 + 112);
    assert_eq!(&pkt[0..2], &[0xff, 0xff], "start code");
    assert_eq!(u16::from_be_bytes([pkt[2], pkt[3]]) as usize, pkt.len());
    assert_eq!(&pkt[8..16], &[1, 2, 3, 4, 5, 6, 7, 8], "factory device id");
    assert_eq!(&pkt[24..32], &[0x07, 0x2a, 0x00, 0x20, 0, 0, 0, 0], "opcode");
    let c = &pkt[32..];
    assert_eq!(&c[0..4], &CLOCK_STATS_FLAGS_PLL_LOCKED);
    assert_eq!(c[0..4], [0x00, 0x03, 0x00, 0x02]);
    assert_eq!(&c[4..8], &[0x00, 0x00, 0x00, 0x9f]);
    assert_eq!(&c[8..12], &freq_offset.to_be_bytes());
    assert_eq!(&c[12..18], &MAC);
    assert_eq!(&c[18..20], &[0, 0]);
    assert_eq!(&c[20..28], &MASTER);
    assert_eq!(&c[28..36], &MASTER);
    assert!(c[36..].iter().all(|b| *b == 0));
  }

  #[tokio::test]
  async fn unlocked_host_status_sets_the_pll_not_locked_flag() {
    let handle = ClockStatusHandle::new();
    handle.set(HostClockStatus { master_clock_id: Some(MASTER), locked: false });
    let (pkt, _) = clock_stats_reply(Some(handle), running_clock()).await;
    let pkt = pkt.expect("a clock-stats reply");
    let flags = &pkt[32..36];
    assert_eq!(flags, &CLOCK_STATS_FLAGS_PLL_NOT_LOCKED);
    assert_eq!(flags[3] & 0x01, 0x01);
    // Only the flags word differs from the locked reply.
    assert_eq!(
      clock_stats_content(clock_stats_flags(true), 5, MAC, MASTER)[4..],
      clock_stats_content(clock_stats_flags(false), 5, MAC, MASTER)[4..]
    );
    assert_eq!(clock_stats_flags(true)[3] & 0x01, 0);
  }

  #[tokio::test]
  async fn no_leader_sends_nothing_and_updates_are_seen() {
    let handle = ClockStatusHandle::new();
    // Default: no leader.
    let (pkt, _) = clock_stats_reply(Some(handle.clone()), running_clock()).await;
    assert!(pkt.is_none(), "nothing may be sent without a leader");

    handle.set(HostClockStatus { master_clock_id: Some(MASTER), locked: true });
    let (pkt, _) = clock_stats_reply(Some(handle.clone()), running_clock()).await;
    assert!(pkt.is_some());

    // The leader stops: back to silence.
    handle.set(HostClockStatus::default());
    let (pkt, _) = clock_stats_reply(Some(handle), running_clock()).await;
    assert!(pkt.is_none());
  }

  #[tokio::test]
  async fn no_clock_overlay_sends_nothing() {
    let handle = ClockStatusHandle::new();
    handle.set(HostClockStatus { master_clock_id: Some(MASTER), locked: true });
    let clock = Arc::new(RwLock::new(MediaClock::new(false)));
    let (pkt, freq_offset) = clock_stats_reply(Some(handle), clock).await;
    assert!(freq_offset.is_none());
    assert!(pkt.is_none());
  }

  #[tokio::test]
  async fn without_a_host_handle_the_tmp_file_path_is_used() {
    // No such file for this MAC: nothing is sent (and on Windows /tmp does not even exist).
    let (pkt, _) = clock_stats_reply(None, running_clock()).await;
    assert!(pkt.is_none());
    assert_eq!(clock_stats_master_from_tmp_file(&MAC), None);
  }
}
