//! Integration test: the ARC server answers a Dante Controller's receive-channel (subscription) list.
//!
//! ONS fork (ZanyHunter/open-network-soundcard#15, control-plane.md section 7 finding 1): in the lab capture of
//! 2026-11-01, Dante Controller sent opcode `0x3000` 1,124 times and an old ONS build never answered. The
//! request bytes below are the ones in `docs/protocol-notes/pcaps/ons-dc-device-view-20261101.pcapng` (ONS
//! repository, excerpt frames 16, 29, 30, 37), sent to a real `DeviceServer` over loopback.

use std::collections::BTreeMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use inferno_aoip::device_server::{ClockOverlay, ClockPublisher, DeviceServer, Sample, Settings};
use tokio::net::UdpSocket;

const ALT_PORT: u16 = 9300;
const RX_CHANNELS: usize = 16;

fn make_settings() -> Settings {
  let mut config = BTreeMap::new();
  config.insert("NAME".to_string(), "arc_loopback".to_string());
  config.insert("PROCESS_ID".to_string(), "7".to_string());
  config.insert("ALT_PORT".to_string(), ALT_PORT.to_string());
  config.insert("RX_CHANNELS".to_string(), RX_CHANNELS.to_string());
  config.insert("TX_CHANNELS".to_string(), "8".to_string());
  config.insert("SAMPLE_RATE".to_string(), "48000".to_string());
  config.insert("BIND_IP".to_string(), "127.0.0.1".to_string());
  let state_dir = std::env::temp_dir().join("inferno_arc_loopback");
  config.insert("STATE_DIR".to_string(), state_dir.to_string_lossy().into_owned());
  let mut settings = Settings::new("arc_loopback", "arc_loopback", Some(Ipv4Addr::LOCALHOST), &config);
  settings.make_rx_channels(RX_CHANNELS);
  settings.make_tx_channels(8);
  settings
}

/// Sends one request datagram and returns the reply, or `None` if nothing came back within 2 s.
async fn ask(socket: &UdpSocket, request: &[u8]) -> Option<Vec<u8>> {
  let mut buf = vec![0u8; 2048];
  for _ in 0..3 {
    socket.send(request).await.unwrap();
    if let Ok(Ok(n)) = tokio::time::timeout(Duration::from_secs(2), socket.recv(&mut buf)).await {
      buf.truncate(n);
      return Some(buf);
    }
  }
  None
}

/// Checks the 10-byte header of a reply: start code, length, the request's sequence number and opcode, and
/// result code 1 (OK). Returns the content.
fn check_reply<'a>(request: &[u8], reply: &'a [u8]) -> &'a [u8] {
  assert!(reply.len() >= 12, "reply too short: {}", hex::encode(reply));
  assert_eq!(&reply[0..2], &request[0..2], "start code");
  assert_eq!(usize::from(u16::from_be_bytes([reply[2], reply[3]])), reply.len(), "length field");
  assert_eq!(&reply[4..6], &request[4..6], "sequence number");
  assert_eq!(&reply[6..8], &request[6..8], "opcode");
  assert_eq!(&reply[8..10], &[0, 1], "result code");
  &reply[10..]
}

// ARC `0x3000` requests exactly as Dante Controller sent them in the lab capture: the sequence number, the
// opcode and the paging words `0001 <1-based start index> 0000`.
const REQ_3000_FROM_1: &str = "2729001002a230000000000100010000"; // excerpt frame 16
const REQ_3000_FROM_1_LATER: &str = "27290010030f30000000000100010000"; // frame 29
const REQ_3000_FROM_16: &str = "27290010031030000000000100100000"; // frame 30: start index 0x10

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_receive_channel_list_is_answered() {
  let _ = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).try_init();

  let running = Arc::new(AtomicBool::new(true));
  let clock = ClockPublisher::new();
  let clock_thread = {
    let (clock, running) = (clock.clone(), running.clone());
    std::thread::spawn(move || {
      while running.load(Ordering::Relaxed) {
        clock.publish(ClockOverlay { clock_id: 1, last_sync: 0, shift: 0, freq_scale: 0.0 });
        std::thread::sleep(Duration::from_millis(100));
      }
    })
  };
  tokio::time::sleep(Duration::from_millis(300)).await;

  let mut server = DeviceServer::start_with_clock(make_settings(), Some(clock)).await;

  let socket = UdpSocket::bind(SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 0)).await.unwrap();
  socket.connect(SocketAddr::new(Ipv4Addr::LOCALHOST.into(), ALT_PORT)).await.unwrap();

  // A control request the server certainly knows, so a missing `0x3000` reply is not a dead server.
  let counts_request = hex::decode("2729000a029810000000").unwrap();
  let counts = ask(&socket, &counts_request).await.expect("no answer to 0x1000");
  check_reply(&counts_request, &counts);

  // Phase 1: before any receive callback exists (no subscriber yet).
  // Phase 2: after receive_with_callback creates the subscriber, which is what ONS does.
  for phase in 1..=2 {
    for (name, request_hex, first_channel) in [
      ("from channel 1", REQ_3000_FROM_1, 1u16),
      ("from channel 1, later sequence", REQ_3000_FROM_1_LATER, 1),
      ("from channel 16", REQ_3000_FROM_16, 16),
    ] {
      let request = hex::decode(request_hex).unwrap();
      let reply = ask(&socket, &request)
        .await
        .unwrap_or_else(|| panic!("phase {phase}: no answer to 0x3000 {name} (the old build's failure)"));
      let content = check_reply(&request, &reply);
      // `u8 slots, u8 count`, then 20 bytes per channel `(id, 6, common descriptor, ...)`.
      assert!(content.len() >= 2, "phase {phase} {name}: no item header");
      let count = usize::from(content[1]);
      if phase == 2 {
        let expected = RX_CHANNELS - usize::from(first_channel) + 1;
        assert_eq!(count, expected, "phase 2 {name}: channels listed");
        assert_eq!(u16::from_be_bytes([content[2], content[3]]), first_channel, "first channel id");
        assert_eq!(u16::from_be_bytes([content[4], content[5]]), 6, "item type word");
      }
    }
    // A subscribe request (here with no items) and a remove-subscription request for a channel that does
    // not exist are answered in both phases: refused before the receiver runs (never silence), and once it
    // runs the first is OK and the second is refused because the channel does not exist.
    for (name, request_hex, expected_code) in [
      ("0x3010 without items", "2729000c0400301000000000", if phase == 1 { 0xFFFFu16 } else { 1 }),
      ("0x3014 nonexistent channel", "27ff0010040130140000000100000063", 0xFFFF),
    ] {
      let request = hex::decode(request_hex).unwrap();
      let reply = ask(&socket, &request)
        .await
        .unwrap_or_else(|| panic!("phase {phase}: no answer to {name}"));
      assert!(reply.len() >= 10, "phase {phase} {name}: short reply");
      assert_eq!(&reply[6..8], &request[6..8], "{name}: opcode");
      assert_eq!(u16::from_be_bytes([reply[8], reply[9]]), expected_code, "phase {phase} {name}: result code");
    }
    if phase == 1 {
      server.receive_with_callback(Box::new(|_n: usize, _ch: &Vec<Vec<Sample>>| {})).await;
      // let the ARC loop pick up the new subscriber
      tokio::time::sleep(Duration::from_millis(300)).await;
    }
  }

  server.shutdown().await;
  running.store(false, Ordering::Relaxed);
  let _ = clock_thread.join();
}
