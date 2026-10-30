//! Socket options for multicast egress and audio buffers (ONS fork, issue W6).
//!
//! Upstream binds every socket to the NIC's unicast IP and relies on the OS for everything else. That
//! is enough on Linux for unicast, but on Windows the egress interface of a *multicast* datagram is
//! chosen from the routing table (lowest interface metric) and not from the source address the socket
//! is bound to, so on a multi-NIC PC Dante multicast can leave on the wrong NIC. Windows also ships a
//! 64 KiB default receive buffer, which holds only about 44 full audio packets and overflows on a short
//! scheduling hiccup.
//!
//! The generic helpers here compile and work on every platform (so they are unit-testable on Linux CI).
//! The `*_platform` wrappers apply them on Windows only, keeping Linux behaviour exactly as upstream.
//!
//! This file is part of the ONS fork of Inferno and is licensed GPL-3.0-or-later, like the rest of
//! `inferno_aoip`.

use std::io;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use log::warn;
use socket2::{Domain, Protocol, SockRef, Socket, Type};

/// `SO_RCVBUF` for audio RX sockets, in bytes.
///
/// A flow carries at most `MAX_PAYLOAD_BYTES` (1400) of audio plus headers, about 1.45 KiB per packet.
/// A busy 4 flow receiver at 48 kHz with 4 samples per packet is about 12000 packets/s, about 17 MB/s,
/// so 2 MiB buffers about 120 ms (more for smaller packets). That comfortably covers a 10 to 20 ms
/// scheduler stall on a non-realtime thread and still costs little memory per flow.
/// The Windows default is 64 KiB (about 4 ms at that rate).
pub const AUDIO_RCVBUF_BYTES: usize = 2 * 1024 * 1024;

/// `SO_SNDBUF` for audio TX sockets, in bytes.
///
/// TX sends non-blocking from one paced thread; 512 KiB holds a few hundred packets so a transient NIC
/// queue stall does not turn into `WSAEWOULDBLOCK` and a dropped packet. Windows default is 64 KiB.
pub const AUDIO_SNDBUF_BYTES: usize = 512 * 1024;

/// Sets `IP_MULTICAST_IF` so multicast datagrams sent on `sock` leave via the NIC owning `iface`.
pub fn set_multicast_egress(sock: SockRef<'_>, iface: Ipv4Addr) -> io::Result<()> {
  sock.set_multicast_if_v4(&iface)
}

/// Sets `SO_RCVBUF` and `SO_SNDBUF` to the requested sizes. Either may be `None` to leave it alone.
///
/// The OS may round or clamp the value (Linux doubles it and clamps to `rmem_max`/`wmem_max`), so this
/// returns the sizes the OS reports afterwards as `(recv, send)`.
pub fn set_buffer_sizes(
  sock: SockRef<'_>,
  recv: Option<usize>,
  send: Option<usize>,
) -> io::Result<(usize, usize)> {
  if let Some(n) = recv {
    sock.set_recv_buffer_size(n)?;
  }
  if let Some(n) = send {
    sock.set_send_buffer_size(n)?;
  }
  Ok((sock.recv_buffer_size()?, sock.send_buffer_size()?))
}

/// Windows only: select the multicast egress NIC for a socket that may send multicast.
/// No-op elsewhere. An unspecified `iface` (`0.0.0.0`) is ignored.
pub fn apply_multicast_egress_platform<S>(sock: &S, iface: Ipv4Addr)
where
  for<'a> SockRef<'a>: From<&'a S>,
{
  if cfg!(windows) && !iface.is_unspecified() {
    // Not fatal: the socket still works, just possibly on the NIC the routing table prefers.
    if let Err(e) = set_multicast_egress(SockRef::from(sock), iface) {
      warn!("IP_MULTICAST_IF {iface} failed (continuing with OS default): {e}");
    }
  }
}

/// Windows only: enlarge buffers on an audio socket. `recv`/`send` select which directions matter
/// for this socket. No-op elsewhere.
pub fn apply_audio_buffers_platform<S>(sock: &S, recv: bool, send: bool)
where
  for<'a> SockRef<'a>: From<&'a S>,
{
  if cfg!(windows) {
    let r = set_buffer_sizes(
      SockRef::from(sock),
      recv.then_some(AUDIO_RCVBUF_BYTES),
      send.then_some(AUDIO_SNDBUF_BYTES),
    );
    match r {
      Ok((rcv, snd)) => log::debug!("audio socket buffers: SO_RCVBUF={rcv} SO_SNDBUF={snd}"),
      Err(e) => warn!("setting audio socket buffer sizes failed (continuing with OS default): {e}"),
    }
  }
}

/// Creates a non-blocking socket that receives the multicast group `media_addr` on the NIC `iface`.
///
/// * Windows: cannot bind to a multicast address, so binds `0.0.0.0:port` with `SO_REUSEADDR` (several
///   bundles share port 4321) and joins the group on `iface`.
/// * Others: binds to `media_addr` like upstream (only that group's traffic is delivered) and joins.
///
/// In both cases `IP_MULTICAST_IF` is set to `iface` on Windows and the audio receive buffer is
/// enlarged on Windows.
pub fn bind_multicast_receiver(
  media_addr: SocketAddrV4,
  iface: Ipv4Addr,
) -> io::Result<mio::net::UdpSocket> {
  let sock = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
  sock.set_reuse_address(true)?;
  let bind_addr =
    if cfg!(windows) { SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, media_addr.port()) } else { media_addr };
  sock.bind(&SocketAddr::V4(bind_addr).into())?;
  sock.join_multicast_v4(media_addr.ip(), &iface)?;
  apply_multicast_egress_platform(&sock, iface);
  apply_audio_buffers_platform(&sock, true, false);
  sock.set_nonblocking(true)?;
  Ok(mio::net::UdpSocket::from_std(sock.into()))
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::net::UdpSocket;
  use std::time::Duration;

  #[test]
  fn buffer_sizes_are_applied_and_reported() {
    let s = UdpSocket::bind("127.0.0.1:0").unwrap();
    let (rcv, snd) =
      set_buffer_sizes(SockRef::from(&s), Some(AUDIO_RCVBUF_BYTES), Some(AUDIO_SNDBUF_BYTES)).unwrap();
    // The OS may clamp (Linux rmem_max), but never below the 64 KiB-ish default; Windows reports
    // the requested value exactly.
    assert!(rcv >= 32 * 1024, "rcvbuf {rcv}");
    assert!(snd >= 32 * 1024, "sndbuf {snd}");
    if cfg!(windows) {
      assert_eq!(rcv, AUDIO_RCVBUF_BYTES);
      assert_eq!(snd, AUDIO_SNDBUF_BYTES);
    }
  }

  #[test]
  fn buffer_sizes_none_leaves_options_alone() {
    let s = UdpSocket::bind("127.0.0.1:0").unwrap();
    let before = SockRef::from(&s).recv_buffer_size().unwrap();
    let (rcv, _) = set_buffer_sizes(SockRef::from(&s), None, None).unwrap();
    assert_eq!(before, rcv);
  }

  #[test]
  fn multicast_if_roundtrips_and_platform_wrappers_accept_std_sockets() {
    let s = UdpSocket::bind("127.0.0.1:0").unwrap();
    set_multicast_egress(SockRef::from(&s), Ipv4Addr::LOCALHOST).unwrap();
    assert_eq!(SockRef::from(&s).multicast_if_v4().unwrap(), Ipv4Addr::LOCALHOST);
    apply_multicast_egress_platform(&s, Ipv4Addr::LOCALHOST);
    apply_multicast_egress_platform(&s, Ipv4Addr::UNSPECIFIED); // ignored
    apply_audio_buffers_platform(&s, true, true);
  }

  /// Two sockets exchange a multicast datagram through the loopback interface, the sender selecting
  /// 127.0.0.1 as egress and the receiver created by `bind_multicast_receiver`. Skipped (passes with
  /// a note on stderr) when the OS has no loopback multicast route, which is common in containers.
  #[test]
  fn loopback_multicast_via_selected_interface() {
    let group = Ipv4Addr::new(239, 255, 66, 7);
    let port = 40_000 + (std::process::id() % 2000) as u16;
    let rx = match bind_multicast_receiver(SocketAddrV4::new(group, port), Ipv4Addr::LOCALHOST) {
      Ok(s) => s,
      Err(e) => {
        eprintln!("SKIP: cannot join multicast on loopback: {e}");
        return;
      }
    };
    let tx = UdpSocket::bind("127.0.0.1:0").unwrap();
    if let Err(e) = set_multicast_egress(SockRef::from(&tx), Ipv4Addr::LOCALHOST) {
      eprintln!("SKIP: IP_MULTICAST_IF on loopback unsupported: {e}");
      return;
    }
    SockRef::from(&tx).set_multicast_loop_v4(true).unwrap();
    if let Err(e) = tx.send_to(b"ons-w6", (group, port)) {
      eprintln!("SKIP: cannot send multicast via loopback: {e}");
      return;
    }
    let mut buf = [0u8; 64];
    for _ in 0..50 {
      match rx.recv_from(&mut buf) {
        Ok((n, from)) => {
          assert_eq!(&buf[..n], b"ons-w6");
          assert_eq!(from.ip(), Ipv4Addr::LOCALHOST);
          return;
        }
        Err(e) if e.kind() == io::ErrorKind::WouldBlock => std::thread::sleep(Duration::from_millis(20)),
        Err(e) => panic!("recv: {e}"),
      }
    }
    eprintln!(
      "SKIP: no multicast datagram delivered over loopback within 1 s (no loopback multicast route?)"
    );
  }
}
