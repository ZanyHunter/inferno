//! TEMPORARY Windows shim standing in for the `usrvclock` crate.
//!
//! The real `usrvclock` (a submodule hosted on GitLab by lumifaza, MIT OR Apache-2.0) is built on Unix
//! datagram sockets and `nix`, so it cannot compile for Windows. This shim exists ONLY so that
//! `inferno_aoip` compiles on `x86_64-pc-windows-msvc` (issue W3). It is selected by `cfg(not(unix))`
//! in the `ons-usrvclock` facade crate and is replaced by a real in-process clock source in W2
//! (ZanyHunter/open-network-soundcard#88).
//!
//! What it provides, exactly the API surface `inferno_aoip` uses:
//! * [`ClockOverlay`] with the same math as upstream. The underlying clock is `std::time::Instant`
//!   (QueryPerformanceCounter on Windows) measured from the first call, in nanoseconds.
//! * [`AsyncClient`], a bare `tokio::sync::watch` channel with no transport. `start` ignores the path.
//!   [`AsyncClient::publish`] lets an in-process owner push overlays (the future `ons-ptp` hook).
//! * [`SafeClock`], an empty placeholder (inferno_aoip only imports it).
//!
//! It does NOT provide `Server`, `BlockingClient`, PHC devices or the clock-id mapping. `clock_id` is
//! carried but ignored. Do not build features on this crate.
//!
//! Overlay math follows usrvclock, (c) 2024 Teodor Wozniak, MIT OR Apache-2.0.

use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::Instant;

/// Placeholder path constant, kept for API compatibility. Unused on Windows.
pub const DEFAULT_SERVER_SOCKET_PATH: &str = "ons-usrvclock-unused";

/// Clock overlay: `overlay = t + shift + (t - last_sync) * freq_scale`, all in nanoseconds.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ClockOverlay {
    pub clock_id: i64,
    pub last_sync: i64,
    pub shift: i64,
    pub freq_scale: f64,
}

fn epoch() -> Instant {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    *EPOCH.get_or_init(Instant::now)
}

impl ClockOverlay {
    /// Converts an underlying-clock timestamp to the overlay timescale. May wrap.
    pub fn underlying_to_overlay_ns(&self, timestamp: i64) -> i64 {
        let elapsed = timestamp.wrapping_sub(self.last_sync);
        let correction = ((elapsed as f64) * self.freq_scale).round() as i64;
        timestamp.wrapping_add(self.shift).wrapping_add(correction)
    }

    /// Underlying clock now, in ns since the first call in this process (Instant, QPC-backed).
    pub fn now_underlying_ns(&self) -> i64 {
        epoch().elapsed().as_nanos() as i64
    }

    /// Overlay clock now, in ns. May wrap.
    pub fn now_ns(&self) -> i64 {
        self.underlying_to_overlay_ns(self.now_underlying_ns())
    }

    /// No hardware clock to add on Windows: equals `freq_scale`.
    pub fn freq_scale_including_hw(&self) -> f64 {
        self.freq_scale
    }
}

/// Empty placeholder for upstream's `SafeClock` (only imported by `inferno_aoip`).
pub struct SafeClock;

/// Error type kept for signature compatibility with the upstream error callback. Never produced.
#[derive(Debug)]
pub enum OverlayReceiveError {}

/// In-process stand-in for upstream's `AsyncClient`: a watch channel with no transport.
pub struct AsyncClient {
    sender: tokio::sync::watch::Sender<Option<ClockOverlay>>,
}

impl AsyncClient {
    /// Same signature as upstream. `path` and `error_handler` are ignored; no overlay will ever
    /// arrive unless the owner calls [`AsyncClient::publish`].
    pub fn start(
        _path: PathBuf,
        _error_handler: Box<dyn FnMut(OverlayReceiveError) + Send>,
    ) -> Self {
        // Initialise the Instant epoch so "now" starts near process start.
        let _ = epoch();
        Self { sender: tokio::sync::watch::Sender::new(None) }
    }

    /// Pushes a new overlay to all subscribers.
    pub fn publish(&self, overlay: ClockOverlay) {
        self.sender.send_replace(Some(overlay));
    }

    /// Stops the client (no-op, kept for API compatibility).
    pub async fn stop(self) -> Result<(), std::convert::Infallible> {
        Ok(())
    }

    /// Subscribes to overlay changes.
    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<Option<ClockOverlay>> {
        self.sender.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overlay_math() {
        let o = ClockOverlay { clock_id: 0, last_sync: 1000, shift: 50, freq_scale: 0.001 };
        assert_eq!(o.underlying_to_overlay_ns(2000), 2000 + 50 + 1);
        assert_eq!(o.freq_scale_including_hw(), 0.001);
    }

    #[test]
    fn publish_reaches_subscriber() {
        let c = AsyncClient::start("x".into(), Box::new(|_| {}));
        let rx = c.subscribe();
        assert!(rx.borrow().is_none());
        let o = ClockOverlay { clock_id: 1, last_sync: 0, shift: 0, freq_scale: 0.0 };
        c.publish(o);
        assert_eq!(*rx.borrow(), Some(o));
    }
}
