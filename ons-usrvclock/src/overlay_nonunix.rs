//! `ClockOverlay` for targets without the upstream `usrvclock` (Windows).
//!
//! Same fields and math as upstream `usrvclock::ClockOverlay` (Copyright 2024 Teodor Wozniak,
//! MIT OR Apache-2.0), minus the socket transport. The underlying clock is QueryPerformanceCounter in
//! nanoseconds, absolute (not relative to process start), so a host that converts its own QPC ticks with
//! `ticks * 1e9 / freq` (floor) produces timestamps on exactly this scale. `clock_id` is carried and
//! ignored.

/// Clock overlay: `overlay = t + shift + (t - last_sync) * freq_scale`, all in nanoseconds.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ClockOverlay {
    pub clock_id: i64,
    pub last_sync: i64,
    pub shift: i64,
    pub freq_scale: f64,
}

impl ClockOverlay {
    /// Converts an underlying-clock timestamp to the overlay timescale. May wrap.
    pub fn underlying_to_overlay_ns(&self, timestamp: i64) -> i64 {
        let elapsed = timestamp.wrapping_sub(self.last_sync);
        let correction = ((elapsed as f64) * self.freq_scale).round() as i64;
        timestamp.wrapping_add(self.shift).wrapping_add(correction)
    }

    /// Underlying clock now, in ns (QueryPerformanceCounter on Windows).
    pub fn now_underlying_ns(&self) -> i64 {
        qpc_now_ns()
    }

    /// Overlay clock now, in ns. May wrap.
    pub fn now_ns(&self) -> i64 {
        self.underlying_to_overlay_ns(self.now_underlying_ns())
    }
}

/// QueryPerformanceCounter, converted to ns with the same formula ons-ptp uses
/// (`ticks * 1e9 / freq`, floor, in 128 bit).
#[cfg(windows)]
pub fn qpc_now_ns() -> i64 {
    use std::sync::OnceLock;
    use windows_sys::Win32::System::Performance::{QueryPerformanceCounter, QueryPerformanceFrequency};

    static FREQ: OnceLock<u64> = OnceLock::new();
    let freq = *FREQ.get_or_init(|| {
        let mut f = 0i64;
        // SAFETY: valid pointer to a local; the call cannot fail on Windows XP and later.
        unsafe { QueryPerformanceFrequency(&mut f) };
        (f as u64).max(1)
    });
    let mut ticks = 0i64;
    // SAFETY: valid pointer to a local.
    unsafe { QueryPerformanceCounter(&mut ticks) };
    ((ticks as u128) * 1_000_000_000u128 / (freq as u128)) as i64
}

/// Fallback for exotic non-Unix, non-Windows targets: `Instant` since first use.
#[cfg(not(windows))]
pub fn qpc_now_ns() -> i64 {
    use std::sync::OnceLock;
    use std::time::Instant;
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    EPOCH.get_or_init(Instant::now).elapsed().as_nanos() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overlay_math() {
        let o = ClockOverlay { clock_id: 0, last_sync: 1000, shift: 50, freq_scale: 0.001 };
        assert_eq!(o.underlying_to_overlay_ns(2000), 2000 + 50 + 1);
        assert_eq!(o.underlying_to_overlay_ns(0), 50 - 1);
    }

    #[test]
    fn underlying_is_monotonic_and_advances() {
        let o = ClockOverlay { clock_id: 0, last_sync: 0, shift: 0, freq_scale: 0.0 };
        let a = o.now_underlying_ns();
        std::thread::sleep(std::time::Duration::from_millis(20));
        let b = o.now_underlying_ns();
        assert!(b - a >= 15_000_000, "{a} -> {b}");
        assert!(b - a < 2_000_000_000, "{a} -> {b}");
    }

    #[cfg(windows)]
    #[test]
    fn underlying_is_absolute_qpc() {
        // Absolute QPC counts from boot, so it is far beyond the few ms a process-relative epoch gives.
        assert!(qpc_now_ns() > 1_000_000_000, "QPC should be seconds since boot at least");
    }
}
