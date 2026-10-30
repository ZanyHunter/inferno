//! Facade over `usrvclock` plus the in-process clock source. See `Cargo.toml` and FORK.md.
//!
//! * [`ClockOverlay`]: on Unix a transparent re-export of the upstream crate (no behaviour change; the
//!   underlying clock is chosen by `clock_id`). Elsewhere, `overlay_nonunix`: same fields and math, with
//!   QueryPerformanceCounter (nanoseconds, absolute) as the underlying clock.
//! * [`ClockPublisher`]: the in-process clock source, on every platform. The host publishes
//!   [`ClockOverlay`]s; Inferno's consumers read them. This replaces the usrvclock socket transport
//!   on Windows and lets Unix hosts skip the socket too.

#[cfg(unix)]
pub use upstream_usrvclock::*;

#[cfg(not(unix))]
mod overlay_nonunix;
#[cfg(not(unix))]
pub use overlay_nonunix::*;

mod publisher;
pub use publisher::ClockPublisher;
