//! Facade over `usrvclock`. See `Cargo.toml` and FORK.md.
//!
//! * Unix: transparent re-export of the upstream crate (no behaviour change).
//! * Everything else (Windows): [`windows_shim`], a TEMPORARY stand-in. Replaced in W2.

#[cfg(unix)]
pub use upstream_usrvclock::*;

#[cfg(not(unix))]
mod windows_shim;
#[cfg(not(unix))]
pub use windows_shim::*;
