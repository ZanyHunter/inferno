use thread_priority::Error;
#[cfg(target_family = "unix")]
use thread_priority::thread_native_id;

#[cfg(target_family = "unix")]
pub fn set_current_thread_realtime(priority_hint: u8) -> Result<(), Error> {
  use thread_priority::unix::set_thread_priority_and_policy;
  use thread_priority::{
    RealtimeThreadSchedulePolicy, ThreadPriority, ThreadPriorityValue, ThreadSchedulePolicy,
  };

  set_thread_priority_and_policy(
    thread_native_id(),
    ThreadPriority::Crossplatform(ThreadPriorityValue::try_from(priority_hint).unwrap()),
    ThreadSchedulePolicy::Realtime(RealtimeThreadSchedulePolicy::Fifo),
  )
}

/// What the Windows thread setup was granted.
#[cfg(not(target_family = "unix"))]
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RealtimeGrant {
  /// The thread is registered with MMCSS under the "Pro Audio" task (kept until the thread ends).
  pub mmcss: bool,
  /// Without MMCSS: the thread priority the fallback reached (`TIME_CRITICAL` or `HIGHEST`).
  pub thread_priority: Option<&'static str>,
}

/// Registers the current thread with MMCSS ("Pro Audio") and raises its priority; without MMCSS (service
/// missing, no privilege) falls back to `SetThreadPriority`. Never panics and says what it got.
/// ONS fork: ZanyHunter/open-network-soundcard#145.
#[cfg(not(target_family = "unix"))]
pub fn enter_realtime() -> RealtimeGrant {
  use windows_sys::Win32::System::Threading::{
    AvSetMmThreadCharacteristicsW, AvSetMmThreadPriority, GetCurrentThread, SetThreadPriority,
    THREAD_PRIORITY_HIGHEST, THREAD_PRIORITY_TIME_CRITICAL,
  };
  /// `AVRT_PRIORITY_HIGH`.
  const AVRT_PRIORITY_HIGH: i32 = 1;

  let mut grant = RealtimeGrant::default();
  let task: Vec<u16> = "Pro Audio".encode_utf16().chain(Some(0)).collect();
  let mut index = 0u32;
  // SAFETY: `task` is NUL terminated and `index` is a valid out pointer.
  let handle = unsafe { AvSetMmThreadCharacteristicsW(task.as_ptr(), &mut index) };
  if !handle.is_null() {
    grant.mmcss = true;
    // SAFETY: `handle` is the live registration just returned. It is released when the thread ends.
    unsafe { AvSetMmThreadPriority(handle, AVRT_PRIORITY_HIGH) };
    return grant;
  }
  // SAFETY: the pseudo handle of the current thread is always valid.
  let thread = unsafe { GetCurrentThread() };
  for (level, name) in [
    (THREAD_PRIORITY_TIME_CRITICAL, "TIME_CRITICAL"),
    (THREAD_PRIORITY_HIGHEST, "HIGHEST"),
  ] {
    // SAFETY: as above.
    if unsafe { SetThreadPriority(thread, level) } != 0 {
      grant.thread_priority = Some(name);
      break;
    }
  }
  grant
}

#[cfg(not(target_family = "unix"))]
pub fn set_current_thread_realtime(_priority_hint: u8) -> Result<(), Error> {
  // ONS fork: MMCSS "Pro Audio" (or a high thread priority without it) instead of upstream's Unix SCHED_FIFO.
  let grant = enter_realtime();
  if grant.mmcss {
    log::debug!("realtime thread: MMCSS Pro Audio");
  } else {
    log::warn!("realtime thread: no MMCSS, thread priority {:?}", grant.thread_priority);
  }
  Ok(())
}

#[cfg(all(test, not(target_family = "unix")))]
mod tests {
  use super::*;

  #[test]
  fn enter_realtime_reports_a_grant_and_does_not_panic() {
    // On its own thread so the priority change does not leak into the test runner.
    let grant = std::thread::spawn(enter_realtime).join().expect("no panic");
    // Either MMCSS or a priority fallback, or nothing (no privilege): never both.
    assert!(!(grant.mmcss && grant.thread_priority.is_some()));
  }

  #[test]
  fn set_current_thread_realtime_returns_ok() {
    std::thread::spawn(|| set_current_thread_realtime(81)).join().unwrap().unwrap();
  }
}
