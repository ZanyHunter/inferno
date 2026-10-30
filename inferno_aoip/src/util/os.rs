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

#[cfg(not(target_family = "unix"))]
pub fn set_current_thread_realtime(_priority_hint: u8) -> Result<(), Error> {
  // ONS fork: minimal compile fix. MMCSS "Pro Audio" registration and time-critical priority are
  // tracked in ZanyHunter/open-network-soundcard#28.
  use thread_priority::ThreadPriority;
  thread_priority::set_current_thread_priority(ThreadPriority::Max)
}
