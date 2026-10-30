//! The in-process clock source.

use std::sync::Arc;

use tokio::sync::watch;

use crate::ClockOverlay;

/// Handle through which the host application publishes clock updates to Inferno.
///
/// Clone it freely: all clones share one channel, so any number of `DeviceServer`s (and the host's own
/// clock thread) can use the same clock. [`ClockPublisher::publish`] never blocks and may be called from
/// any thread, with or without a tokio runtime (an ordinary servo thread is fine).
///
/// Consumers see the latest published overlay only (a `watch` channel), which is what the media clock
/// needs. Until the first [`publish`](Self::publish), consumers report "clock not ready".
///
/// Contract for the published overlay (identical to the usrvclock one):
/// `overlay_ns(t) = t + shift + round((t - last_sync) * freq_scale)` where `t` is the underlying clock in
/// ns and `last_sync` is on the same scale. On Windows the underlying clock is
/// QueryPerformanceCounter converted to ns (`ticks * 1e9 / freq`, floor, absolute, not relative to process
/// start), so `last_sync` must be a QPC ns value; `clock_id` is ignored there. On Unix the underlying clock
/// is the POSIX clock named by `clock_id`.
#[derive(Clone)]
pub struct ClockPublisher {
    tx: Arc<watch::Sender<Option<ClockOverlay>>>,
}

impl ClockPublisher {
    /// A publisher with no overlay yet.
    pub fn new() -> Self {
        Self { tx: Arc::new(watch::Sender::new(None)) }
    }

    /// Publishes a new overlay to all consumers. Works with no consumers attached.
    pub fn publish(&self, overlay: ClockOverlay) {
        self.tx.send_replace(Some(overlay));
    }

    /// The latest published overlay, if any.
    pub fn current(&self) -> Option<ClockOverlay> {
        *self.tx.borrow()
    }

    /// Subscribes to overlay changes. Same type usrvclock's `AsyncClient::subscribe` returns, so existing
    /// Inferno consumers need no change.
    pub fn subscribe(&self) -> watch::Receiver<Option<ClockOverlay>> {
        self.tx.subscribe()
    }
}

impl Default for ClockPublisher {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for ClockPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClockPublisher").field("current", &self.current()).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ovl(last_sync: i64) -> ClockOverlay {
        ClockOverlay { clock_id: 1, last_sync, shift: 5, freq_scale: 0.0 }
    }

    #[test]
    fn nothing_until_published() {
        let p = ClockPublisher::new();
        assert!(p.current().is_none());
        assert!(p.subscribe().borrow().is_none());
    }

    #[test]
    fn publish_reaches_current_and_subscribers_and_clones() {
        let p = ClockPublisher::new();
        let rx = p.subscribe();
        let p2 = p.clone();
        p2.publish(ovl(10));
        assert_eq!(p.current(), Some(ovl(10)));
        assert_eq!(*rx.borrow(), Some(ovl(10)));
        // A late subscriber sees the latest value immediately.
        assert_eq!(*p.subscribe().borrow(), Some(ovl(10)));
        p.publish(ovl(20));
        assert_eq!(p2.current(), Some(ovl(20)));
    }

    #[tokio::test]
    async fn changed_wakes_on_publish_from_another_thread() {
        let p = ClockPublisher::new();
        let mut rx = p.subscribe();
        let p2 = p.clone();
        std::thread::spawn(move || p2.publish(ovl(7)));
        tokio::time::timeout(std::time::Duration::from_secs(5), rx.changed()).await.unwrap().unwrap();
        assert_eq!(*rx.borrow_and_update(), Some(ovl(7)));
    }
}
