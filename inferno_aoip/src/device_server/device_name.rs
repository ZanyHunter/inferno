// SPDX-License-Identifier: GPL-3.0-or-later OR AGPL-3.0-or-later
//
// ONS fork addition (same dual licence as the rest of `inferno_aoip`).

//! Run-time device rename (ARC opcode `0x1001`), ONS fork.
//!
//! [`handle_request`] is the whole behaviour of the opcode: read and validate the name, announce it
//! (mDNS records, shared device name), persist it and tell the host. It talks to the outside only
//! through [`NameAnnouncer`], the state storage and the host callback, so the unit tests need no
//! network.
//!
//! * A name that breaks the Dante name rules ([`validate_device_name`]) is **refused**, not
//!   silently altered: the controller would otherwise show a name that differs from the one the
//!   user typed. Dante Controller restricts what its text field accepts, so this only matters for
//!   other clients.
//! * An empty name resets the device to its default name ([`DeviceNaming::default_name`]).
//! * Renaming to the current name succeeds and changes nothing.
//! * With [`DeviceNaming::persist`], the name is saved as `device_name.toml` in the device's state
//!   folder and wins over the name in the settings at the next start; a reset deletes the file.
//!   A host that keeps the name itself (ONS: `config.toml`) turns this off and uses
//!   [`DeviceNaming::on_rename`].

use std::sync::Arc;

use log::{info, warn};
use serde::{Deserialize, Serialize};

use super::mdns_server::DeviceMDNSResponder;
use super::settings::validate_device_name;
use crate::device_info::DeviceInfo;
use crate::protocol::proto_device::set_device_name::{parse_request, NameRequest, NameRequestError};
use crate::state_storage::StateStorage;

/// Name of the persisted item (`device_name.toml`).
pub const STATE_ITEM: &str = "device_name";

/// What changed, handed to [`DeviceNaming::on_rename`] after the new name is in effect.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeviceRename {
  /// The name before the change. The records advertised under it were removed; the host should send
  /// an mDNS goodbye for them so that controllers drop the old entry at once.
  pub old_name: String,
  /// The name now in effect.
  pub new_name: String,
  /// The controller asked for the default name (an empty name).
  pub reset: bool,
}

/// Called after a successful rename, on the ARC server task: keep it short.
#[derive(Clone)]
pub struct RenameCallback(Arc<dyn Fn(&DeviceRename) + Send + Sync>);

impl RenameCallback {
  pub fn new(f: impl Fn(&DeviceRename) + Send + Sync + 'static) -> Self {
    Self(Arc::new(f))
  }
  fn call(&self, rename: &DeviceRename) {
    (self.0)(rename)
  }
}

impl std::fmt::Debug for RenameCallback {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.write_str("RenameCallback")
  }
}

/// How the device is named and how a rename is handled.
#[derive(Clone, Debug)]
pub struct DeviceNaming {
  /// The name an empty rename request goes back to.
  pub default_name: String,
  /// Save the name in the state folder (see the module docs).
  pub persist: bool,
  /// Told about every rename.
  pub on_rename: Option<RenameCallback>,
}

/// Why a rename request was refused.
#[derive(Debug, PartialEq, Eq)]
pub enum RenameError {
  /// The request content is not a UTF-8 string.
  Unreadable,
  /// The name breaks the Dante name rules.
  Invalid(String),
}

impl std::fmt::Display for RenameError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::Unreadable => write!(f, "the name is not valid UTF-8"),
      Self::Invalid(reason) => write!(f, "{reason}"),
    }
  }
}

/// Puts a new name on the network. The real implementation is [`DeviceMDNSResponder`].
pub trait NameAnnouncer {
  /// Stop advertising under the current name, make `new_name` the device name and advertise under it.
  fn rename_device(&self, new_name: &str);
}

impl NameAnnouncer for DeviceMDNSResponder {
  fn rename_device(&self, new_name: &str) {
    DeviceMDNSResponder::rename_device(self, new_name)
  }
}

#[derive(Serialize, Deserialize)]
struct SavedName {
  name: String,
}

/// The name saved by an earlier rename, if there is a usable one.
pub fn load_saved_name(storage: &StateStorage) -> Option<String> {
  let saved: SavedName = storage.load(STATE_ITEM).ok()?;
  match validate_device_name(&saved.name) {
    Ok(()) => Some(saved.name),
    Err(e) => {
      warn!("ignoring the saved device name: {e}");
      None
    }
  }
}

/// Apply the saved name (if [`DeviceNaming::persist`] and there is one) before anything is advertised.
pub fn apply_saved_name(naming: &DeviceNaming, storage: &StateStorage, self_info: &DeviceInfo) {
  if !naming.persist {
    return;
  }
  if let Some(name) = load_saved_name(storage) {
    if name != self_info.friendly_hostname() {
      info!("using the device name saved by an earlier rename: {name}");
      self_info.set_friendly_hostname(&name);
    }
  }
}

fn persist(naming: &DeviceNaming, storage: &StateStorage, name: &str, reset: bool) {
  if !naming.persist {
    return;
  }
  let result = if reset { storage.remove(STATE_ITEM) } else { storage.save(STATE_ITEM, &SavedName { name: name.to_owned() }) };
  if let Err(e) = result {
    warn!("cannot save the device name: {e}");
  }
}

/// Handle the content of an ARC `0x1001` request. `Ok(None)` means the name is already in effect.
pub fn handle_request(
  content: &[u8],
  naming: &DeviceNaming,
  self_info: &DeviceInfo,
  storage: &StateStorage,
  announcer: &dyn NameAnnouncer,
) -> Result<Option<DeviceRename>, RenameError> {
  let (new_name, reset) = match parse_request(content) {
    Ok(NameRequest::Set(name)) => (name, false),
    Ok(NameRequest::Reset) => (naming.default_name.clone(), true),
    Err(NameRequestError::NotUtf8) => return Err(RenameError::Unreadable),
  };
  validate_device_name(&new_name).map_err(|e| RenameError::Invalid(e.to_string()))?;

  let old_name = self_info.friendly_hostname();
  if old_name == new_name {
    // Persist anyway: a reset to the default must forget an earlier saved name even if the two are equal.
    persist(naming, storage, &new_name, reset);
    return Ok(None);
  }
  announcer.rename_device(&new_name);
  persist(naming, storage, &new_name, reset);
  let rename = DeviceRename { old_name, new_name, reset };
  info!(
    "device renamed from {} to {}{}",
    rename.old_name,
    rename.new_name,
    if rename.reset { " (reset to the default name)" } else { "" }
  );
  if let Some(callback) = &naming.on_rename {
    callback.call(&rename);
  }
  Ok(Some(rename))
}

#[cfg(test)]
mod tests {
  use std::cell::RefCell;
  use std::sync::Mutex;

  use super::*;
  use crate::protocol::proto_arc::test_device_info;

  /// Stands in for the mDNS responder: does what it does to the shared name and records the call.
  struct FakeAnnouncer<'a> {
    info: &'a DeviceInfo,
    calls: RefCell<Vec<String>>,
  }

  impl NameAnnouncer for FakeAnnouncer<'_> {
    fn rename_device(&self, new_name: &str) {
      self.calls.borrow_mut().push(new_name.to_owned());
      self.info.set_friendly_hostname(new_name);
    }
  }

  fn temp_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("inferno_aoip_device_name_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
  }

  struct Fixture {
    info: DeviceInfo,
    storage: StateStorage,
    dir: std::path::PathBuf,
  }

  impl Fixture {
    fn new(tag: &str) -> Self {
      let dir = temp_dir(tag);
      let info = test_device_info();
      let storage = StateStorage::new(&info, &dir);
      Self { info, storage, dir }
    }
    fn naming(&self, persist: bool) -> DeviceNaming {
      DeviceNaming { default_name: "ONS-DEFAULT".into(), persist, on_rename: None }
    }
  }

  impl Drop for Fixture {
    fn drop(&mut self) {
      let _ = std::fs::remove_dir_all(&self.dir);
    }
  }

  fn run(fx: &Fixture, content: &[u8], naming: &DeviceNaming) -> (Result<Option<DeviceRename>, RenameError>, Vec<String>) {
    let announcer = FakeAnnouncer { info: &fx.info, calls: RefCell::new(vec![]) };
    let r = handle_request(content, naming, &fx.info, &fx.storage, &announcer);
    (r, announcer.calls.into_inner())
  }

  #[test]
  fn a_valid_name_is_announced_and_reported() {
    let fx = Fixture::new("valid");
    let (r, calls) = run(&fx, b"ONS-RENAME-TEST\0", &fx.naming(false));
    assert_eq!(
      r,
      Ok(Some(DeviceRename { old_name: "test".into(), new_name: "ONS-RENAME-TEST".into(), reset: false }))
    );
    assert_eq!(calls, vec!["ONS-RENAME-TEST"]);
    assert_eq!(fx.info.friendly_hostname(), "ONS-RENAME-TEST");
  }

  #[test]
  fn the_same_name_changes_nothing() {
    let fx = Fixture::new("same");
    let (r, calls) = run(&fx, b"test\0", &fx.naming(false));
    assert_eq!(r, Ok(None));
    assert!(calls.is_empty());
  }

  #[test]
  fn invalid_names_are_refused_and_nothing_changes() {
    let fx = Fixture::new("invalid");
    for bad in [&b"has space\0"[..], b"-lead\0", b"trail-\0", b"under_score\0", &b"x".repeat(32), "wörld\0".as_bytes()] {
      let (r, calls) = run(&fx, bad, &fx.naming(true));
      assert!(matches!(r, Err(RenameError::Invalid(_))), "{bad:?}");
      assert!(calls.is_empty());
      assert_eq!(fx.info.friendly_hostname(), "test");
    }
    assert!(load_saved_name(&fx.storage).is_none());
    // A name of exactly 31 characters is fine.
    let (r, _) = run(&fx, &[b"x".repeat(31), vec![0]].concat(), &fx.naming(false));
    assert!(matches!(r, Ok(Some(_))));
  }

  #[test]
  fn a_name_that_is_not_utf8_is_unreadable() {
    let fx = Fixture::new("utf8");
    let (r, calls) = run(&fx, &[0xff, 0xfe, 0], &fx.naming(false));
    assert_eq!(r, Err(RenameError::Unreadable));
    assert!(calls.is_empty());
  }

  #[test]
  fn an_empty_name_resets_to_the_default() {
    let fx = Fixture::new("reset");
    let naming = fx.naming(true);
    run(&fx, b"CUSTOM\0", &naming);
    assert_eq!(load_saved_name(&fx.storage).as_deref(), Some("CUSTOM"));

    let (r, calls) = run(&fx, &[], &naming);
    assert_eq!(
      r,
      Ok(Some(DeviceRename { old_name: "CUSTOM".into(), new_name: "ONS-DEFAULT".into(), reset: true }))
    );
    assert_eq!(calls, vec!["ONS-DEFAULT"]);
    assert_eq!(load_saved_name(&fx.storage), None, "a reset forgets the saved name");
  }

  #[test]
  fn the_name_survives_a_restart_when_persisting() {
    let fx = Fixture::new("persist");
    run(&fx, b"KEPT-NAME\0", &fx.naming(true));

    // A new start: the settings carry the old name, the saved one wins.
    let restarted = test_device_info();
    apply_saved_name(&fx.naming(true), &fx.storage, &restarted);
    assert_eq!(restarted.friendly_hostname(), "KEPT-NAME");
  }

  #[test]
  fn nothing_is_saved_or_applied_without_persist() {
    let fx = Fixture::new("nopersist");
    run(&fx, b"HOST-KEEPS-IT\0", &fx.naming(false));
    assert!(load_saved_name(&fx.storage).is_none());

    let _ = fx.storage.save(STATE_ITEM, &SavedName { name: "OLD".into() });
    let info = test_device_info();
    apply_saved_name(&fx.naming(false), &fx.storage, &info);
    assert_eq!(info.friendly_hostname(), "test");
  }

  #[test]
  fn a_corrupt_saved_name_is_ignored() {
    let fx = Fixture::new("corrupt");
    let _ = fx.storage.save(STATE_ITEM, &SavedName { name: "not valid!".into() });
    let info = test_device_info();
    apply_saved_name(&fx.naming(true), &fx.storage, &info);
    assert_eq!(info.friendly_hostname(), "test");
  }

  #[test]
  fn the_host_is_called_back_after_the_change() {
    let fx = Fixture::new("callback");
    let seen: Arc<Mutex<Vec<DeviceRename>>> = Default::default();
    let sink = seen.clone();
    let info_for_cb = fx.info.clone();
    let naming = DeviceNaming {
      default_name: "ONS-DEFAULT".into(),
      persist: false,
      on_rename: Some(RenameCallback::new(move |r| {
        // By the time the host hears of it the new name is already in effect.
        assert_eq!(info_for_cb.friendly_hostname(), r.new_name);
        sink.lock().unwrap().push(r.clone());
      })),
    };
    run(&fx, b"FIRST\0", &naming);
    run(&fx, b"FIRST\0", &naming); // unchanged: no second call
    run(&fx, &[], &naming);
    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 2);
    assert_eq!((seen[0].old_name.as_str(), seen[0].new_name.as_str(), seen[0].reset), ("test", "FIRST", false));
    assert_eq!((seen[1].old_name.as_str(), seen[1].new_name.as_str(), seen[1].reset), ("FIRST", "ONS-DEFAULT", true));
  }
}
