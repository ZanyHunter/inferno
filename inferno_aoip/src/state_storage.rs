use std::{
  error::Error,
  fs::{create_dir_all, File},
  io::{Read, Write},
  path::{Path, PathBuf},
};

use crate::{common::*, device_info::DeviceId, device_info::DeviceInfo};
use serde::{Deserialize, Serialize};
use toml;

const PATH_SUFFIX: &str = ".toml";

/// Per-device persistent state: one TOML file per item (`rx_channels.toml`, ...) in
/// `<state_dir>/<device id hex>/`. The format stays TOML (upstream's choice); see FORK.md.
pub struct StateStorage {
  dir: PathBuf,
}

impl StateStorage {
  /// `state_dir` comes from `Settings::state_dir`.
  pub fn new(self_info: &DeviceInfo, state_dir: &Path) -> Self {
    Self::in_dir(state_dir, &self_info.factory_device_id)
  }
  /// A failure to create the folder is logged and not fatal: loads then find nothing and saves
  /// return errors that the callers log.
  fn in_dir(state_dir: &Path, device_id: &DeviceId) -> Self {
    let dir = state_dir.join(hex::encode(device_id));
    create_dir_all(&dir).log_and_forget();
    info!("using state directory: {}", dir.display());
    Self { dir }
  }
  fn full_path(&self, name: &str) -> PathBuf {
    self.dir.join(format!("{name}{PATH_SUFFIX}"))
  }
  pub fn save(&self, name: &str, value: &impl Serialize) -> Result<(), Box<dyn Error>> {
    let content = toml::to_string(&value)?;
    let tmp_path = self.full_path(&format!("tmp.{name}"));
    let mut file = File::create(&tmp_path)?;
    file.write_all(content.as_bytes())?;
    drop(file);
    std::fs::rename(tmp_path, self.full_path(name))?;
    Ok(())
  }
  pub fn load<T: for<'a> Deserialize<'a>>(&self, name: &str) -> Result<T, Box<dyn Error>> {
    let mut file = File::open(self.full_path(name))?;
    let mut content: String = "".to_owned();
    file.read_to_string(&mut content)?;
    Ok(toml::from_str(&content)?)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::sync::atomic::{AtomicUsize, Ordering};
  use std::time::{SystemTime, UNIX_EPOCH};

  fn temp_state_dir() -> PathBuf {
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("inferno_aoip_test_{ts}_{n}"))
  }

  fn temp_state_storage() -> StateStorage {
    let dir = temp_state_dir();
    std::fs::create_dir_all(&dir).unwrap();
    StateStorage { dir }
  }

  #[derive(Serialize, Deserialize, Debug, PartialEq)]
  struct TestConfig {
    name: String,
    count: u32,
  }

  #[test]
  fn save_and_load_roundtrip() {
    let storage = temp_state_storage();
    let config = TestConfig { name: "test".to_string(), count: 42 };
    storage.save("config", &config).unwrap();
    let loaded: TestConfig = storage.load("config").unwrap();
    assert_eq!(config, loaded);
  }

  #[test]
  fn load_missing_file_fails() {
    let storage = temp_state_storage();
    let result: Result<TestConfig, Box<dyn Error>> = storage.load("nonexistent");
    assert!(result.is_err());
  }

  #[test]
  fn save_overwrites_existing() {
    let storage = temp_state_storage();
    let config1 = TestConfig { name: "first".to_string(), count: 1 };
    let config2 = TestConfig { name: "second".to_string(), count: 2 };
    storage.save("config", &config1).unwrap();
    storage.save("config", &config2).unwrap();
    let loaded: TestConfig = storage.load("config").unwrap();
    assert_eq!(loaded.name, "second");
    assert_eq!(loaded.count, 2);
  }

  #[test]
  fn save_uses_atomic_rename() {
    let storage = temp_state_storage();
    let config = TestConfig { name: "atomic".to_string(), count: 99 };
    storage.save("atomic", &config).unwrap();
    // tmp file should not exist after rename
    assert!(!storage.full_path("tmp.atomic").exists());
    // final file should exist
    assert!(storage.full_path("atomic").exists());
  }

  #[test]
  fn load_malformed_toml_fails() {
    let storage = temp_state_storage();
    std::fs::write(storage.full_path("bad"), "not valid toml [[[").unwrap();
    let result: Result<TestConfig, Box<dyn Error>> = storage.load("bad");
    assert!(result.is_err());
  }

  #[test]
  fn state_goes_under_the_injected_dir_in_a_device_id_folder() {
    let root = temp_state_dir();
    let storage = StateStorage::in_dir(&root, &[0x00, 0x1d, 0xc1, 0xaa, 0xbb, 0xcc, 0, 0]);
    assert_eq!(storage.dir, root.join("001dc1aabbcc0000"));
    assert!(storage.dir.is_dir());
    let config = TestConfig { name: "x".into(), count: 1 };
    storage.save("rx_channels", &config).unwrap();
    assert!(root.join("001dc1aabbcc0000").join("rx_channels.toml").is_file());
    std::fs::remove_dir_all(&root).ok();
  }
}
