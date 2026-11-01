# FORK.md: ZanyHunter/inferno

This is a fork of [teodly/inferno](https://github.com/teodly/inferno) (canonical home:
[gitlab.com/lumifaza/inferno](https://gitlab.com/lumifaza/inferno)) maintained for the
[Open Network Soundcard](https://github.com/ZanyHunter/open-network-soundcard) (ONS) project. ONS consumes
`inferno_aoip` from here as a pinned git dependency and never copies its sources.

ONS is not affiliated with Audinate. This fork contains no Audinate code, SDKs or binaries.

## Licence and credits

* `inferno_aoip` is **GPL-3.0-or-later OR AGPL-3.0-or-later** (upstream's dual licence). ONS and this fork
  use it under **GPL-3.0-or-later**. Do not remove the AGPL or GPL files, and do not remove or alter
  upstream copyright notices, author fields or file headers.
* All credit for the protocol work goes to Teo (teodly) and the upstream contributors. Keep the upstream
  README, `LICENSE*` files and `authors` fields as they are. New files that are ours state their licence in
  the header or manifest.
* The submodules keep their own licences: `searchfire` and `usrvclock-rs` are MIT OR Apache-2.0.
* New code in `ons-usrvclock/` is MIT OR Apache-2.0 (it is the in-process clock source, a compatible
  licence for a library GPL code links against).

## Branches

| Branch | Purpose |
|---|---|
| `dev` | The fork's default branch. Upstream `dev` plus ONS patches, which arrive only through reviewed PRs. |
| `windows-support` | The branch for the Windows port work (W1 to W3 and later Windows patches). PRs into `dev` come from here or from short topic branches. |
| `master`, `stable`, other upstream branches | Untouched mirrors of upstream. |

ONS pins an exact commit (`rev = "<sha>"`) that is reachable from `dev`, never a branch name, so a moved
branch can never change an ONS build.

## How to update from upstream

Upstream is merged in, not rebased onto, so pinned commits stay reachable.

```sh
git remote add upstream https://github.com/teodly/inferno   # once
git fetch upstream
git checkout -b sync-upstream-YYYY-MM-DD dev
git merge upstream/dev    # resolve conflicts in the ONS-patched files listed below
git submodule update --init --recursive
cargo build -p inferno_aoip -p inferno2pipe --target x86_64-pc-windows-msvc
cargo test -p inferno_aoip -p inferno2pipe -p ons-usrvclock
# open a PR to dev; after it merges, bump the pinned rev in ONS in its own PR
```

If upstream moves a submodule pointer, review the new commits before accepting. Never force-push `dev`
or `windows-support` once ONS has pinned a commit on it. Always pass `--repo ZanyHunter/inferno` to `gh`:
in a fork it can otherwise target upstream. Do not open PRs or issues on teodly/inferno from this
automation. Patches that are generic (not Windows or ONS specific) are sent upstream by a human, kept small
and separate so they can be cherry-picked.

## Submodules

`searchfire`, `usrvclock-rs` and `alsa-sys-all` are Git submodules hosted on GitLab
(gitlab.com/lumifaza). The fork **pins the upstream commits** recorded in the tree (the gitlink), so a given
fork commit always builds the same sources. They are not mirrored. CI and local clones use
`git clone --recurse-submodules` (or `git submodule update --init --recursive`).

Risk: if the GitLab repositories disappear or are rewritten, the pinned commits become unreachable.
If that becomes a concern, mirror them under `ZanyHunter/` and repoint `.gitmodules` (not done; needs the
owner's approval).

`alsa-sys-all` is only needed by `alsa_pcm_inferno` (Linux). On Windows `cargo` never builds it, but the
submodule must still be checked out because Cargo reads every path dependency's manifest.

## What is patched

| Area | Patch | Tracking |
|---|---|---|
| Workspace | `default-members` leaves out `alsa_pcm_inferno` (Linux/libasound only) and `searchfire`. `cargo build -p alsa_pcm_inferno` still works on Linux. | ONS #87 |
| CI | `.github/workflows/ci.yml`: Windows (`windows-latest`) build and test, plus a Linux regression job. | ONS #87 |
| `inferno_aoip/src/util/os.rs` | Fix the never-compiled `cfg(not(unix))` branch: missing `ThreadPriority` import, `u32` vs `u8`. Behaviour is still only `ThreadPriority::Max`. MMCSS and time-critical priority are ONS #28. | ONS #94 (W3) |
| `inferno_aoip/src/device_server/info_mcast_server.rs` | `freq_scale_including_hw()` is Linux only (`clock_adjtime`); other targets use `freq_scale`. | ONS #94 (W3) |
| `inferno_aoip/src/media_clock.rs`, `device_server/{info_mcast_server,settings,mod}.rs` | **Clock stats from the host.** New `ClockStatusHandle` / `HostClockStatus { master_clock_id: Option<[u8;8]>, locked: bool }` (exported from `device_server`), passed with `SettingsBuilder::clock_status` / `Settings::clock_status`. `send_clock_stats` (reply to the 0x0021 request) uses it instead of the Linux-only `/tmp/clock-stats.<mac>0000*` file: no leader sends nothing, otherwise the reply carries the leader id. Flags as observed from a Mac DVS on 2026-11-01: following a master and synced `00 03 00 03` with the real frequency offset (upstream's constant, so the upstream comment `0x01 = PLL not locked` was wrong); following but not yet locked `00 03 00 01` with a frequency offset of 0; a device with no leader reports itself as master with `00 01 00 06` (own MAC + 0000), which we do not send (we send nothing). Without a handle the `/tmp` file is still read, with the historic flags word. The file parser no longer panics on a short file. Same message layout as before. | ONS #136 |
| `device_info.rs`, `protocol/proto_device.rs` (new), `device_server/{device_name,arc_server,mdns_server,info_mcast_server,settings,mod}.rs`, `state_storage.rs` | **Device rename and settings answers.** ARC `0x1001` (the request `netaudio device name` sends: `<name>\0`, empty = reset to default) renames the device at run time: the name is validated (Dante name rules, refused with result code `0xFFFF` if invalid), `DeviceInfo::friendly_hostname` became a shared `Arc<RwLock<String>>` (read it with `friendly_hostname()`), `DeviceMDNSResponder::rename_device` removes the arc/cmc/chan/bund records under the old name and announces them under the new one (it now tracks which channels and bundles are advertised), the name is saved as `device_name.toml` (`persist_device_name`, default on; a host that keeps the name itself turns it off) and `on_device_rename` calls the host with a `DeviceRename { old_name, new_name, reset }` (ONS sends the mDNS goodbye for the old name and writes `config.toml`). ARC `0x1100` (read settings) now returns sample rate and default/configured/active/min/max latency in the layout of a hardware reply (`0x8020`, `0x8204`, `0x8205`, `0x8301`, `0x8302`, `0x8306`; other requested ids are reported unavailable) instead of zeros; `0x1102` returns the property directory (all read-only); `0x1101` (change settings) is refused with `0xFFFF` and one log line. Conmon `0x0081`/`0x0083` (sample rate and encoding status) are answered (fixed 48 kHz / 24 bit). `DeviceCapabilities` (`SettingsBuilder::latency_range`, `board_flags`) carries the latency range and the feature flags of the board-info reply (default unchanged). Formats were checked against the parser of the `netaudio` package; items not confirmed on hardware are marked **(verify)** in `protocol/proto_device.rs`. | ONS #96, #19, #20 |
| `inferno_aoip/tests/loopback_trx.rs` | Uses an in-process `ClockPublisher` instead of a `usrvclock::Server`, so it runs on Windows and Unix alike (no `cfg(unix)` gate). | ONS #88 (W2) |
| `inferno_aoip/src/media_clock.rs`, `device_server/mod.rs` | `ClockReceiver` is now an inferno_aoip type with two sources: an in-process `ClockPublisher` (all platforms, preferred) or the usrvclock socket (Unix only, upstream behaviour). New `DeviceServer::start_with_clock(settings, Option<ClockPublisher>)`; `DeviceServer::start` is unchanged for callers (Unix socket via `CLOCK_PATH`). `ClockOverlay` and `ClockPublisher` are exported from `device_server`. On a non-Unix target without a publisher the clock never becomes ready (logged as an error). | ONS #88 (W2) |
| `ons-usrvclock/` | Facade crate that `inferno_aoip` depends on (as `usrvclock`). `ClockOverlay`: on Unix the real upstream `usrvclock` type re-exported unchanged; on Windows a same-shaped type (same math) whose underlying clock is QueryPerformanceCounter in absolute ns (`ticks * 1e9 / freq`, floor), so a host converting its own QPC ticks lands on the same scale. `ClockPublisher` (all platforms): the in-process clock source, a cloneable handle on a `tokio::sync::watch` channel. The host calls `publish(ClockOverlay)` from any thread and Inferno's consumers (media clock, flows, info_mcast_server) read it. There is no `usrvclock` dependency on Windows. The crate name is historical. ONS feeds it from the `ons-ptp` MediaClock (`ons-dante`, feature `inferno`). | ONS #88 (W2) |
| `inferno_aoip/src/device_server/settings.rs` | **Typed `SettingsBuilder`** (`Settings::builder`) returning `Result<_, SettingsError>`: NIC chosen by `NicSelector` (GUID, IPv4, friendly name, auto) or a pre-resolved `NicInfo`; device ID set explicitly or from the NIC MAC (`mac[0..6] ++ process_id BE`, so `mac ++ 00 00` by default; **verify** against a hardware device); device name checked against the Dante name rules (1 to 31 chars of `[A-Za-z0-9-]`, no leading/trailing `-`; **verify**); ports, latencies, channel counts, sample rate and bit depth range-checked. `Settings::try_new` is the fallible form of the upstream env-var/`BTreeMap` path (`INFERNO_*` variables still work, plus the new `STATE_DIR`); `Settings::new` keeps its signature and only panics there, with the error message. Names from the env path are not validated (only cut to 31 chars on a char boundary) and the historic default name `<app> <hex ip>` is kept. The old startup `unwrap`/`expect` calls are gone; the full robustness pass is W9. | ONS #89 (W4) |
| `inferno_aoip/src/state_storage.rs`, `Settings::state_dir` | **Injectable state dir.** `StateStorage::new(&self_info, &settings.state_dir)`; no more `platform_dirs` `unwrap()`. Default: `%ProgramData%\OpenNetworkSoundcard` on Windows (`default_state_dir`), the upstream `platform_dirs` path elsewhere. Files are `<state_dir>/<device id hex>/{rx,tx}_channels.toml` etc. Tests use temp dirs only. | ONS #90 (W5) |

### State file format decision (W5): keep TOML

Channel names and RX subscriptions stay in **per-item TOML files** (`rx_channels.toml`, `tx_channels.toml`, ...)
inside a per-device-ID folder, written with write-temp-then-rename. We do not move to the single `state.json`
that ONS ADR-006 originally sketched, because:

* the engine already reads and writes TOML, with tests; a JSON switch would be churn in code we want to keep
  mergeable with upstream, for no functional gain;
* TOML is human-editable, which helps support, and `ons-common` already uses TOML for `config.toml`;
* per-item files keep one corrupt file from losing everything (a bad `rx_channels.toml` only resets names).

The device *name* is not persisted by the engine; the host owns it (`device_name` in ONS `config.toml`) and
passes it through `SettingsBuilder::device_name`. ONS passes the root of `%ProgramData%\OpenNetworkSoundcard`
as the state dir, so Inferno's device-ID folder sits beside `config.toml` and `logs\`.

Known test gap: `searchfire`'s `client_and_server` integration test (real mDNS on UDP 5353) times out on
Windows (30 s). It is a submodule test and is not run in CI. Behaviour of mDNS next to the Windows DNS
Client service is covered by ONS issue W7.

## Building

```sh
git clone --recurse-submodules https://github.com/ZanyHunter/inferno
cd inferno
cargo build -p inferno_aoip -p inferno2pipe --target x86_64-pc-windows-msvc
cargo test  -p inferno_aoip -p inferno2pipe -p ons-usrvclock
```

Build and unit tests pass on `x86_64-pc-windows-msvc`. Nothing has been run on a network yet.
