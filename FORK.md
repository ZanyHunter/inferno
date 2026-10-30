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
* New code in `ons-usrvclock/` is MIT OR Apache-2.0 (it is part of the usrvclock replacement, a compatible
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
| `inferno_aoip/tests/loopback_trx.rs` | Gated `#![cfg(unix)]` (needs `usrvclock::Server`). To be ported in W2. | ONS #88 (W2) |
| `ons-usrvclock/` | **TEMPORARY.** Facade crate that `inferno_aoip` now depends on (as `usrvclock`). Unix: re-exports the real upstream `usrvclock` unchanged. Windows: `windows_shim.rs`, a minimal stand-in (`ClockOverlay` math with `Instant`/QPC as the underlying clock, an `AsyncClient` that is only a `watch` channel with a `publish()` method, empty `SafeClock`). It exists so `inferno_aoip` compiles; **no overlay ever arrives** unless the owner calls `publish`. | Replaced by ONS #88 (W2) |

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
