# Changelog

Notable changes, newest first. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
intends to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
from 0.1.0 onward. Until then, `main` is the release.

## Unreleased

### Added

- **Node labels and task constraints.** An agent declares what it is
  (`--label gpu=true --label region=eu-west`); a task says what it needs
  (`gpu=true`, `region!=us-east`, or a bare `nvme` for "has this label").
  Constraints filter before any scoring, so a task no node satisfies is
  refused rather than placed somewhere it was not allowed. Carried through the
  client API and all three SDKs.
- **Idle heartbeat backoff.** An idle node doubles the gap between heartbeats
  up to half the controller's eviction window, which the controller now
  declares at registration; work or a real change in load snaps it straight
  back. A mesh spends most of its life idle, and that was the state costing the
  most power.
- **Result cache.** Repeated work keyed by content-addressed identity — task
  kind, payload hash, module, and inputs. Failures are never cached. Size and
  TTL are configurable; hit and miss counts are reported.
- **Examples, eight of them**, from one terminal to two devices to a browser
  upload, each with a script for Unix and Windows.
- **A documentation site** at `docs/`, published to GitHub Pages.
- **A threat model** at `docs/security.md`, including what is deliberately out
  of scope.
- **CI across Linux, Windows, and macOS**, with feature-combination builds,
  `clippy -D warnings`, `cargo audit`, and cross-builds for `aarch64` and
  `armv7`.
- **Dual licensing files, a contributing guide, and a security policy.**

### Changed

- **The Japanese README is written in Japanese**, not translated from the
  English one.
- **Release binaries are much smaller**: fat LTO, one codegen unit, symbols
  stripped, panics aborted. The controller is 1.3 MB and the agent 2.7 MB.
  Dependency debuginfo is off in dev builds too, which is most of what `target/`
  used to weigh.
- **The TypeScript SDK can be type-checked** — it has a `tsconfig.json` and a
  `typecheck` script, which it did not before.

### Fixed

- **Connections are bound to the identity they authenticated as.** Message
  handlers had trusted the node id in the message body, so a registered agent
  could attach a data channel for another node, complete another node's
  transfers, or send heartbeats that steered the scheduler on another node's
  behalf. Every handler now uses the connection's own identity, and data
  channels are claimed with a per-node token issued at registration.
- **The `random` WASM capability draws from the OS CSPRNG.** It previously
  stretched a clock-and-address seed through an LCG and was documented as
  unsuitable for keys — but documentation does not travel with the bytes, and
  a module author reaching for it to build a nonce got something predictable
  with no signal that anything was wrong.
- **Chunk deduplication no longer deadlocks** when the receiver already holds
  some of the chunks: the assembler fills those from the local store instead of
  waiting for a transfer that will never come.
- **Heartbeats no longer corrupt frames under cancellation.** They ran inside a
  `select!` that could drop a partially written frame; they now own a task.
