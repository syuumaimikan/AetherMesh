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
- **Task priorities and a queue.** `critical`, `high`, `normal`, `low`,
  `background`; higher first, FIFO within a level, and a level of promotion for
  every 30 seconds waited so low priority means later rather than never.
  Dispatch was strictly first-come, first-served before this — and, since one
  task is dispatched at a time, a long backlog meant urgent work waited behind
  all of it.
- **The mesh runs more than one task at a time.** Dispatch needed exclusive
  access to the controller, so however many nodes were registered, exactly one
  task was ever in flight. `TaskTransport` now takes `&self` and the
  controller's mutable state is shared, so dispatches run concurrently up to a
  bound.
- **The scheduler counts work it has already sent.** Nodes report load on a
  heartbeat, so for the seconds between two of them a node that has just been
  given sixty-four tasks looks exactly as idle as one given none — and got all
  sixty-four. Measured on four agents: 64/0/0/0 before, 16/16/16/16 after,
  166 tasks/s to 638.
- **Placement weights are settable** (`[scheduler_weights]`). The README had
  claimed they were configurable; they were not reachable from the binary.
- **Workflow results report where each step ran**, how big its output was,
  whether each dependency's data was already there, and how many bytes moved
  to place it — so a workflow's data movement can be read off the result
  rather than inferred from global counters.
- **Workflows** — tasks that depend on other tasks, run in dependency order.
  A step's output is kept on the node that produced it and recorded in the same
  catalog as any published dataset, so the existing locality score already
  prefers that node for whatever reads it next. Measured on a live three-node
  mesh: three chained steps, 8 MiB flowing through them, **zero bytes** of
  intermediate data moved. Cycles and dangling dependencies are refused before
  anything runs; a failed step stops what waits on it and reports which steps
  were skipped.
- **A regression check that runs in CI** (`aether-benchmark regress`). Every
  push builds a real controller and two real agents, reruns the network
  benchmark, and fails if traffic reduction or bytes moved drifted from
  `bench/baseline.json`. Only byte counts gate: they are arithmetic given the
  configuration, verified identical across restarts, seeds and node counts.
  Wall clock on a shared runner is reported beside them and never fails the
  build, because a check that cries wolf is a check that gets switched off.
- **Benchmark reproducibility.** Every network report ends with the command
  that produces it again, seed included; records the client's CPU count, OS,
  and an RFC 3339 timestamp; and `docs/benchmarks.md` documents what has to be
  true for a rerun to mean anything. Checked by rerunning a report's own
  command against a restarted mesh and getting the same bytes.
- **A benchmark that measures a mesh which is actually running**
  (`aether-benchmark network`). Everything before it simulated a mesh in one
  process, where the "bandwidth" was a number somebody typed. This one connects
  as an ordinary client, so pointing it at real machines needs an address
  rather than a code change. It records the environment it measured, says so
  when every node is on loopback, and refuses to report a number from a mesh
  that does not match the `nodes.toml` you declared.
- **Queue policies**: `max_queue_size`, `queue_timeout_secs`, a per-task
  `timeout_ms`, and a rejection policy of `reject`, `drop_oldest`, or
  `drop_lowest_priority`. All off by default. Whatever the queue decides, the
  caller is told — a refusal or an expiry comes back as an error rather than a
  reply channel that never resolves.
- **A terminal dashboard** (`aether-tui`). Throughput, what the mesh did not
  have to move, and which node holds what — plus sending a task from the
  dashboard to watch where it lands. It reconnects on its own and keeps the last
  known numbers on screen while it does, because "the mesh went quiet" and
  "every node left" are different emergencies.
- **A release workflow** building the controller, agent, and dashboard for
  Linux, macOS, and Windows, including `aarch64` and `armv7`.
- **`Stats` on the client API and traffic counters on `/metrics`.** The figures
  that describe the whole point of the project — bytes moved, bytes saved by
  compression, transfers and chunks skipped — were private fields on a struct
  one task owns exclusively, and so were unreadable from anywhere else.
- **A storage budget on the agent** (`--storage-budget-mb`). An agent's data
  cache previously grew for as long as the process ran, with no way to bound
  it — fine on a workstation, fatal on a board with 1 GB of RAM. Over budget it
  drops the least recently used datasets and tells the controller which ones,
  so the catalog does not keep sending work whose inputs are gone.
- **`/metrics` and `/healthz`** (`--metrics-listen`). The counters existed and
  were already formatted for Prometheus; there was no way to fetch them.
- **Java and C# SDKs**, joining TypeScript, Python, and Go. Neither has a
  runtime dependency: the Java one parses the protocol's JSON itself rather
  than putting a dependency-resolution problem between a user and their first
  task. Both were run against a live mesh, and both are checked in CI.
- **`MeshExecutor` for Python** — a real `concurrent.futures.Executor`, so code
  already written against a thread pool moves over by changing the constructor.
  It refuses a Python callable rather than silently running it locally.
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
- **A node that hung up is skipped, not dispatched at.** The registry keeps a
  node until the health monitor times it out — deliberately, since a late
  heartbeat is not a death — so the scheduler would pick a node whose socket
  had already closed, fail, and spend a retry discovering it. Dispatch now asks
  the transport first. A closed socket is not ambiguous.
- **An agent no longer registers with a fabricated CPU figure.** It sampled CPU
  immediately after constructing the collector, and CPU usage is a difference
  between two samples — on Windows the number came out as 100 %, which kept
  work off a completely idle machine until its first heartbeat corrected it.
- **Chunk deduplication no longer deadlocks** when the receiver already holds
  some of the chunks: the assembler fills those from the local store instead of
  waiting for a transfer that will never come.
- **Heartbeats no longer corrupt frames under cancellation.** They ran inside a
  `select!` that could drop a partially written frame; they now own a task.
