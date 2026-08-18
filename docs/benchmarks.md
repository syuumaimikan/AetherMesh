# Benchmarks

Two questions, measured separately, because they have different answers:

1. **What does the framework cost per task?** — a trivial task, submitted and
   awaited one at a time. What is left is scheduling, serialization, and round
   trips.
2. **What does the framework do with your data?** — every task reads the same
   8 MiB dataset. What is left is data movement.

## Against Dask

[Dask distributed](https://distributed.dask.org) is the closest widely used
system to compare against: a scheduler, workers, tasks that carry data.

```bash
python -m pip install "dask[distributed]"
cargo build --release -p aether-controller -p aether-agent
python bench/comparison/compare.py --tasks 100 --workers 3
```

100 tasks, 3 workers, one machine (16-core Windows desktop), loopback:

| system | workload | tasks/s | wall ms | p50 ms | p99 ms |
|---|---|---:|---:|---:|---:|
| **aethermesh** | overhead | **5,503** | 18 | **0.17** | **0.26** |
| dask | overhead | 63 | 1,582 | 15.39 | 39.09 |
| **aethermesh** | dataset (8 MiB) | **402** | 249 | **1.67** | **2.47** |
| dask-scatter | dataset (8 MiB) | 31 | 3,232 | 30.86 | 46.18 |
| dask-naive | dataset (8 MiB) | 21 | 4,699 | 40.39 | 87.56 |

`dask-naive` closes over the dataset, so a copy travels with every task.
`dask-scatter` calls `client.scatter(..., broadcast=True)` first, which is the
idiomatic fix. **AetherMesh behaves like the scatter case without being asked**:
publishing is separate from submitting, and the data reaches a node once.

### What this comparison is not

- **Not the same task body.** Dask runs a Python callable hashing with
  `hashlib.blake2b`; AetherMesh runs its built-in `hash` task in Rust with
  BLAKE3. The dataset row therefore mixes framework cost with a
  language-and-algorithm difference. The **overhead row is the fair
  framework-to-framework number** — both are doing nothing but moving a task
  through the system.
- **Not a feature comparison.** Dask does far more than AetherMesh: arbitrary
  Python callables, task graphs with dependencies, dataframes and arrays,
  spilling, adaptive scaling, a dashboard. AetherMesh runs built-in tasks and
  WebAssembly modules, and that is the whole menu.
- **Not a network measurement.** One machine, loopback. A real link makes the
  data-movement gap wider (moving 8 MiB per task hurts more at 100 Mbps than at
  loopback speed) and the overhead gap narrower (round trips cost more).
- **Not a scaling study.** One client, submitting sequentially, three workers.
  Neither system is being asked to do anything clever with concurrency.

Run it yourself before believing any of it — that is why the harness is in the
repository rather than the numbers alone.

## Against itself

The in-process harness isolates the optimization layer: the same binary, once
with every optimization off and once with them on.

```bash
cargo run -p aether-benchmark -- compare --tasks 100 --nodes 3 --dataset-bytes 8388608
```

| Metric | Baseline | AetherMesh |
|---|---:|---:|
| Bytes on the wire | 839,291,600 | **477,569** |
| Traffic reduction | — | **99.9 %** |
| Execution time | 71,173 ms | **404 ms** |
| P50 / P95 / P99 | 708 / 743 / 750 ms | **2.8 / 2.9 / 3.0 ms** |

*Baseline* is no dedup, no chunking, no compression, and load-only scheduling —
what a naive dispatcher does. Both sides run in-process through the real message
encoding and the real executor, so this isolates the optimization layer rather
than measuring network hardware.

## Reading any of these

The honest summary: AetherMesh is fast at the things it does because it does few
things, in Rust, over a small binary protocol. Where it genuinely differs in
kind rather than degree is data movement — content addressing and the locality
score mean the "send it once" case is the default rather than something you have
to know to ask for.

## On real sockets

Everything above runs a mesh inside one process, or two processes on one
machine. The `network` benchmark connects to a controller that is actually
running, as an ordinary client, and measures what crossed the wire.

```bash
cargo build --release -p aether-controller -p aether-agent -p aether-benchmark

./target/release/aether-controller --listen 127.0.0.1:7000 --client-listen 127.0.0.1:7100 &
./target/release/aether-agent --controller 127.0.0.1:7000 --advertise 127.0.0.1:7001 &
./target/release/aether-agent --controller 127.0.0.1:7000 \
  --identity-path ./second.id --advertise 127.0.0.1:7002 &

./target/release/aether-benchmark network --tasks 20 --dataset-bytes 4194304
```

Two agents on one machine, 16 cores, loopback:

```
20 tasks over a 4.0 MiB dataset (seed 1787032350104588100)

                         naive    aethermesh
  bytes sent          80.0 MiB       4.0 MiB
  wall clock            691 ms         37 ms
  mean task             1.0 ms        0.8 ms
  sends skipped              0            19

  traffic reduction: 95.0 %
```

The naive side publishes a fresh copy per task — different bytes, so a different
content hash, so nothing deduplicates and no node ever already holds it. That is
what a system which ships data to code does. AetherMesh publishes once, and 19
of the 20 tasks found the data already in place. The one that did not is the 5 %
that remains.

### Reproducing a number exactly

Every report ends with the command that produces it again:

```
  reproduce with:
    cargo run --release -p aether-benchmark -- network --controller 127.0.0.1:7100 \
      --tasks 20 --dataset-bytes 4194304 --seed 1787032350104588100
```

The seed is in it because the default is deliberately **not** fixed. The nodes
remember what they have been sent, so running the same seed twice measures a
mesh that already holds everything — the second run moves no bytes and reports
0 %. Two conditions have to hold for a rerun to mean anything:

1. **Pass the seed from the report.** Without it you get different datasets.
2. **Restart the agents first**, or use a seed that has never been run against
   this mesh. A node that already holds the data is a different starting
   condition, not a faster one.

The report says so when this happens rather than leaving a zero unexplained:

```
  ! The baseline moved no bytes, so there is nothing to compare against. The
    nodes most likely still hold this run's datasets from a previous one -
    pass a different --seed, or restart the agents.
```

Checked on this repository: the command above, run against a freshly restarted
two-agent mesh, reproduced 80.0 MiB / 4.0 MiB / 95.0 % exactly. Wall clock
differed by 3 % between runs, as wall clock does.

### On more than one machine

Nothing about the benchmark is local. It speaks the client API, so the only
change is the address:

```bash
./target/release/aether-benchmark network --controller 192.168.1.10:7100
```

Declare the mesh you mean to measure and a mismatch is refused rather than
reported:

```toml
# bench/nodes.toml
controller = "192.168.1.10:7100"

[[nodes]]
name = "desktop"
hostname = "workstation"

[[nodes]]
name = "raspberry-pi"
hostname = "rpi4"
labels = ["arch=arm64"]

[[nodes]]
name = "cloud"
hostname = "vm-eu-west-1"
labels = ["region=eu-west"]
```

```
Error: expected 3 node(s) but the mesh has 2; a result measured on a
different mesh is not the result you asked for
```

Publishing a one-node number as a three-node one is the easiest benchmark lie
there is, and this is the guard against making it by accident.

### What is in a report

`--format json` emits everything a reader needs to decide whether the number
applies to them:

| Field | Why it is there |
|---|---|
| `measured_at` | UTC, so two reports can be ordered |
| `client_os`, `client_arch`, `client_cpus` | the machine that submitted the work |
| `environment.nodes[]` | every node's hostname, address, cores, measured latency and link speed, labels |
| `loopback_only` | whether this measured a network at all |
| `seed`, `command` | how to run it again |
| `warnings[]` | reasons to distrust the numbers, in the report rather than a footnote |
| `baseline_bytes`, `aethermesh_bytes`, `reduction_percent` | the result |

### The number this repository still does not have

**Nobody has run any of this on real hardware over a real network.** Every
figure on this page, including the one above, comes from one machine talking to
itself. The transfer saving is real — bytes that do not cross a loopback socket
would not cross a real one either — but the latency and throughput columns are
measuring a memory copy.

That gap is now a `nodes.toml` and three machines away rather than a harness
away, which is as far as it can be closed from here. If you have the machines,
this is the most valuable contribution the project can receive.

### Catching a regression

`bench/baseline.json` is a committed report. `regress` runs the same work again
and compares:

```bash
cargo run --release -p aether-benchmark -- regress --baseline bench/baseline.json
```

```
                            baseline       current    change
  bytes moved              1048576.0     1048576.0      0.0%  ok
  traffic reduction %           87.5          87.5      0.0%  ok
  sends skipped                  7.0           7.0      0.0%  ok
  wall clock ms                  7.8           7.7     -0.4%  ok
  mean task ms                   0.2           0.2      1.2%  ok

  No gated regression.
```

It exits non-zero when a **gated** metric regressed, and CI runs it on every
push against a real controller and two real agents.

**Only the byte counts gate.** That is the whole design. `bytes_uncompressed`
for a given task count and dataset size is arithmetic rather than measurement —
checked identical across restarts, across seeds, and between a one-agent and a
two-agent mesh. It moves when deduplication or locality breaks and at no other
time.

Wall clock on a shared CI runner does not behave that way. A check built on it
fails for reasons that have nothing to do with the change, gets labelled flaky,
and then gets ignored — which leaves you worse off than before, because now
nobody is watching. So timings are printed beside the gated metrics and marked
`warn` when they drift, and `--gate-timing 20` turns them into failures for
anyone running on hardware they control.

A comparison between reports that measured *different work* is refused outright
rather than reported as a change:

```
Error: the reports measured different work (8 vs 20 tasks); comparing them
would report a difference nobody made
```

### Moving the baseline

When a change legitimately alters the byte counts — a different chunk size, a
new compression rule — regenerate it and say why in the commit:

```bash
cargo run --release -p aether-benchmark -- network \
  --tasks 8 --dataset-bytes 1048576 --format json --output bench/baseline.json
```

A baseline that gets regenerated whenever it complains is not a baseline. The
number is meant to be argued with.
