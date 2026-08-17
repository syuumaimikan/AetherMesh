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
