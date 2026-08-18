"""Split a suite across the mesh, and let an urgent run overtake a nightly one.

    python shards.py                  # 24 shards at normal priority
    python shards.py --contended      # a nightly flood, then an urgent run

The unit here is a *shard*: a slice of a test suite, or of a dataset, or of
anything you would otherwise loop over on one machine. The mesh does not care
what is inside — this uses the `cpu` builtin so the example needs no test suite
of its own.

`--contended` only demonstrates anything if a queue actually forms, so it
measures the queue rather than assuming one, and it submits urgent and ordinary
work at the same moment so there is something to compare against. On a
workstation with sixteen-core agents nothing ever waits; see the README for how
to make the mesh small enough to have a backlog.
"""
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, "../../sdk/python")
from aethermesh import AetherMesh

SHARDS = 24
WORK = 40_000_000          # iterations per shard
CONTENDED = "--contended" in sys.argv


def shard(index: int, priority: str) -> dict:
    """Runs one shard on its own connection.

    One connection is one queue: the protocol matches replies to requests in
    order, so a shard waiting on a node would hold up every shard queued behind
    it on the same socket. A connection per worker is the fix, and it is the
    same reason a web service wants a pool.
    """
    with AetherMesh.connect(port=7100) as mesh:
        started = time.perf_counter()
        result = mesh.run("cpu", WORK.to_bytes(8, "little"), priority=priority)
        return {
            "shard": index,
            "priority": priority,
            "node": result.node_id[:8],
            "ok": result.success,
            "task_ms": result.duration_ms,
            "wall_ms": (time.perf_counter() - started) * 1000,
        }


def run(count: int, priority: str, workers: int) -> list[dict]:
    with ThreadPoolExecutor(max_workers=workers) as pool:
        return list(pool.map(lambda i: shard(i, priority), range(count)))


class QueueWatcher(threading.Thread):
    """Samples how deep the controller's queue gets while work is in flight."""

    def __init__(self):
        super().__init__(daemon=True)
        self.deepest = 0
        self.stop = threading.Event()

    def run(self):
        with AetherMesh.connect(port=7100) as mesh:
            while not self.stop.is_set():
                self.deepest = max(self.deepest, mesh.stats()["queue"]["depth"])
                time.sleep(0.02)


def report(label: str, results: list[dict], wall: float) -> None:
    nodes: dict[str, int] = {}
    for entry in results:
        nodes[entry["node"]] = nodes.get(entry["node"], 0) + 1
    work = sum(entry["task_ms"] for entry in results)

    print(f"\n{label}")
    print(f"  shards        {len(results)}, {sum(1 for e in results if not e['ok'])} failed")
    print(f"  wall          {wall * 1000:.0f} ms")
    print(f"  slowest shard {max(e['wall_ms'] for e in results):.0f} ms")
    print(f"  work          {work:.0f} ms across {len(nodes)} node(s)"
          f" -> {work / (wall * 1000):.1f}x")
    print(f"  spread        {dict(sorted(nodes.items()))}")


with AetherMesh.connect(port=7100) as mesh:
    print(f"{len(mesh.nodes())} node(s) in the mesh")

if not CONTENDED:
    started = time.perf_counter()
    results = run(SHARDS, "normal", workers=SHARDS)
    report(f"{SHARDS} shards, all normal priority", results, time.perf_counter() - started)
    sys.exit(0)

# A nightly job is already using the whole mesh when somebody pushes a fix.
FLOOD = SHARDS * 8
PAIRS = SHARDS // 4
print(f"\nflooding the mesh with {FLOOD} background shards, then submitting"
      f" {PAIRS} critical and {PAIRS} normal at the same moment")

watcher = QueueWatcher()
watcher.start()

with ThreadPoolExecutor(max_workers=FLOOD) as background:
    flood = [background.submit(shard, i, "background") for i in range(FLOOD)]
    time.sleep(0.3)  # let the backlog build

    # The comparison that proves anything: two runs submitted at the same
    # moment into the same queue, differing only in priority. Either one alone
    # is a number with nothing to compare it against.
    with ThreadPoolExecutor(max_workers=PAIRS * 2) as racers:
        urgent_jobs = [racers.submit(shard, i, "critical") for i in range(PAIRS)]
        patient_jobs = [racers.submit(shard, i, "normal") for i in range(PAIRS)]
        urgent = [job.result() for job in urgent_jobs]
        patient = [job.result() for job in patient_jobs]

    nightly = [job.result() for job in flood]

watcher.stop.set()
watcher.join(timeout=1)

urgent_wait = sum(e["wall_ms"] for e in urgent) / len(urgent)
patient_wait = sum(e["wall_ms"] for e in patient) / len(patient)

print(f"\nqueue reached {watcher.deepest} deep")
print(f"  {len(nightly)} background shards, slowest "
      f"{max(e['wall_ms'] for e in nightly):.0f} ms")
print("\n  submitted at the same moment, into the same queue:")
print(f"    critical  mean {urgent_wait:6.0f} ms   slowest "
      f"{max(e['wall_ms'] for e in urgent):6.0f} ms")
print(f"    normal    mean {patient_wait:6.0f} ms   slowest "
      f"{max(e['wall_ms'] for e in patient):6.0f} ms")

if watcher.deepest == 0:
    print("\n  No queue ever formed, so priority had nothing to reorder and this")
    print("  run proves nothing. The mesh is faster than this flood. Start the")
    print("  agents with --max-concurrent-tasks 1 and try again — see the README.")
elif urgent_wait < patient_wait:
    print(f"\n  The critical shards waited {patient_wait / urgent_wait:.1f}x less than the")
    print("  normal ones submitted alongside them. Priority decides who waits, not")
    print("  who gets a bigger share: on an idle mesh it changes nothing at all.")
else:
    print("\n  The critical shards did NOT finish sooner. Either the queue drained")
    print("  faster than this took, or something is wrong — worth saying so rather")
    print("  than quoting whichever number flatters the feature.")
