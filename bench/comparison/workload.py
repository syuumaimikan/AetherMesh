"""The workload both systems run, and the shape of a result.

Two shapes of work, because they measure different things:

* ``overhead`` — a trivial task. What is left is the framework: scheduling,
  serialization, and round trips.
* ``dataset`` — every task reads the same large dataset. What is left is data
  movement: whether the system sends it once or once per task.

The task body is deliberately identical in spirit rather than in code: each
system runs its own natural implementation, which is what a user would write.
The comparison is of systems, not of hash function implementations, so the
dataset run reports bytes moved alongside wall-clock time.
"""

from __future__ import annotations

import hashlib
import json
import sys
from dataclasses import asdict, dataclass, field


@dataclass
class Measurement:
    """One system's numbers for one workload."""

    system: str
    workload: str
    tasks: int
    workers: int
    dataset_bytes: int
    wall_ms: float
    latencies_ms: list[float] = field(default_factory=list)
    notes: str = ""

    @property
    def throughput(self) -> float:
        return self.tasks / (self.wall_ms / 1000.0) if self.wall_ms > 0 else 0.0

    def percentile(self, quantile: float) -> float:
        if not self.latencies_ms:
            return 0.0
        ordered = sorted(self.latencies_ms)
        rank = max(1, min(len(ordered), int(quantile * len(ordered) + 0.999)))
        return ordered[rank - 1]

    def to_dict(self) -> dict:
        data = asdict(self)
        data.pop("latencies_ms")
        data["throughput_tasks_per_sec"] = round(self.throughput, 1)
        data["p50_ms"] = round(self.percentile(0.50), 3)
        data["p95_ms"] = round(self.percentile(0.95), 3)
        data["p99_ms"] = round(self.percentile(0.99), 3)
        data["wall_ms"] = round(self.wall_ms, 3)
        return data


def dataset(size_bytes: int) -> bytes:
    """Repetitive but not uniform, like real data: compressible, not trivial."""
    return bytes((i // 64) % 251 for i in range(size_bytes))


def digest(payload: bytes, data: bytes) -> bytes:
    """The dataset task, in Python. Dask runs this; AetherMesh runs its own."""
    hasher = hashlib.blake2b(digest_size=32)
    hasher.update(payload)
    hasher.update(data)
    return hasher.digest()


def trivial(payload: bytes) -> int:
    """The overhead task: touch the argument, return something tiny."""
    return len(payload)


def report(measurements: list[Measurement], as_json: bool) -> None:
    if as_json:
        json.dump([m.to_dict() for m in measurements], sys.stdout, indent=2)
        print()
        return

    header = f"{'system':<24}{'workload':<10}{'tasks':>7}{'wall ms':>12}{'tasks/s':>10}{'p50':>9}{'p99':>9}"
    print(header)
    print("-" * len(header))
    for m in measurements:
        print(
            f"{m.system:<24}{m.workload:<10}{m.tasks:>7}{m.wall_ms:>12.1f}"
            f"{m.throughput:>10.1f}{m.percentile(0.50):>9.2f}{m.percentile(0.99):>9.2f}"
        )
        if m.notes:
            print(f"{'':<24}{m.notes}")
