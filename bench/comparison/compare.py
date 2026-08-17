"""Runs the same workload on AetherMesh and on Dask and prints both.

    python -m pip install "dask[distributed]"
    cargo build --release -p aether-controller -p aether-agent
    python bench/comparison/compare.py --tasks 100 --workers 3

Numbers from one machine over loopback are a lower bound on what a real network
does to the data-movement row and an upper bound on what it does to the
overhead row. Read `docs/benchmarks.md` before quoting any of it.
"""

from __future__ import annotations

import argparse

import aether_bench
import dask_bench
from workload import report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tasks", type=int, default=100)
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--dataset-bytes", type=int, default=8 * 1024 * 1024)
    parser.add_argument("--client-port", type=int, default=7180)
    parser.add_argument("--agent-port", type=int, default=7080)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--skip-dask", action="store_true")
    args = parser.parse_args()

    measurements = aether_bench.run(
        args.tasks, args.workers, args.dataset_bytes, args.client_port, args.agent_port
    )
    if not args.skip_dask:
        measurements += dask_bench.run(args.tasks, args.workers, args.dataset_bytes)

    # Group by workload so the rows that belong together are next to each other.
    order = {"overhead": 0, "dataset": 1}
    measurements.sort(key=lambda m: (order.get(m.workload, 9), m.system))
    report(measurements, args.json)


if __name__ == "__main__":
    main()
