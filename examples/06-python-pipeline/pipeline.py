"""Publish once, run many tasks over the same data.

This is the shape most real work takes: one dataset, many passes over it. The
point of the example is the timing difference between the first task and the
rest — the first pays for moving the data, the others do not.

    python pipeline.py [--mb 32] [--tasks 20]
"""

from __future__ import annotations

import argparse
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "sdk" / "python"))

from aethermesh import AetherMesh  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=7100)
    parser.add_argument("--mb", type=int, default=32, help="dataset size")
    parser.add_argument("--tasks", type=int, default=20)
    args = parser.parse_args()

    with AetherMesh.connect(host=args.host, port=args.port) as mesh:
        nodes = mesh.nodes()
        if not nodes:
            raise SystemExit("no nodes: start an agent first")
        print(f"{len(nodes)} node(s): {', '.join(node.hostname for node in nodes)}")

        # Something compressible but not uniform, like real data.
        dataset = bytes((i // 64) % 251 for i in range(args.mb * 1024 * 1024))

        started = time.perf_counter()
        published = mesh.publish(dataset)
        print(
            f"published {published.size_bytes / 1024 / 1024:.0f} MiB "
            f"in {(time.perf_counter() - started) * 1000:.0f} ms "
            f"as {published.data_id[:16]}…"
        )

        latencies = []
        for index in range(args.tasks):
            started = time.perf_counter()
            result = mesh.run("hash", f"pass-{index}".encode(), inputs=[published.data_id])
            elapsed = (time.perf_counter() - started) * 1000
            latencies.append(elapsed)

            if not result.success:
                raise SystemExit(f"task {index} failed: {result.error}")
            if index < 3 or index == args.tasks - 1:
                print(
                    f"  task {index:>3}: {result.output.hex()[:16]}… "
                    f"on {result.node_id[:8]} in {elapsed:6.1f} ms"
                )

        print()
        print(f"first task : {latencies[0]:7.1f} ms   (includes moving the data)")
        print(f"median     : {statistics.median(latencies[1:]):7.1f} ms   (data already there)")
        print(f"total      : {sum(latencies):7.1f} ms for {args.tasks} tasks")


if __name__ == "__main__":
    main()
