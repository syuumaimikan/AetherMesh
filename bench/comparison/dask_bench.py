"""Runs the workload on Dask distributed.

Two dataset variants, because both are things people actually write:

* ``dask-naive`` closes over the dataset, so every task carries a copy.
* ``dask-scatter`` calls ``client.scatter(..., broadcast=True)`` first, which
  is the idiomatic fix and moves the data once.

AetherMesh does the scatter case automatically, without the user knowing to
ask for it — that is the comparison worth making.
"""

from __future__ import annotations

import argparse
import time

from workload import Measurement, dataset, digest, report, trivial


def run(tasks: int, workers: int, dataset_bytes: int) -> list[Measurement]:
    from distributed import Client, LocalCluster

    measurements: list[Measurement] = []

    with LocalCluster(
        n_workers=workers,
        threads_per_worker=1,
        processes=True,
        dashboard_address=None,
        silence_logs=50,
    ) as cluster, Client(cluster) as client:
        client.wait_for_workers(workers)

        # Overhead: trivial task, submitted and awaited one at a time so the
        # numbers are per-task latency rather than batch pipelining.
        payload = b"seed"
        latencies: list[float] = []
        started = time.perf_counter()
        for _ in range(tasks):
            task_started = time.perf_counter()
            client.submit(trivial, payload, pure=False).result()
            latencies.append((time.perf_counter() - task_started) * 1000)
        wall = (time.perf_counter() - started) * 1000
        measurements.append(
            Measurement("dask", "overhead", tasks, workers, 0, wall, latencies)
        )

        data = dataset(dataset_bytes)

        # Naive: the dataset travels with every task.
        latencies = []
        started = time.perf_counter()
        for _ in range(tasks):
            task_started = time.perf_counter()
            client.submit(digest, payload, data, pure=False).result()
            latencies.append((time.perf_counter() - task_started) * 1000)
        wall = (time.perf_counter() - started) * 1000
        measurements.append(
            Measurement(
                "dask-naive",
                "dataset",
                tasks,
                workers,
                dataset_bytes,
                wall,
                latencies,
                notes="dataset captured by the task: one copy per submission",
            )
        )

        # Idiomatic: scattered once, then referenced by future.
        started = time.perf_counter()
        handle = client.scatter(data, broadcast=True)
        scatter_ms = (time.perf_counter() - started) * 1000

        latencies = []
        started = time.perf_counter()
        for _ in range(tasks):
            task_started = time.perf_counter()
            client.submit(digest, payload, handle, pure=False).result()
            latencies.append((time.perf_counter() - task_started) * 1000)
        wall = (time.perf_counter() - started) * 1000 + scatter_ms
        measurements.append(
            Measurement(
                "dask-scatter",
                "dataset",
                tasks,
                workers,
                dataset_bytes,
                wall,
                latencies,
                notes=f"explicit scatter first ({scatter_ms:.0f} ms), then reused",
            )
        )

    return measurements


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tasks", type=int, default=100)
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--dataset-bytes", type=int, default=8 * 1024 * 1024)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    report(run(args.tasks, args.workers, args.dataset_bytes), args.json)


if __name__ == "__main__":
    main()
