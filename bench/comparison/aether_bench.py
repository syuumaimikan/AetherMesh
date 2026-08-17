"""Runs the workload on AetherMesh, through the Python SDK.

Same harness, same client language, same submit-and-wait loop as the Dask
benchmark — so what differs is the system, not the driver.

The controller and agents are started as child processes, exactly as a user
would run them, and shut down afterwards.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "sdk" / "python"))

from aethermesh import AetherMesh  # noqa: E402
from workload import Measurement, dataset, report  # noqa: E402

REPO = Path(__file__).resolve().parents[2]


def binary(name: str) -> Path:
    """Prefers a release build, falls back to debug."""
    for profile in ("release", "debug"):
        candidate = REPO / "target" / profile / (name + (".exe" if os.name == "nt" else ""))
        if candidate.exists():
            return candidate
    raise SystemExit(f"{name} is not built: run cargo build --release -p {name}")


class Mesh:
    """A controller with N agents, started and stopped around a benchmark."""

    def __init__(self, workers: int, agent_port: int, client_port: int) -> None:
        self.workers = workers
        self.agent_port = agent_port
        self.client_port = client_port
        self.processes: list[subprocess.Popen] = []

    def __enter__(self) -> "Mesh":
        env = dict(os.environ, RUST_LOG="warn")
        self.processes.append(
            subprocess.Popen(
                [
                    str(binary("aether-controller")),
                    "--listen",
                    f"127.0.0.1:{self.agent_port}",
                    "--client-listen",
                    f"127.0.0.1:{self.client_port}",
                ],
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        )
        time.sleep(1.0)

        for index in range(self.workers):
            self.processes.append(
                subprocess.Popen(
                    [
                        str(binary("aether-agent")),
                        "--controller",
                        f"127.0.0.1:{self.agent_port}",
                        "--heartbeat-secs",
                        "2",
                        # Each agent needs its own identity file, or they all
                        # register as the same node.
                        "--identity-path",
                        str(Path(os.environ.get("TEMP", "/tmp")) / f"aether-bench-node-{index}"),
                    ],
                    env=env,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
            )
        return self

    def __exit__(self, *_exc: object) -> None:
        for process in reversed(self.processes):
            process.terminate()
        for process in reversed(self.processes):
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()

    def wait_ready(self, mesh: AetherMesh, timeout: float = 30.0) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if len(mesh.nodes()) >= self.workers:
                return
            time.sleep(0.2)
        raise SystemExit(f"only {len(mesh.nodes())} of {self.workers} agents registered")


def run(
    tasks: int,
    workers: int,
    dataset_bytes: int,
    client_port: int,
    agent_port: int,
    latency_ms: float = 0.0,
    bandwidth_bytes_per_sec: int = 0,
) -> list[Measurement]:
    """Runs the workload, optionally through a shaped link.

    With `latency_ms` or `bandwidth_bytes_per_sec` set, the client talks to the
    controller through a relay that adds delay and a rate ceiling — the same
    workload over something that behaves less like loopback.
    """
    if latency_ms > 0 or bandwidth_bytes_per_sec > 0:
        return asyncio.run(
            _run_shaped(
                tasks,
                workers,
                dataset_bytes,
                client_port,
                agent_port,
                latency_ms,
                bandwidth_bytes_per_sec,
            )
        )

    measurements: list[Measurement] = []

    with Mesh(workers, agent_port, client_port) as cluster:
        with AetherMesh.connect(port=client_port) as mesh:
            cluster.wait_ready(mesh)

            payload = b"seed"
            latencies: list[float] = []
            started = time.perf_counter()
            for _ in range(tasks):
                task_started = time.perf_counter()
                mesh.run("echo", payload)
                latencies.append((time.perf_counter() - task_started) * 1000)
            wall = (time.perf_counter() - started) * 1000
            measurements.append(
                Measurement("aethermesh", "overhead", tasks, workers, 0, wall, latencies)
            )

            data = dataset(dataset_bytes)
            started = time.perf_counter()
            published = mesh.publish(data)
            publish_ms = (time.perf_counter() - started) * 1000

            latencies = []
            started = time.perf_counter()
            for _ in range(tasks):
                task_started = time.perf_counter()
                result = mesh.run("hash", payload, inputs=[published.data_id])
                if not result.success:
                    raise SystemExit(f"task failed: {result.error}")
                latencies.append((time.perf_counter() - task_started) * 1000)
            wall = (time.perf_counter() - started) * 1000 + publish_ms
            measurements.append(
                Measurement(
                    "aethermesh",
                    "dataset",
                    tasks,
                    workers,
                    dataset_bytes,
                    wall,
                    latencies,
                    notes=f"published once ({publish_ms:.0f} ms), moved to a node on first use",
                )
            )

    return measurements


async def _run_shaped(
    tasks: int,
    workers: int,
    dataset_bytes: int,
    client_port: int,
    agent_port: int,
    latency_ms: float,
    bandwidth_bytes_per_sec: int,
) -> list[Measurement]:
    """Same workload, with the client's link shaped by a local relay."""
    from link import ShapedLink

    link = ShapedLink(
        "127.0.0.1",
        client_port,
        latency_ms=latency_ms,
        bandwidth_bytes_per_sec=bandwidth_bytes_per_sec or 12_500_000,
    )

    with Mesh(workers, agent_port, client_port) as cluster:
        shaped_port = await link.start()
        try:
            # The client is synchronous, so it runs on a worker thread while the
            # relay keeps pumping on this loop.
            def workload() -> list[Measurement]:
                with AetherMesh.connect(port=shaped_port, timeout=600) as mesh:
                    cluster.wait_ready(mesh)
                    measurements = _measure(mesh, tasks, workers, dataset_bytes)
                    for measurement in measurements:
                        measurement.system = "aethermesh-shaped"
                        measurement.notes = (
                            f"{latency_ms:.0f} ms one-way, "
                            f"{(bandwidth_bytes_per_sec or 12_500_000) / 1_000_000:.1f} MB/s ceiling"
                        )
                    return measurements

            return await asyncio.to_thread(workload)
        finally:
            await link.stop()


def _measure(mesh: AetherMesh, tasks: int, workers: int, dataset_bytes: int) -> list[Measurement]:
    """The two workloads, against an already-connected mesh."""
    measurements: list[Measurement] = []
    payload = b"seed"

    latencies: list[float] = []
    started = time.perf_counter()
    for _ in range(tasks):
        task_started = time.perf_counter()
        mesh.run("echo", payload)
        latencies.append((time.perf_counter() - task_started) * 1000)
    wall = (time.perf_counter() - started) * 1000
    measurements.append(Measurement("aethermesh", "overhead", tasks, workers, 0, wall, latencies))

    if dataset_bytes > 0:
        data = dataset(dataset_bytes)
        started = time.perf_counter()
        published = mesh.publish(data)
        publish_ms = (time.perf_counter() - started) * 1000

        latencies = []
        started = time.perf_counter()
        for _ in range(tasks):
            task_started = time.perf_counter()
            result = mesh.run("hash", payload, inputs=[published.data_id])
            if not result.success:
                raise SystemExit(f"task failed: {result.error}")
            latencies.append((time.perf_counter() - task_started) * 1000)
        wall = (time.perf_counter() - started) * 1000 + publish_ms
        measurements.append(
            Measurement(
                "aethermesh",
                "dataset",
                tasks,
                workers,
                dataset_bytes,
                wall,
                latencies,
                notes=f"published once ({publish_ms:.0f} ms), moved to a node on first use",
            )
        )

    return measurements


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tasks", type=int, default=100)
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--dataset-bytes", type=int, default=8 * 1024 * 1024)
    parser.add_argument("--client-port", type=int, default=7180)
    parser.add_argument("--agent-port", type=int, default=7080)
    parser.add_argument(
        "--latency-ms",
        type=float,
        default=0.0,
        help="one-way delay to add on the client link (0 = loopback)",
    )
    parser.add_argument(
        "--bandwidth",
        type=int,
        default=0,
        help="bytes per second ceiling on the client link (0 = unlimited)",
    )
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    report(
        run(
            args.tasks,
            args.workers,
            args.dataset_bytes,
            args.client_port,
            args.agent_port,
            args.latency_ms,
            args.bandwidth,
        ),
        args.json,
    )


if __name__ == "__main__":
    main()
