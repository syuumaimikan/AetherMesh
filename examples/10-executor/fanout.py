"""The same fan-out, twice: once on local threads, once on the mesh.

Only the first line of each block differs. That is the point — code already
written against `concurrent.futures` moves over without being rewritten.
"""

from __future__ import annotations

import argparse
import hashlib
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, "../../sdk/python")

from aethermesh import AetherMeshError, MeshExecutor  # noqa: E402


try:
    from blake3 import blake3 as _blake3  # type: ignore

    def local_hash(payload: bytes) -> bytes:
        """Exactly what the mesh's `hash` task computes."""
        return _blake3(payload).digest()

    COMPARABLE = True
except ImportError:
    def local_hash(payload: bytes) -> bytes:
        """A stand-in: same shape of work, different algorithm.

        The mesh's `hash` task is BLAKE3, which the standard library does not
        have. `pip install blake3` to compare the digests as well as the time.
        """
        return hashlib.blake2b(payload, digest_size=32).digest()

    COMPARABLE = False


def local_spin(iterations: int) -> int:
    """What the mesh's `cpu` task does: a fixed amount of integer arithmetic."""
    total = 0
    for index in range(iterations):
        total = (total + index) % 1_000_003
    return total


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=7100)
    parser.add_argument("--token", default=None)
    parser.add_argument("--tasks", type=int, default=16)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--kib", type=int, default=256, help="payload size per task")
    parser.add_argument("--iterations", type=int, default=20_000_000)
    args = parser.parse_args()

    payloads = [bytes([index % 251]) * (args.kib * 1024) for index in range(args.tasks)]

    print(f"hashing {args.kib} KiB x {args.tasks} tasks:")
    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        local = list(pool.map(local_hash, payloads))
    local_ms = (time.perf_counter() - started) * 1000
    print(f"threads : {local_ms:8.1f} ms")

    try:
        pool = MeshExecutor.connect(
            host=args.host, port=args.port, token=args.token, max_workers=args.workers
        )
    except (OSError, AetherMeshError) as error:
        print(f"cannot reach the controller at {args.host}:{args.port}: {error}")
        return 1

    started = time.perf_counter()
    with pool:
        digest = pool.builtin("hash")
        remote = list(pool.map(digest, payloads))
    mesh_ms = (time.perf_counter() - started) * 1000
    print(f"mesh    : {mesh_ms:8.1f} ms")

    assert len(remote) == len(local), "map() must yield one result per input"
    if COMPARABLE:
        assert remote == local, "the mesh computed something different"
        print("          same digests from both, in the same order")
    else:
        print("          results arrived in input order (pip install blake3 to compare digests)")
    print("          hashing is cheap per byte, so on one machine the network wins. Below is not.")

    # CPU-bound work is where a Python thread pool stops being a pool at all:
    # the GIL serialises it. The mesh runs it on nodes, so it actually spreads.
    counts = [args.iterations] * args.tasks
    print(f"\nCPU-bound, {args.iterations:,} iterations x {args.tasks} tasks:")

    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        list(pool.map(local_spin, counts))
    local_ms = (time.perf_counter() - started) * 1000
    print(f"threads : {local_ms:8.1f} ms   (the GIL means these did not overlap)")

    with MeshExecutor.connect(
        host=args.host, port=args.port, token=args.token, max_workers=args.workers
    ) as pool:
        spin = pool.builtin("cpu")
        # The `cpu` task takes its iteration count as a little-endian u64.
        started = time.perf_counter()
        list(pool.map(spin, [count.to_bytes(8, "little") for count in counts]))
        mesh_ms = (time.perf_counter() - started) * 1000
    print(f"mesh    : {mesh_ms:8.1f} ms   ({local_ms / max(mesh_ms, 0.001):.0f}x)")
    print(
        "          Most of that gap is a Python loop against a native task, not\n"
        "          distribution. The distributed part is that the eight threads\n"
        "          did not overlap and the eight mesh tasks did."
    )

    # `as_completed` works too, because these are ordinary futures.
    with MeshExecutor.connect(
        host=args.host, port=args.port, token=args.token, max_workers=args.workers
    ) as pool:
        digest = pool.builtin("hash")
        futures = {pool.submit(digest, payload=data): index for index, data in enumerate(payloads[:4])}
        print("\nas_completed, in finishing order:")
        for future in as_completed(futures):
            print(f"  task {futures[future]}: {future.result()[:8].hex()}…")

        # A Python function is refused rather than quietly run locally.
        try:
            pool.submit(local_hash, payload=b"x")
        except TypeError as error:
            print(f"\nsubmitting a Python function: {error}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
