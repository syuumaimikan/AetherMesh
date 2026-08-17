"""Publishes a dataset once and hashes it from three tasks.

Run a controller and at least one agent first, then:
    python examples/hash.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from aethermesh import AetherMesh  # noqa: E402


def main() -> None:
    with AetherMesh.connect(
        host=os.environ.get("AETHERMESH_HOST", "127.0.0.1"),
        port=int(os.environ.get("AETHERMESH_PORT", "7100")),
        token=os.environ.get("AETHERMESH_TOKEN"),
    ) as mesh:
        print("nodes:", [node.hostname for node in mesh.nodes()])

        # 4 MiB of repetitive data: published once, transferred once.
        published = mesh.publish(b"\xab" * (4 * 1024 * 1024))
        print(
            f"published {published.size_bytes} bytes as {published.data_id[:16]}…")

        for index in range(3):
            result = mesh.run("hash", b"seed", inputs=[published.data_id])
            print(
                f"task {index}: {result.output.hex()[:16]}… "
                f"on {result.node_id[:8]} in {result.duration_ms:.1f} ms"
            )


if __name__ == "__main__":
    main()
