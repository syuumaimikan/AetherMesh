"""Shows which node a set of constraints selects, and when nothing does.

Start a controller and three labelled agents first — see the README.
"""

from __future__ import annotations

import argparse
import sys

sys.path.insert(0, "../../sdk/python")

from aethermesh import AetherMesh, AetherMeshError  # noqa: E402

#: Each row is (description, constraints).
CASES: list[tuple[str, list[str]]] = [
    ("anywhere", []),
    ("kind=gpu", ["kind=gpu"]),
    ("region=eu-west", ["region=eu-west"]),
    ("region!=eu-west", ["region!=eu-west"]),
    ("kind=gpu, region=us-east", ["kind=gpu", "region=us-east"]),
]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=7100)
    parser.add_argument("--token", default=None)
    args = parser.parse_args()

    try:
        mesh = AetherMesh.connect(host=args.host, port=args.port, token=args.token)
    except (OSError, AetherMeshError) as error:
        print(f"cannot reach the controller at {args.host}:{args.port}: {error}")
        return 1

    with mesh:
        nodes = mesh.nodes()
        print(f"{len(nodes)} node(s):")
        for node in nodes:
            labels = " ".join(f"{key}={value}" for key, value in sorted(node.labels.items()))
            print(f"  {node.node_id[:8]}  {node.hostname:<12} {labels or '(no labels)'}")

        if not nodes:
            print("\nnothing to place work on — start an agent")
            return 1

        print()
        width = max(len(name) for name, _ in CASES)
        for name, constraints in CASES:
            try:
                result = mesh.run("echo", b"x", constraints=constraints)
            except AetherMeshError as error:
                # No node satisfies them. The task is refused, not relocated.
                print(f"{name:<{width}} -> refused: {error}")
                continue

            where = result.node_id[:8] if result.success else "failed on the node"
            print(f"{name:<{width}} -> {where}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
