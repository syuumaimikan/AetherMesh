"""A nightly job that fails halfway, and does not start over when you rerun it.

    python pipeline.py

Needs a controller started with `checkpoint_path` set — see the README.

The shape is the ordinary one for batch work: ingest, then several independent
transforms over what ingest produced, then a rollup that reads all of them. The
rollup is pinned to a machine with `role=rollup`, which is where this example's
3am failure comes from: that machine is not in the mesh yet.

Run it, start the missing node, run it again. Nothing that finished runs twice.
"""
import sys

sys.path.insert(0, "../../sdk/python")
from aethermesh import AetherMesh, AetherMeshError, Step

RUN = "nightly"
SIZE = 4 * 1024 * 1024

STAGES = [
    Step("echo", payload=bytes(range(256)) * (SIZE // 256)),  # 0 ingest
    Step("hash", depends_on=(0,)),                            # 1 transform
    Step("hash", depends_on=(0,)),                            # 2 transform
    Step("hash", depends_on=(0,)),                            # 3 transform
    # 4 rollup: only on the machine that owns the warehouse credentials, or
    # the fast disk, or whatever makes it special. Here: a label.
    Step("hash", depends_on=(1, 2, 3), constraints=("role=rollup",)),
]

with AetherMesh.connect(port=7100) as mesh:
    nodes = mesh.nodes()
    rollup_nodes = [n for n in nodes if n.labels.get("role") == "rollup"]
    print(f"{len(nodes)} node(s), {len(rollup_nodes)} of them able to run the rollup")

    before = mesh.stats()["traffic"]

    try:
        result = mesh.workflow(STAGES, run=RUN)
    except AetherMeshError as error:
        print(f"\nrun {RUN!r} stopped: {error}")
        print("\nWhatever finished before this is recorded. Start a node that")
        print("can run the rollup and run this again:")
        print("\n  aether-agent --controller 127.0.0.1:7000 \\")
        print("      --identity-path ./rollup.id --label role=rollup\n")
        sys.exit(1)

    moved = mesh.stats()["traffic"]["bytes_uncompressed"] - before["bytes_uncompressed"]

    print(f"\nrun {RUN!r}: success={result.success}")
    print(f"  resumed  {result.resumed}   (finished by an earlier run, not run again)")
    print(f"  ran      {[s.step for s in result.steps]}")
    print(f"  skipped  {result.skipped}")
    for step in result.steps:
        mark = "ok " if step.success else "FAIL"
        detail = f" — {step.error}" if step.error else ""
        print(f"    [{mark}] stage {step.step} on {step.node_id[:8]} "
              f"{step.duration_ms:5.1f} ms{detail}")

    print(f"\n  {moved} bytes moved this run "
          f"({moved / SIZE:.2f} x the {SIZE // (1024 * 1024)} MiB ingest)")
