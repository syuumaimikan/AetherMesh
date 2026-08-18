"""A three-step chain: does the intermediate result move between the steps?"""
import sys
sys.path.insert(0, "../../sdk/python")
from aethermesh import AetherMesh, Step

SIZE = 8 * 1024 * 1024

with AetherMesh.connect(port=7100) as mesh:
    print("nodes:", [n.hostname + " " + n.node_id[:8] for n in mesh.nodes()])
    before = mesh.stats()["traffic"]

    result = mesh.workflow([
        Step("echo", payload=bytes(range(256)) * (SIZE // 256)),
        Step("hash", depends_on=(0,)),
        Step("hash", depends_on=(1,)),
    ])

    print("success:", result.success, "skipped:", result.skipped)
    for step in result.steps:
        print(f"  step {step.step}: ran on {step.node_id[:8]} in {step.duration_ms:.1f} ms")

    after = mesh.stats()["traffic"]
    moved = after["bytes_uncompressed"] - before["bytes_uncompressed"]
    print(f"\nintermediate data moved: {moved} bytes ({moved / SIZE:.2f} x the 8 MiB payload)")
    print("all three steps on one node:", len({s.node_id for s in result.steps}) == 1)
