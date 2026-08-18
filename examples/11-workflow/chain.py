"""A three-step chain: does the intermediate result move between the steps?"""
import base64, sys
sys.path.insert(0, "../../sdk/python")
from aethermesh import AetherMesh

SIZE = 8 * 1024 * 1024

def traffic(m):
    return m._request({"type": "stats"})["traffic"]

with AetherMesh.connect(port=7100) as mesh:
    print("nodes:", [n.hostname + " " + n.node_id[:8] for n in mesh.nodes()])
    before = traffic(mesh)

    r = mesh._request({"type": "workflow", "steps": [
        {"kind": "echo", "payload": base64.b64encode(bytes(range(256)) * (SIZE // 256)).decode()},
        {"kind": "hash", "depends_on": [0]},
        {"kind": "hash", "depends_on": [1]},
    ]})
    assert r["type"] == "workflow", r
    print("success:", r["success"], "skipped:", r["skipped"])
    for s in r["steps"]:
        print(f"  step {s['step']}: ran on {s['node_id'][:8]} in {s['duration_ms']:.1f} ms")

    after = traffic(mesh)
    moved = after["bytes_uncompressed"] - before["bytes_uncompressed"]
    print(f"\nintermediate data moved: {moved} bytes ({moved / SIZE:.2f} x the 8 MiB payload)")
    print("all three steps on one node:", len({s["node_id"] for s in r["steps"]}) == 1)
