"""AetherMesh verification helper.

Speaks the client protocol directly (length-prefixed JSON), so it needs no
SDK and no dependencies. Python 3.10+.

    python verify.py nodes                 who is in the mesh
    python verify.py traffic               publish once, run twice, show savings
    python verify.py workflow              a 3-step chain; how many bytes moved
    python verify.py resume <run-name>     resumable run; run it twice

Point it elsewhere with --controller host:port (default 127.0.0.1:7100) and
--token if the mesh requires one.
"""

import argparse
import base64
import json
import os
import socket
import sys


class Mesh:
    def __init__(self, address: str, token: str | None):
        host, _, port = address.rpartition(":")
        self.sock = socket.create_connection((host, int(port)), timeout=120)
        self.call({"type": "hello", "token": token})

    def call(self, request: dict) -> dict:
        body = json.dumps(request).encode()
        self.sock.sendall(len(body).to_bytes(4, "big") + body)
        size = int.from_bytes(self._read(4), "big")
        return json.loads(self._read(size))

    def _read(self, size: int) -> bytes:
        buf = b""
        while len(buf) < size:
            chunk = self.sock.recv(size - len(buf))
            if not chunk:
                raise ConnectionError("the controller closed the connection")
            buf += chunk
        return buf

    def close(self):
        self.sock.close()


def step(kind: str, payload: bytes = b"", depends_on=()) -> dict:
    return {
        "kind": kind,
        "payload": base64.b64encode(payload).decode(),
        "depends_on": list(depends_on),
    }


def check(label: str, actual, expected, note: str = "") -> bool:
    ok = actual == expected if not callable(expected) else expected(actual)
    mark = "OK  " if ok else "FAIL"
    suffix = f"   ({note})" if note else ""
    print(f"  [{mark}] {label}: {actual}{suffix}")
    return ok


def cmd_nodes(mesh: Mesh) -> bool:
    reply = mesh.call({"type": "nodes"})
    nodes = reply.get("nodes", [])
    print(f"{len(nodes)} node(s):")
    for node in nodes:
        print(
            f"  {node['node_id'][:8]}  {node['hostname']:<20} "
            f"cores={node['cpu_cores']:<3} cpu={node['cpu_usage']:.2f} "
            f"held={node.get('datasets_held', 0)} "
            f"connected={node.get('connected')}"
        )
    return check("at least one node", len(nodes), lambda n: n >= 1)


def cmd_traffic(mesh: Mesh) -> bool:
    before = mesh.call({"type": "stats"})["traffic"]

    # Random, not zeros: a zeroed dataset is one repeated chunk, so
    # chunk dedup would flatter the number being measured here.
    payload = os.urandom(4 * 1024 * 1024)
    published = mesh.call(
        {"type": "publish", "data": base64.b64encode(payload).decode()}
    )
    data_id = published["data_id"]
    print(f"published {published['size_bytes']} bytes as {data_id[:16]}…")

    for index in range(4):
        result = mesh.call({"type": "submit", "kind": "hash", "inputs": [data_id]})
        if result["type"] != "result" or not result["success"]:
            print(f"  task {index} failed: {result}")
            return False
        print(f"  task {index}: {result['duration_ms']:.1f} ms on {result['node_id'][:8]}")

    after = mesh.call({"type": "stats"})["traffic"]
    moved = after["bytes_uncompressed"] - before["bytes_uncompressed"]
    skipped = after["transfers_skipped"] - before["transfers_skipped"]

    print(f"\n4 tasks over one 4 MiB dataset:")
    ok = check("bytes moved", moved, lambda b: b <= 9 * 1024 * 1024,
               "at most one copy per node, not one per task (4 MiB x 4 = 16 MiB naive)")
    return check("transfers skipped", skipped, lambda s: s >= 3) and ok


def cmd_workflow(mesh: Mesh) -> bool:
    before = mesh.call({"type": "stats"})["traffic"]
    reply = mesh.call(
        {
            "type": "workflow",
            "steps": [
                step("echo", os.urandom(2 * 1024 * 1024)),
                step("hash", b"", [0]),
                step("hash", b"", [1]),
            ],
        }
    )
    if reply["type"] != "workflow":
        print(f"  refused: {reply}")
        return False
    after = mesh.call({"type": "stats"})["traffic"]

    for outcome in reply["steps"]:
        print(f"  step {outcome['step']} on {outcome['node_id'][:8]} "
              f"{outcome['duration_ms']:.1f} ms success={outcome['success']}")
    moved = after["bytes_uncompressed"] - before["bytes_uncompressed"]

    ok = check("all steps succeeded", reply["success"], True)
    return check("intermediate bytes moved", moved, 0,
                 "each step ran where the previous one left its output") and ok


def cmd_resume(mesh: Mesh, run: str) -> bool:
    # The last step cannot succeed, so the first two are all that get recorded.
    steps = [step("echo", b"seed"), step("hash", b"", [0]), step("no-such-kind", b"", [1])]

    first = mesh.call({"type": "workflow", "steps": steps, "run": run})
    if first["type"] != "workflow":
        print(f"  refused: {first}")
        return False
    print(f"  run 1: ran={[s['step'] for s in first['steps']]} "
          f"resumed={first.get('resumed', [])}")

    second = mesh.call({"type": "workflow", "steps": steps, "run": run})
    print(f"  run 2: ran={[s['step'] for s in second['steps']]} "
          f"resumed={second.get('resumed', [])}")

    ok = check("second run resumed the finished steps", second.get("resumed"), [0, 1],
               "needs checkpoint_path set on the controller")
    ok = check("second run only ran the failing step",
               [s["step"] for s in second["steps"]], [2]) and ok

    # A different workflow under the same name must be refused, not resumed.
    other = mesh.call(
        {"type": "workflow", "steps": [step("echo", b"different")], "run": run}
    )
    return check("a different workflow under this name is refused",
                 other["type"], "error") and ok


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["nodes", "traffic", "workflow", "resume"])
    parser.add_argument("run", nargs="?", default="verify")
    parser.add_argument("--controller", default="127.0.0.1:7100")
    parser.add_argument("--token", default=None)
    args = parser.parse_args()

    mesh = Mesh(args.controller, args.token)
    try:
        if args.command == "nodes":
            ok = cmd_nodes(mesh)
        elif args.command == "traffic":
            ok = cmd_traffic(mesh)
        elif args.command == "workflow":
            ok = cmd_workflow(mesh)
        else:
            ok = cmd_resume(mesh, args.run)
    finally:
        mesh.close()

    print("\nPASS" if ok else "\nFAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
