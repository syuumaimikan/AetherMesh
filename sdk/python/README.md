# aethermesh (Python)

Python client for [AetherMesh](../../README.md): publish data once, run tasks —
including WebAssembly modules — across a mesh of machines.

Python 3.10+. No dependencies: the protocol is a socket, `struct`, and `json`.

## Start a mesh

```bash
cargo run -p aether-controller            # agent port 7000, client API port 7100
cargo run -p aether-agent                 # on every machine that should do work
```

## Use it

```python
from aethermesh import AetherMesh

with AetherMesh.connect(port=7100) as mesh:
    print([node.hostname for node in mesh.nodes()])

    # Published once; the mesh moves it only to nodes that actually need it.
    data = mesh.publish(open("input.bin", "rb").read())

    result = mesh.run("hash", b"seed", inputs=[data.data_id])
    print(result.output.hex(), f"{result.duration_ms:.1f} ms on {result.node_id[:8]}")
```

### WebAssembly tasks

```python
module = mesh.publish_file("uppercase.wasm")
result = mesh.run_wasm(module.data_id, b"hello")
result.output  # b"HELLO"
```

Building a module from TypeScript, Rust, or Go: [`docs/wasm-tasks.md`](../../docs/wasm-tasks.md).

### Authentication and TLS

```python
mesh = AetherMesh.connect(
    host="mesh.example.com",
    port=7100,
    token=os.environ["AETHERMESH_TOKEN"],
    tls_ca_path="cert.pem",
)
```

## API

| Method | Returns |
|---|---|
| `AetherMesh.connect(host, port, token, tls_ca_path, ...)` | a connected client (also a context manager) |
| `mesh.publish(data)` | `Published(data_id, size_bytes)` |
| `mesh.publish_file(path)` | same, reading from disk |
| `mesh.run(kind, payload=b"", inputs=[])` | `TaskResult` — built-in `echo`, `hash`, `cpu` |
| `mesh.run_wasm(module_id, payload=b"", inputs=[])` | `TaskResult` |
| `mesh.nodes()` | `list[NodeSummary]` |
| `mesh.close()` | — |

A `TaskResult` carries `task_id`, `node_id`, `success`, `output`, `duration_ms`,
and `error`. A task that ran and failed comes back with `success=False` and an
`error`; only transport and protocol problems raise `AetherMeshError`.

## Benchmarks

The comparison harness in [`bench/comparison`](../../bench/comparison) uses this
SDK, so the AetherMesh and Dask numbers come from the same driver:

```bash
python bench/comparison/compare.py --tasks 100 --workers 3
```
