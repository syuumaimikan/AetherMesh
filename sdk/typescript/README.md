# @aethermesh/client

TypeScript / JavaScript client for [AetherMesh](../../README.md): publish data
once, run tasks — including WebAssembly modules — across a mesh of machines.

Requires Node 20+ (Node 22.6+ or 24 runs the `.ts` sources directly; older
versions need a transpile step). No runtime dependencies.

## Start a mesh

```bash
cargo run -p aether-controller            # agent port 7000, client API port 7100
cargo run -p aether-agent                 # on every machine that should do work
```

## Use it

```ts
import { AetherMesh } from "@aethermesh/client";

const mesh = await AetherMesh.connect({ host: "127.0.0.1", port: 7100 });

// Published once; the mesh moves it only to nodes that actually need it.
const dataset = await mesh.publish(new Uint8Array(8 * 1024 * 1024).fill(7));

const result = await mesh.run("hash", new TextEncoder().encode("seed"), [dataset.dataId]);
console.log(Buffer.from(result.output).toString("hex"), `${result.durationMs.toFixed(1)} ms`);

mesh.close();
```

### WebAssembly tasks

```ts
const module = await mesh.publishFile("uppercase.wasm");
const result = await mesh.runWasm(module.dataId, new TextEncoder().encode("hello"));
new TextDecoder().decode(result.output); // "HELLO"
```

Building a module from TypeScript, Rust, or Go: [`docs/wasm-tasks.md`](../../docs/wasm-tasks.md).

### Authentication and TLS

```ts
const mesh = await AetherMesh.connect({
  host: "mesh.example.com",
  port: 7100,
  token: process.env.AETHERMESH_TOKEN,
  tlsCaPath: "cert.pem",
});
```

## API

| Method | Returns |
|---|---|
| `AetherMesh.connect(options)` | a connected client |
| `mesh.publish(bytes)` | `{ dataId, sizeBytes }` |
| `mesh.publishFile(path)` | same, reading from disk |
| `mesh.run(kind, payload?, inputs?)` | `TaskResult` — built-in `echo`, `hash`, `cpu` |
| `mesh.runWasm(moduleId, payload?, inputs?)` | `TaskResult` |
| `mesh.nodes()` | `NodeSummary[]` |
| `mesh.close()` | — |

A `TaskResult` carries `{ taskId, nodeId, success, output, durationMs, error? }`.
A task that ran and failed comes back with `success: false` and an `error`; only
transport and protocol problems throw.

## The wire protocol

Four bytes of big-endian length, then one JSON object, both directions. The
whole client protocol is that plus five message types — see
[`crates/aether-controller/src/client.rs`](../../crates/aether-controller/src/client.rs)
if you want to write an SDK for another language.

```json
{"type":"hello","token":"…"}      → {"type":"welcome","protocol":1}
{"type":"publish","data":"base64"} → {"type":"published","data_id":"…","size_bytes":42}
{"type":"submit","kind":"wasm","payload":"base64","module":"…","inputs":[]}
                                   → {"type":"result","success":true,"output":"base64",…}
{"type":"nodes"}                   → {"type":"nodes","nodes":[…]}
```

## Examples

```bash
node examples/hash.ts
node examples/wasm.ts uppercase.wasm "hello from typescript"
```
