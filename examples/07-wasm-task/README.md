# 07 · A task written in another language

Tasks run as WebAssembly, so the work can be written in TypeScript, Rust, Go,
C, or anything with a WASM target — and the node still never runs an
unsandboxed process.

## The shortest path: no toolchain at all

The repository ships a WAT module and an assembler, so you can see the whole
path before installing anything:

```bash
cargo run -p aether-wasm --example wat2wasm -- ../wasm/uppercase.wat uppercase.wasm
node ../../sdk/typescript/examples/wasm.ts uppercase.wasm "hello from typescript"
```

```
module 58e46fef2361318a… (233 bytes)
output: HELLO FROM TYPESCRIPT
ran on aebf4c04 in 2.02 ms
```

The module is published like any dataset: content-addressed, transferred to a
node once, and counted toward that node's locality score. A hundred tasks
sharing a 5 MB module move 5 MB, not 500 MB.

## The contract

Three exports, no imports:

| Export | Signature | Meaning |
|---|---|---|
| `memory` | — | linear memory the host reads and writes |
| `alloc` | `(i32) -> i32` | reserve `len` bytes, return the offset |
| `run` | `(i32, i32) -> i64` | run over `(ptr, len)`, return `ptr << 32 \| len` |

Per-language recipes — Rust, AssemblyScript, TinyGo, Javy — are in
[`docs/wasm-tasks.md`](../../docs/wasm-tasks.md).

## What a module may spend

| Limit | Default |
|---|---|
| Fuel | 100,000,000 units, roughly one per instruction |
| Memory | 64 MiB |
| Output | 64 MiB |

An endless loop costs one task, not the node. There is no filesystem, no
network, and no clock unless the operator grants them:

```bash
AETHERMESH_WASM_CLOCK=1 AETHERMESH_WASM_LOG=1 aether-agent --controller …
AETHERMESH_WASM_READ_DIR=/srv/models aether-agent --controller …
```

A module that imports something it was not granted fails to instantiate rather
than silently getting a stub — the failure is loud on purpose.

## Reading the task's data

A module can read the datasets the task declared, which is how a task works on
data it did not carry in its payload:

```wat
(import "aether" "input_count" (func $input_count (result i32)))
(import "aether" "input_len"   (func $input_len (param i32) (result i32)))
(import "aether" "input_read"  (func $input_read (param i32 i32 i32) (result i32)))
```

```ts
const model = await mesh.publish(await readFile("model.bin"));
const wasm  = await mesh.publishFile("infer.wasm");
await mesh.runWasm(wasm.dataId, input, [model.dataId]);   // model reaches the node once
```
