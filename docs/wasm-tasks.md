# Writing tasks in other languages

AetherMesh runs tasks as **WebAssembly modules**. Anything that compiles to
WASM — TypeScript, Rust, Go, C, Zig, Python via a WASM interpreter — can be work
the mesh schedules, without the mesh ever running an unsandboxed process.

A module is published like any other dataset, so it is content-addressed,
deduplicated, and transferred to each node exactly once, however many tasks use
it.

## The contract

A task module exports three things and imports nothing:

| Export | Signature | Meaning |
|---|---|---|
| `memory` | — | linear memory the host reads and writes |
| `alloc` | `(i32) -> i32` | reserve `len` bytes, return the offset |
| `run` | `(i32, i32) -> i64` | run over `(ptr, len)`, return `ptr << 32 \| len` |

The host writes your input at `alloc(len)`, calls `run(ptr, len)`, and reads
back the slice the return value points at. Both halves are unsigned; a range
outside memory is rejected rather than trusted.

There are **no host functions**. No filesystem, no network, no clock, no
randomness. A module computes over the bytes it is given and returns bytes. That
is the whole interface, and it is why running someone else's module is a
bounded risk.

## What a module may spend

| Limit | Default | What happens |
|---|---|---|
| Fuel | 100,000,000 units (~1 per instruction) | task fails with "out of fuel" |
| Memory | 64 MiB | growth beyond it traps |
| Output | 64 MiB | oversized return is refused |

An endless loop costs one task, not the node.

## Rust

```rust
// cargo build --release --target wasm32-unknown-unknown
static mut BUFFER: [u8; 1 << 20] = [0; 1 << 20];

#[unsafe(no_mangle)]
pub extern "C" fn alloc(_len: i32) -> i32 {
    &raw const BUFFER as i32
}

#[unsafe(no_mangle)]
pub extern "C" fn run(ptr: i32, len: i32) -> i64 {
    let input = unsafe { std::slice::from_raw_parts_mut(ptr as *mut u8, len as usize) };
    input.make_ascii_uppercase();
    ((ptr as i64) << 32) | len as i64
}
```

## TypeScript / AssemblyScript

[AssemblyScript](https://www.assemblyscript.org) compiles a TypeScript subset
straight to WASM:

```ts
// assembly/index.ts — npx asc assembly/index.ts -o task.wasm --exportRuntime
export function alloc(len: i32): i32 {
  return changetype<i32>(new ArrayBuffer(len));
}

export function run(ptr: i32, len: i32): i64 {
  for (let i = 0; i < len; i++) {
    const byte = load<u8>(ptr + i);
    if (byte >= 97 && byte <= 122) store<u8>(ptr + i, byte - 32);
  }
  return (<i64>ptr << 32) | <i64>len;
}
```

For full JavaScript rather than a subset, [Javy](https://github.com/bytecodealliance/javy)
bundles QuickJS with your script into a single module; wrap its entry point in
the `alloc`/`run` exports above.

## Go

TinyGo emits small modules:

```go
//go:build tinygo
// tinygo build -o task.wasm -target=wasm-unknown ./main.go

package main

var buffer [1 << 20]byte

//go:wasmexport alloc
func alloc(length int32) int32 { return int32(uintptr(unsafe.Pointer(&buffer))) }

//go:wasmexport run
func run(ptr int32, length int32) int64 {
    input := unsafe.Slice((*byte)(unsafe.Pointer(uintptr(ptr))), length)
    for i, b := range input {
        if b >= 'a' && b <= 'z' {
            input[i] = b - 32
        }
    }
    return int64(ptr)<<32 | int64(length)
}

func main() {}
```

## No toolchain at all

The repository ships a WAT example and an assembler, so you can try the path
before installing anything:

```bash
cargo run -p aether-wasm --example wat2wasm -- examples/wasm/uppercase.wat uppercase.wasm
```

## Running one

From TypeScript:

```ts
const module = await mesh.publishFile("uppercase.wasm");
const result = await mesh.runWasm(module.dataId, new TextEncoder().encode("hello"));
console.log(new TextDecoder().decode(result.output)); // HELLO
```

From Rust:

```rust
let module = controller.publish(std::fs::read("uppercase.wasm")?);
let result = controller.submit(Task::wasm(module.id, b"hello".to_vec())).await?;
```

The module counts as a task input, so the scheduler prefers nodes that already
have it — a 5 MB module does not get re-sent for every task.

## Choosing an engine

| Build | Engine | When |
|---|---|---|
| `--features wasm` (default) | `wasmi`, pure-Rust interpreter | anywhere, including Raspberry Pi; no C toolchain |
| `--no-default-features --features wasm-jit` | `wasmtime`, Cranelift JIT | CPU-bound modules on x86-64/aarch64 servers |

Both enforce the same limits and expose the same ABI, so a module does not know
or care which one ran it.
