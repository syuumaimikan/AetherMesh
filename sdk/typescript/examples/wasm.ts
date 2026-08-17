/**
 * Runs a WebAssembly task written in AssemblyScript-style WAT.
 *
 * The module is published like any other dataset, so it reaches each node once
 * and is then reused. Compile your own module from TypeScript with
 * AssemblyScript or Javy, or from Rust with `--target wasm32-unknown-unknown`;
 * it only has to export `memory`, `alloc(i32) -> i32`, and
 * `run(i32, i32) -> i64`.
 *
 *   node examples/wasm.ts [module.wasm]
 */

import { AetherMesh } from "../src/index.ts";

const mesh = await AetherMesh.connect({
  host: process.env.AETHERMESH_HOST ?? "127.0.0.1",
  port: Number(process.env.AETHERMESH_PORT ?? 7100),
  token: process.env.AETHERMESH_TOKEN,
});

const modulePath = process.argv[2];
if (!modulePath) {
  console.error("usage: node examples/wasm.ts <module.wasm>");
  console.error("see docs/wasm-tasks.md for how to build one");
  mesh.close();
  process.exit(1);
}

const module = await mesh.publishFile(modulePath);
console.log(`module ${module.dataId.slice(0, 16)}… (${module.sizeBytes} bytes)`);

const input = new TextEncoder().encode(process.argv[3] ?? "aethermesh");
const result = await mesh.runWasm(module.dataId, input);

if (!result.success) {
  console.error("task failed:", result.error);
  mesh.close();
  process.exit(1);
}

console.log("output:", new TextDecoder().decode(result.output));
console.log(`ran on ${result.nodeId.slice(0, 8)} in ${result.durationMs.toFixed(2)} ms`);

mesh.close();
