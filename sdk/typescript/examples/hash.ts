/**
 * Publishes a dataset once and hashes it from three tasks.
 *
 * Run a controller and at least one agent first, then:
 *   node examples/hash.ts
 */

import { AetherMesh } from "../src/index.ts";

const mesh = await AetherMesh.connect({
  host: process.env.AETHERMESH_HOST ?? "127.0.0.1",
  port: Number(process.env.AETHERMESH_PORT ?? 7100),
  token: process.env.AETHERMESH_TOKEN,
});

console.log(
  "nodes:",
  (await mesh.nodes()).map((node) => node.hostname),
);

// 4 MiB of repetitive data: published once, transferred once.
const dataset = new Uint8Array(4 * 1024 * 1024).fill(0xab);
const published = await mesh.publish(dataset);
console.log(
  `published ${published.sizeBytes} bytes as ${published.dataId.slice(0, 16)}…`,
);

for (let i = 0; i < 3; i++) {
  const result = await mesh.run("hash", new TextEncoder().encode("seed"), [
    published.dataId,
  ]);
  const digest = Buffer.from(result.output).toString("hex").slice(0, 16);
  console.log(
    `task ${i}: ${digest}… on ${result.nodeId.slice(0, 8)} in ${result.durationMs.toFixed(1)} ms`,
  );
}

mesh.close();
