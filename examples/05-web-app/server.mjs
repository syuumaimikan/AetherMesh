/**
 * A browser cannot open a raw TCP socket, so this is the smallest honest
 * bridge: an HTTP server that serves one page and forwards its requests to the
 * mesh over the client API.
 *
 *   node server.mjs
 *   open http://127.0.0.1:8080
 *
 * The bridge is where authentication belongs. The browser never sees the mesh
 * token, and the mesh never sees the browser: exactly two endpoints are
 * exposed, and neither of them lets a page choose which module to run.
 */

import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

import { AetherMesh } from "../../sdk/typescript/src/index.ts";

const HERE = dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.PORT ?? 8080);

const mesh = await AetherMesh.connect({
  host: process.env.AETHERMESH_HOST ?? "127.0.0.1",
  port: Number(process.env.AETHERMESH_PORT ?? 7100),
  token: process.env.AETHERMESH_TOKEN,
});
console.log(`bridge connected to the mesh; ${(await mesh.nodes()).length} node(s)`);

/** Reads a whole request body, refusing anything absurd. */
async function readBody(request, limit = 32 * 1024 * 1024) {
  const chunks = [];
  let size = 0;
  for await (const chunk of request) {
    size += chunk.length;
    if (size > limit) throw new Error("request body too large");
    chunks.push(chunk);
  }
  return Buffer.concat(chunks);
}

function json(response, status, body) {
  const payload = JSON.stringify(body);
  response.writeHead(status, {
    "content-type": "application/json",
    "content-length": Buffer.byteLength(payload),
  });
  response.end(payload);
}

const server = createServer(async (request, response) => {
  try {
    if (request.method === "GET" && (request.url === "/" || request.url === "/index.html")) {
      const page = await readFile(join(HERE, "index.html"));
      response.writeHead(200, { "content-type": "text/html; charset=utf-8" });
      response.end(page);
      return;
    }

    if (request.method === "GET" && request.url === "/api/nodes") {
      json(response, 200, { nodes: await mesh.nodes() });
      return;
    }

    // Hash whatever the page uploaded. The task kind is fixed here on purpose:
    // the browser picks the data, never the code.
    if (request.method === "POST" && request.url === "/api/hash") {
      const body = await readBody(request);
      const started = performance.now();
      const published = await mesh.publish(body);
      const result = await mesh.run("hash", new Uint8Array(), [published.dataId]);

      json(response, result.success ? 200 : 500, {
        dataId: published.dataId,
        sizeBytes: published.sizeBytes,
        digest: Buffer.from(result.output).toString("hex"),
        nodeId: result.nodeId,
        taskMs: result.durationMs,
        totalMs: performance.now() - started,
        error: result.error,
      });
      return;
    }

    json(response, 404, { error: "not found" });
  } catch (error) {
    json(response, 500, { error: String(error?.message ?? error) });
  }
});

server.listen(PORT, "127.0.0.1", () => {
  console.log(`open http://127.0.0.1:${PORT}`);
});

for (const signal of ["SIGINT", "SIGTERM"]) {
  process.on(signal, () => {
    mesh.close();
    server.close(() => process.exit(0));
  });
}
