/**
 * A web service backed by the mesh, shaped like one you would actually deploy.
 *
 *   POOL_SIZE=8 node server.mjs
 *
 * What this adds over 05-web-app, which is deliberately the smallest possible
 * bridge:
 *
 *   - a pool of mesh connections, because one connection is one queue
 *   - interactive requests jump the queue ahead of batch ones
 *   - mesh failures map onto HTTP status codes a caller can act on
 *   - /healthz says whether the mesh can actually do work, not whether this
 *     process is running
 *
 * The browser still never chooses which code runs. It chooses data; the task
 * kinds are fixed here.
 */

import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

import { hostname } from "node:os";

import { MeshPool } from "./pool.mjs";

const HERE = dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.PORT ?? 8081);
const POOL_SIZE = Number(process.env.POOL_SIZE ?? 8);
const MESH_HOST = process.env.AETHERMESH_HOST ?? "127.0.0.1";
const MESH_PORT = Number(process.env.AETHERMESH_PORT ?? 7100);

const pool = await MeshPool.connect(
  { host: MESH_HOST, port: MESH_PORT, token: process.env.AETHERMESH_TOKEN },
  POOL_SIZE,
);
console.log(`pool of ${pool.size} connections ready on port ${PORT}`);

/**
 * Which machine a node id belongs to.
 *
 * The mesh answers in node ids, which are stable and unreadable. A person
 * looking at "where did my request go" wants a hostname, so the bridge keeps
 * a short-lived map of one to the other rather than making the page guess.
 */
let nodeNames = new Map();
let namesFetchedAt = 0;

async function nameOf(nodeId) {
  if (Date.now() - namesFetchedAt > 5000) {
    const nodes = await pool.use((mesh) => mesh.nodes());
    nodeNames = new Map(nodes.map((node) => [node.nodeId, node]));
    namesFetchedAt = Date.now();
  }
  const node = nodeNames.get(nodeId);
  return node ? `${node.hostname} (${node.address})` : "unknown node";
}

/**
 * Every place the request stopped on its way to being answered.
 *
 * This is the whole question "where is my work received": the browser reaches
 * this process, this process reaches the controller, the controller hands the
 * task to whichever node it chose, and that node is where the code actually
 * ran. Nothing here is inferred — the node id comes back with the result.
 */
async function pathOf(result, totalMs) {
  return [
    { hop: "browser", where: "your machine", note: "picked the data" },
    {
      hop: "bridge",
      where: `${hostname()}:${PORT} (this node process, pid ${process.pid})`,
      note: `held the mesh connection, ${totalMs.toFixed(1)} ms round trip`,
    },
    {
      hop: "controller",
      where: `${MESH_HOST}:${MESH_PORT}`,
      note: "chose the node and moved the data if it had to",
    },
    {
      hop: "agent",
      where: await nameOf(result.nodeId),
      note: `ran it in ${result.durationMs.toFixed(2)} ms — this is where your work executed`,
    },
  ];
}

function json(response, status, body) {
  const payload = JSON.stringify(body);
  response.writeHead(status, {
    "content-type": "application/json",
    "content-length": Buffer.byteLength(payload),
  });
  response.end(payload);
}

async function readBody(request, limit = 64 * 1024 * 1024) {
  const chunks = [];
  let size = 0;
  for await (const chunk of request) {
    size += chunk.length;
    if (size > limit) throw Object.assign(new Error("body too large"), { status: 413 });
    chunks.push(chunk);
  }
  return Buffer.concat(chunks);
}

/**
 * Turns a mesh outcome into an HTTP answer.
 *
 * The distinction that matters: a task that *ran* and failed is the caller's
 * problem (422 — the work was wrong), while a task that could not be placed is
 * ours (503 — come back in a moment). Collapsing both into 500 makes a client
 * retry the one thing retrying cannot fix.
 */
function statusFor(error) {
  const text = String(error?.message ?? error);
  if (text.includes("no node available")) return 503;
  if (text.includes("queue")) return 503;
  if (error?.status) return error.status;
  return 500;
}

const routes = {
  /** Liveness of the *mesh*, not of this process. */
  async "GET /healthz"(_request, response) {
    const nodes = await pool.use((mesh) => mesh.nodes());
    const connected = nodes.filter((node) => node.connected).length;
    json(response, connected > 0 ? 200 : 503, {
      nodes: nodes.length,
      connected,
      poolIdle: pool.idle,
    });
  },

  async "GET /api/nodes"(_request, response) {
    json(response, 200, { nodes: await pool.use((mesh) => mesh.nodes()) });
  },

  /** What the mesh has moved and saved, for a dashboard. */
  async "GET /api/stats"(_request, response) {
    json(response, 200, await pool.use((mesh) => mesh.stats()));
  },

  /** The last few tasks the whole mesh ran — including other clients'. */
  async "GET /api/recent"(_request, response) {
    json(response, 200, { tasks: await pool.use((mesh) => mesh.recent(20)) });
  },

  /**
   * Hashes an upload. Published once; a repeat of the same bytes finds them
   * already on a node and moves nothing.
   */
  async "POST /api/hash"(request, response) {
    const body = await readBody(request);
    const started = performance.now();

    const outcome = await pool.use(async (mesh) => {
      const published = await mesh.publish(body);
      const result = await mesh.run("hash", new Uint8Array(), [published.dataId]);
      return { published, result };
    });

    const totalMs = performance.now() - started;
    json(response, outcome.result.success ? 200 : 422, {
      dataId: outcome.published.dataId,
      sizeBytes: outcome.published.sizeBytes,
      digest: Buffer.from(outcome.result.output).toString("hex"),
      nodeId: outcome.result.nodeId,
      taskMs: outcome.result.durationMs,
      totalMs,
      path: await pathOf(outcome.result, totalMs),
      error: outcome.result.error,
    });
  },

  /**
   * Work with a knob on it, so the difference between an interactive request
   * and a batch one is visible under load.
   *
   *   POST /api/work?iterations=20000000&batch=1
   */
  async "POST /api/work"(request, response) {
    const url = new URL(request.url, "http://localhost");
    const iterations = BigInt(url.searchParams.get("iterations") ?? "5000000");
    const batch = url.searchParams.get("batch") === "1";

    const payload = new Uint8Array(8);
    new DataView(payload.buffer).setBigUint64(0, iterations, true);

    const started = performance.now();
    const result = await pool.use((mesh) =>
      // A person waiting on a page beats a nightly job. Without this the queue
      // is first-come, first-served and the page waits behind the batch.
      mesh.run("cpu", payload, [], [], batch ? "background" : "high"),
    );
    const totalMs = performance.now() - started;

    json(response, result.success ? 200 : 422, {
      nodeId: result.nodeId,
      output: Buffer.from(result.output).toString("hex"),
      taskMs: result.durationMs,
      totalMs,
      priority: batch ? "background" : "high",
      path: await pathOf(result, totalMs),
      error: result.error,
    });
  },

  async "GET /"(_request, response) {
    const page = await readFile(join(HERE, "index.html"));
    response.writeHead(200, { "content-type": "text/html; charset=utf-8" });
    response.end(page);
  },
};

const server = createServer(async (request, response) => {
  const path = new URL(request.url, "http://localhost").pathname;
  const route = routes[`${request.method} ${path}`];

  if (!route) {
    json(response, 404, { error: "not found" });
    return;
  }

  try {
    await route(request, response);
  } catch (error) {
    json(response, statusFor(error), { error: String(error?.message ?? error) });
  }
});

server.listen(PORT, "127.0.0.1", () => console.log(`open http://127.0.0.1:${PORT}`));

for (const signal of ["SIGINT", "SIGTERM"]) {
  process.on(signal, () => {
    pool.close();
    server.close(() => process.exit(0));
  });
}
