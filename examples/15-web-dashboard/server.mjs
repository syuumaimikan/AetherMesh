/**
 * The TUI, in a browser.
 *
 *   node server.mjs      # then open http://127.0.0.1:8082
 *
 * One connection to the mesh polls it, and every browser watching gets the
 * same reading over server-sent events. That is the shape worth copying: a
 * dashboard with fifty viewers should ask the controller once a second, not
 * fifty times a second. The mesh has work to do.
 *
 * SSE rather than WebSockets because this only goes one way. A browser
 * subscribes with six lines and no library, and it reconnects on its own.
 */

import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

import { AetherMesh } from "../../sdk/typescript/src/index.ts";

const HERE = dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.PORT ?? 8082);
const EVERY_MS = Number(process.env.POLL_MS ?? 1000);

const mesh = await AetherMesh.connect({
  host: process.env.AETHERMESH_HOST ?? "127.0.0.1",
  port: Number(process.env.AETHERMESH_PORT ?? 7100),
  token: process.env.AETHERMESH_TOKEN,
});

/** Everyone currently watching. */
const viewers = new Set();

/** The last reading, so a browser that just connected sees something at once. */
let latest = null;

async function poll() {
  try {
    const [stats, nodes, recent] = await Promise.all([
      mesh.stats(),
      mesh.nodes(),
      mesh.recent(12),
    ]);
    latest = { at: Date.now(), stats, nodes, recent };
  } catch (error) {
    // A dashboard that goes blank when the controller hiccups is worse than
    // one showing the last good reading with a warning on it.
    latest = { ...latest, at: Date.now(), error: String(error?.message ?? error) };
  }

  const frame = `data: ${JSON.stringify(latest)}\n\n`;
  for (const viewer of viewers) viewer.write(frame);
}

setInterval(poll, EVERY_MS);
await poll();

const server = createServer(async (request, response) => {
  if (request.url === "/events") {
    response.writeHead(200, {
      "content-type": "text/event-stream",
      "cache-control": "no-cache",
      connection: "keep-alive",
    });
    // Whoever just arrived should not wait a whole second for the first frame.
    if (latest) response.write(`data: ${JSON.stringify(latest)}\n\n`);

    viewers.add(response);
    request.on("close", () => viewers.delete(response));
    return;
  }

  const page = await readFile(join(HERE, "index.html"));
  response.writeHead(200, { "content-type": "text/html; charset=utf-8" });
  response.end(page);
});

server.listen(PORT, "127.0.0.1", () =>
  console.log(`open http://127.0.0.1:${PORT} — polling every ${EVERY_MS} ms`),
);

for (const signal of ["SIGINT", "SIGTERM"]) {
  process.on(signal, () => {
    mesh.close();
    server.close(() => process.exit(0));
  });
}
