# 05 · A web page that uses the mesh

A browser cannot open a raw TCP socket, so a page talks to a **bridge** — a
small server that holds the mesh connection and exposes exactly the two
endpoints this page needs.

```
browser  ──HTTP──▶  bridge (node)  ──client API──▶  controller  ──▶  agents
```

## Run it

```bash
# a mesh, if you have not got one
cd ../03-many-agents && ./run.sh 2 && cd -

node server.mjs
# open http://127.0.0.1:8080
```

Pick a file, press **Hash it**. The page shows the digest, which node ran the
task, and two timings. Press **Hash again** on the same file: the data is
already on that node, so nothing moves and the round trip drops.

```
{"dataId":"9e0a0e2d…","sizeBytes":1048576,"digest":"9e0a0e2d…",
 "nodeId":"0f49d768-…","taskMs":0.19,"totalMs":10.4}
```

## Why a bridge, and not the mesh directly

Because the bridge is where authentication belongs.

- The browser never sees `AETHERMESH_TOKEN`; the bridge holds it.
- The page cannot choose which module runs. `/api/hash` names the task itself
  and passes only data — a page that could pick the task kind would be a page
  that could run any published WebAssembly module on your cluster.
- The bridge is where you would put a session check, a per-user quota, or an
  upload limit. There is a 32 MiB body cap in there already.

Put the same shape in front of any web framework: Express, Fastify, Next.js
route handlers, or the Python SDK behind FastAPI. The mesh does not care.

## Deploying this for real

- Terminate TLS at the bridge (or behind a reverse proxy) — the page is HTTP
  only because it is bound to localhost.
- Give the bridge its own mesh token, so it can be revoked without touching the
  agents: see [`08-secure-mesh`](../08-secure-mesh).
- Keep one long-lived `AetherMesh` connection per process, as `server.mjs` does.
  Connecting per request works, but it pays a handshake every time.
