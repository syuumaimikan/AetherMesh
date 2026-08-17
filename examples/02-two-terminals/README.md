# 02 · Two terminals

The same mesh, split the way it actually runs: the control plane in one
process, a worker in another.

## Terminal 1 — the controller

```bash
RUST_LOG=info cargo run --release -p aether-controller
```

```
INFO aether_controller: controller listening addr=127.0.0.1:7000 auth=false tls=false
INFO aether_controller: client API listening client_addr=127.0.0.1:7100 tls=false
```

Two ports, on purpose:

| Port | Who connects | Protocol |
|---|---|---|
| 7000 | agents | binary, length-prefixed |
| 7100 | your programs and SDKs | JSON, length-prefixed |

## Terminal 2 — an agent

```bash
RUST_LOG=info cargo run --release -p aether-agent -- --controller 127.0.0.1:7000
```

Terminal 1 answers:

```
INFO aether_controller::server: node registered node_id=9c5e43a0-… hostname=your-machine
```

## Terminal 3 — send it work

```bash
node ../../sdk/typescript/examples/hash.ts
```

or, without Node:

```bash
python ../../sdk/python/examples/hash.py
```

## Try this

- **Stop the agent** (Ctrl-C). The controller logs `node disconnected`, and a
  submission now fails with "no node available" instead of hanging.
- **Start it again.** It keeps the same node id — that is the identity file
  under your data directory — so the controller sees one node returning, not a
  second machine appearing.
- **Watch a dataset move once.** Run the hash example twice; the second run
  transfers nothing, because the node already holds the data.

Windows PowerShell uses `$env:RUST_LOG='info'` on its own line instead of the
`RUST_LOG=info` prefix.
