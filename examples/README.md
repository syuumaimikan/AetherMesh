<img src="../assets/logo.svg" width="72" align="right" alt="">

# Examples

Each folder is a thing you can run, in the order a person actually learns this:
one terminal, then several, then several machines, then a browser.

| Example | What it shows | Needs |
|---|---|---|
| [`01-one-terminal`](01-one-terminal) | A whole mesh in a single command | Rust |
| [`02-two-terminals`](02-two-terminals) | Controller and agent as separate processes | Rust |
| [`03-many-agents`](03-many-agents) | Several agents on one machine, work spread across them | Rust |
| [`04-two-devices`](04-two-devices) | A laptop and a Raspberry Pi in one mesh | Two machines |
| [`05-web-app`](05-web-app) | A browser page submitting work through a small bridge | Node 20+ |
| [`06-python-pipeline`](06-python-pipeline) | Publish once, run many tasks over the same data | Python 3.10+ |
| [`07-wasm-task`](07-wasm-task) | A task written in another language, run sandboxed | Rust |
| [`08-secure-mesh`](08-secure-mesh) | TLS, tokens, and mutual TLS end to end | Rust |

## The 30-second version

```bash
cargo run -p aether-controller &     # control plane, ports 7000 and 7100
cargo run -p aether-agent &          # one worker
node sdk/typescript/examples/hash.ts # submit work
```

## What every example assumes

- You built the binaries once: `cargo build --release -p aether-controller -p aether-agent`.
- Ports 7000 (agents) and 7100 (clients) are free, or you pass different ones.
- Nothing is exposed to the internet. [`08-secure-mesh`](08-secure-mesh) is the
  one that turns on TLS and tokens, and it is the one to copy from before any
  of this leaves your LAN.
