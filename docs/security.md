# Security

What AetherMesh defends against, what it does not, and where to look when you
disagree with a decision made here.

## The shape of the system

Three kinds of peer, three different levels of trust:

| Peer | Reaches | Trusted to |
|---|---|---|
| Agent | controller :7000 | run tasks, hold data it was sent |
| Client | controller :7100 | publish data, submit tasks |
| Controller | both | place work, move data |

A node is not trusted to speak for another node. A client is not trusted to
choose what code exists — only to run modules that were published to the mesh.
A WebAssembly module is trusted with nothing at all.

## Identity

Every connection is bound to the identity it authenticated as, and message
bodies never override it. This is the rule that most of the code follows
mechanically:

- **Heartbeats** update the metrics of the connection's registered node, not
  the node named in the message. Spoofing another node's CPU figure would steer
  the scheduler, and steering the scheduler moves other people's data.
- **Task results** are accepted only for the node that reported them.
- **Data channels** — extra connections an agent opens for bulk transfer —
  require a per-registration channel token. The controller issues it in
  `RegisterAccepted`; only the agent that registered ever sees it. Without this,
  any holder of the shared mesh token could attach a channel in another node's
  name and receive that node's chunks.
- **Node identity** persists in a file, so a restarted agent is the same node
  rather than a new one.

## Credentials

| Credential | Scope | Revoke by |
|---|---|---|
| Shared mesh token | whole mesh | rotating it everywhere |
| Per-node token (`[node_tokens]`) | one node | deleting one line |
| Client certificate | one machine | removing it from the CA you trust |
| Channel token | one registration | automatic, expires with the connection |

Token comparison is constant-time, and every candidate is compared, so neither
the answer nor its timing says which token was close.

Tokens are bearer credentials: over plaintext they are readable by anyone on
the path. The controller says so at startup when authentication is on and TLS
is not.

## Transport

TLS is behind the `tls` feature and covers both listeners. Setting
`tls_client_ca_path` turns on mutual TLS, which moves the first rejection
earlier — a peer without a certificate never gets to present a token.

There is no "accept any certificate" switch. A deployment that wants
self-signed certificates distributes its own CA; that is the supported path,
and it is what `generate-cert --with-ca` produces.

## The task sandbox

A WebAssembly module gets memory, an input buffer, and a fuel budget:

| Limit | Default |
|---|---|
| Fuel | 100,000,000 units (~1 per instruction) |
| Memory | 64 MiB |
| Output | 64 MiB |

By default it imports nothing but the ability to read the datasets its task
declared. Everything else is a grant the *operator* makes, per agent, through
the environment:

| Capability | Grant | Cost of granting it |
|---|---|---|
| `log` | `AETHERMESH_WASM_LOG=1` | module text reaches your logs |
| `now_unix_millis` | `AETHERMESH_WASM_CLOCK=1` | a side channel: a module can tell how long it has run |
| `random` | `AETHERMESH_WASM_RANDOM=1` | tasks stop being deterministic, so a retry may differ |
| `file_size` / `file_read` | `AETHERMESH_WASM_READ_DIR=/srv/data` | read-only, one directory, resolved and re-checked against the root so `..` and symlinks cannot escape |

A module that imports something it was not granted fails to instantiate. It
does not get a stub that quietly returns zero, because silence is how a
sandbox escape gets missed.

**`random` draws from the operating system's CSPRNG** (`getrandom`), so its
bytes are suitable for keys and nonces. A module cannot see what its caller
does with the buffer it filled, so the weaker generator this used to have was
a trap: it looked random and was not. If the OS refuses entropy the call
returns `-2` rather than a buffer of zeroes.

## The telemetry endpoint

`--metrics-listen` serves `/metrics` and `/healthz` over plain HTTP with **no
authentication**. It is off unless you ask for it, and it is built on the
assumption that it will eventually end up somewhere more convenient than safe:

- Counters and aggregate gauges only. No hostnames, no node ids, no addresses,
  no labels, no task payloads. A per-node metric label would turn an open port
  into an inventory of your network, so there are none.
- Read-only. `GET` and `HEAD`; everything else is a 405.
- Bounded reads, so a client that never sends a newline is disconnected rather
  than accumulated.

Bind it to localhost or a management interface. If you need it authenticated,
put it behind whatever already fronts your other metrics endpoints.

## What is deliberately out of scope

- **Malicious modules exhausting CPU.** Fuel bounds a single task; a client
  submitting a thousand of them is a quota problem, and there is no quota yet.
- **Confidentiality between tasks on one node.** Tasks share a node's data
  store. Two tenants who must not see each other's data need two meshes.
- **Protecting the controller from its operator.** Anything the controller can
  read, the controller's host can read.
- **Side channels between a module and the host.** Timing, cache, and memory
  pressure are not addressed.

## Dependency audit

`cargo audit` runs clean. Two crates carry unmaintained advisories and are
accepted for now, with reasons:

| Crate | Why it stays |
|---|---|
| `bincode` | The agent wire format. Replacing it is a protocol break; no known vulnerability, only a maintenance warning. |
| `rustls-pemfile` | PEM parsing for TLS setup. `rustls` has since absorbed this; migrating is a small, planned change. |

## Reporting something

Open a GitHub security advisory rather than a public issue. Findings that
change the threat model above are the most valuable kind — the model, not just
the bug, is what wants fixing.

## Known limits of this document

No third-party review has been done. What is written here is the design and the
tests that hold it in place: `crates/aether-agent/tests/security.rs` and
`crates/aether-agent/tests/data_channels.rs` are where the refusals are pinned
down, and they are the first thing to read if you doubt a claim on this page.
