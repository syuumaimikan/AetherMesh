# Security policy

## Reporting a vulnerability

Please report security problems privately, through
[GitHub's private vulnerability reporting](https://github.com/syuumaimikan/AetherMesh/security/advisories/new)
rather than a public issue.

Include what you need to make the problem reproducible: the version or commit,
the configuration (TLS on or off, tokens, which capabilities were granted), and
the steps. A proof of concept is welcome but not required — a clear description
of the flaw is enough to start.

Expect an acknowledgement within a few days. This is a small project without a
paid security team, so treat any timeline as best effort rather than a
commitment. If a report turns out to be valid, the fix and an advisory go out
together, and you get credit unless you ask otherwise.

## What is in scope

The [threat model](docs/security.md) is the authoritative version; briefly, a
report is in scope if it lets someone:

- register, impersonate, or evict a node they do not control
- read data, task payloads, or results belonging to another node
- escape the WebAssembly sandbox — reach the filesystem, the network, or the
  host process from inside a module
- bypass a granted capability's boundary, e.g. read outside the directory
  `read_dir` was pointed at
- defeat token or certificate checking on either listener

## What is out of scope

These are known and documented rather than overlooked:

- **Resource exhaustion.** There are no quotas yet. A client submitting a
  million tasks, or a module burning its full fuel budget, is expected to slow
  the mesh down.
- **Confidentiality between tasks on one node.** Tasks share a node's data
  store by design. Two tenants who must not see each other's data need two
  meshes.
- **A mesh run without TLS or tokens.** The defaults are open because the first
  run is on `127.0.0.1`. Exposing that configuration to a network is a
  deployment decision, and `docs/security.md` says so at the top.
- **Advisories in dependencies** that do not affect a code path AetherMesh
  actually reaches. `cargo audit` runs in CI; the two accepted warnings are
  listed with their reasoning in `docs/security.md`.

## Supported versions

Alpha: fixes land on `main`, and there is no back-porting to older tags yet.
