# aether-tui

A terminal dashboard for a running AetherMesh controller: what the mesh is
moving, what it did not have to move, and which node holds what — plus a way to
send a task and watch where it lands.

```bash
cargo install --path crates/aether-tui
aether-tui --controller 127.0.0.1:7100
```

```
 AetherMesh ● live  127.0.0.1:7100  every 1.00s
┌ Throughput ────────────────────────────┐┌ Not moved ─────────────────────────┐┌ Mesh ──────────────────────────────┐
│7.3 MiB/s   peak 7.3 MiB/s              ││compressed away   1020.0 KiB        ││nodes             2/2 connected     │
│18.0 MiB on the wire so far             ││ratio             0.948             ││datasets          26 · 41.0 MiB     │
│ █                                      ││transfers skipped 2                 ││tasks ok          9                 │
│ █                                      ││chunks skipped    3                 ││tasks failed      0                 │
│ █                                      ││retries           0                 ││evicted           0                 │
└────────────────────────────────────────┘└────────────────────────────────────┘└────────────────────────────────────┘
┌ Nodes (2) ─────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│   host                     id        cpu    mem    rtt       link         holds            labels                  │
│●  syuum                    73ef9fd1  24%    80%    —         —            2 · 5.0 MiB      kind=gpu                │
│●  syuum                    824d28e0  24%    80%    —         —            —                kind=cpu region=eu-west │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
 q quit   s send a task   ↑↓ node   +/- poll rate   r refresh   ? help
```

That is a real frame from a real mesh, not a mock-up.

## What the panels mean

**Throughput** — bytes actually written to sockets, per second. Derived from a
cumulative counter, so the first sample shows nothing: one reading of a total is
not a measurement of a rate.

**Not moved** — the point of the project. `compressed away` is what compression
kept off the wire. `transfers skipped` counts whole datasets a node already had.
`chunks skipped` counts pieces deduplicated against data it already held. A high
number here is the mesh doing its job.

**Nodes** — the `holds` column is the one worth watching: work reading those
datasets costs no transfer, which is the decision the scheduler makes on every
task. A node marked `○` is registered but not reachable — the registry keeps it
until its heartbeat times out, deliberately, because one late heartbeat is not
a death.

**Recent tasks** — what the *mesh* finished, whoever asked for it. The panel
beside it is only what this window did, so a task submitted from an SDK, a
script, or another terminal appears here and nowhere else. That is the question
this panel exists for: "I ran something — did it work, and where?"

The last column is the front of the output, not the output: results stay on the
node that produced them, and a preview is for recognising your task rather than
reading a dataset through the control plane. Anything unprintable becomes `.`,
because a task's output is arbitrary bytes and a terminal would happily act on
the escape sequences in it. A watcher's screen is not somewhere a task gets to
write.

## Keys

| | |
|---|---|
| `q`, `Esc` | quit |
| `s` | send a task — pre-fills a constraint from the selected node |
| `↑` `↓`, `k` `j` | move between nodes |
| `+` `-` | poll faster or slower (0.25 s – 10 s) |
| `r` | poll now |
| `?`, `h` | what the columns mean |

In the send form: `Tab` moves between fields, `Enter` sends, `Esc` cancels.
`cpu` takes an iteration count and the form encodes it, so typing `5000000`
works rather than returning "expects an 8 byte iteration count".

## Connecting

```bash
aether-tui --controller mesh.example.com:7100   # AETHERMESH_TOKEN is read from the environment
aether-tui --token s3cret --poll-secs 0.5
```

A controller that goes away is reconnected to every two seconds. The last known
numbers stay on screen while that happens: *the mesh went quiet* and *every node
left* are different emergencies and must not look the same.

## What it cannot do yet

Draining a node, cancelling a task, and changing scheduler weights are not here,
because none of them exist to be controlled yet — placement is immediate and
first-come, first-served, and the weights are fixed at startup. The dashboard
will grow those controls when there is something behind them.

The task history is the last 64 and lives in the controller's memory, so it is
a debugging aid rather than a log: restart the controller and it is gone. That
is deliberate — a control plane that remembers every task it ever ran is a
memory leak with a nice interface.

TLS is not wired up either: the dashboard speaks plaintext to the client API.
Run it over a management network or an SSH tunnel until that lands.
