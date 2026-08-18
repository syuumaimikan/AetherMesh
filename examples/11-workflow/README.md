# 11 · Work that depends on other work

A single task runs where it is cheapest. A *workflow* is where that decision
compounds: if step B reads what step A produced, then running B where A ran
costs nothing to move, and running it anywhere else costs the whole
intermediate result.

## Run it

With a controller and **three** agents up (see
[`03-many-agents`](../03-many-agents)):

```bash
python chain.py
```

```
nodes: ['syuum 6e34d8cc', 'syuum 70ebda25', 'syuum 1cba9b2e']
success: True skipped: []
  step 0: ran on 70ebda25 in 0.9 ms
  step 1: ran on 70ebda25 in 1.7 ms
  step 2: ran on 70ebda25 in 0.0 ms

intermediate data moved: 0 bytes (0.00 x the 8 MiB payload)
all three steps on one node: True
```

Three nodes were available. All three steps ran on one of them, and the 8 MiB
that flowed through the chain crossed the wire **zero times**.

## How

There is no separate rule for this. A task's output is stored on the node that
produced it, and the controller records that in the same catalog it uses for
published datasets. When the next step declares that output as an input, the
ordinary locality score sees a node that already holds the data — so it wins,
and `ensure_inputs` finds nothing to send.

The mechanism that keeps a published dataset still is the one that keeps a
computed one still.

## Writing a workflow

```json
{"type": "workflow", "steps": [
  {"kind": "echo", "payload": "..."},
  {"kind": "hash", "depends_on": [0]},
  {"kind": "hash", "depends_on": [1]}
]}
```

`depends_on` holds indices into `steps`. Each dependency's output becomes an
input of the step that waits for it.

A diamond works the same way:

```json
[{"kind": "echo", "payload": "..."},
 {"kind": "hash", "depends_on": [0]},
 {"kind": "hash", "depends_on": [0]},
 {"kind": "hash", "depends_on": [1, 2]}]
```

Steps 1 and 2 are independent and step 3 waits for both, reading both.

In practice they will land on the *same* node, and not because of the locality
score — see below.

## Concentrate, or spread

Independent steps run at the same time. Whether they run on *different
machines* is a choice, and it is the trade-off this project exists to make
explicit.

One root, six independent branches, one join, on a six-node mesh:

| `[scheduler_weights]` | branches landed on | wall | overlap |
|---|---|---:|---:|
| `locality = 1.0` (default) | 1 node | 36 ms | 1.0x |
| `locality = 0.0` | 4 nodes | 13 ms | **2.7x** |

By default every branch follows the root's output to the node holding it, so
nothing moves and nothing overlaps. Turn locality off and the work spreads: 2.8
times faster, paid for by copying the intermediate result to each node that
took a branch.

Neither is right for everybody. Cheap branches over expensive data want the
default; expensive branches over cheap data want the other. What you should not
have to do is guess which one you are getting.

**This measurement used to say something different.** Before workflow steps ran
concurrently, turning locality off changed nothing at all — there was no second
task in flight to put anywhere. The knob existed and did nothing. It does
something now.

## When it goes wrong

A cycle, or a dependency on a step that does not exist, is **refused before
anything runs**:

```
Error: steps 0 -> 1 form a cycle; a workflow has to finish
```

Discovering that halfway through would leave work half-done on machines
somebody else owns.

A step that runs and fails stops the steps waiting on it, and they come back in
`skipped`:

```
success: False   skipped: [1, 2]
```

Running step 2 on step 1's output when step 1 failed would produce a confident
answer computed from nothing. Branches that do **not** depend on the failure
still run — a diamond with one bad arm should still tell you about the good one.
