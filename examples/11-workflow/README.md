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

## Why a workflow uses one node

Measured, then measured again with the placement weights turned against it:

```
default weights (locality = 1.0)     placement: 0->9bb19595 1->9bb19595 2->9bb19595 3->9bb19595
locality = 0.0, transfer = 0.0       placement: 0->b821ad5e 1->b821ad5e 2->b821ad5e 3->b821ad5e
```

Turning locality off changes nothing. So the concentration is **not** the
locality bonus doing its job, which is what it looks like. Two other things
cause it:

1. **Steps are dispatched one at a time.** `run_workflow` awaits each step
   before starting the next, so exactly one task is ever in flight. Spreading
   independent steps across nodes cannot save wall clock when they were never
   going to overlap. This is a real limitation, not a tuning choice.
2. **Load metrics lag by a heartbeat.** A node that has just been given work
   does not look busier until it reports again, so on a homogeneous mesh every
   node scores identically and the deterministic tie-break picks the same one.

Locality weighting is still what keeps a *chain* still — there the data
genuinely is on one node and moving it would cost. But for independent
branches, the reason they do not spread is the two above.

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
