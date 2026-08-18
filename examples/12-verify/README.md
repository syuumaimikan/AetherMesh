# 12 · Checking the claims yourself

Every number in this repository's READMEs was measured, but measured by the
people who wrote the thing being measured. This checks the four load-bearing
claims against a mesh you started, and prints `PASS` or `FAIL` rather than a
paragraph.

No dependencies and no SDK: it speaks the client protocol directly, which is a
socket, a `u32`, and JSON. Python 3.10+.

## Run it

With a controller and at least one agent up (see
[`02-two-terminals`](../02-two-terminals)):

```bash
python verify.py nodes
python verify.py traffic
python verify.py workflow
```

```
4 tasks over one 4 MiB dataset:
  [OK  ] bytes moved: 4194304   (at most one copy per node, not one per task)
  [OK  ] transfers skipped: 3

  [OK  ] all steps succeeded: True
  [OK  ] intermediate bytes moved: 0
```

`resume` needs a controller started with `checkpoint_path` set (see
[`controller.toml`](../controller.toml)):

```bash
python verify.py resume nightly-check
```

```
  run 1: ran=[0, 1, 2] resumed=[]
  run 2: ran=[2] resumed=[0, 1]
  [OK  ] second run resumed the finished steps: [0, 1]
  [OK  ] second run only ran the failing step: [2]
  [OK  ] a different workflow under this name is refused: error
```

Point it at another machine with `--controller <host>:7100`, and add `--token`
if the mesh requires one.

## Reading a result you did not expect

**`traffic` reports 0 bytes moved.** Nothing was proved: the nodes were still
holding the dataset from an earlier run, so there was nothing to send. Restart
the controller and the agents and measure again. A benchmark result that looks
too good is a bug report about the benchmark — this repository has made that
mistake once already, and the note in `.dev-state` is about that day.

**`resume` reports `resumed=[]`.** Either the controller has no
`checkpoint_path`, or the agent was restarted between the two runs. A step is
skipped only when its output is still on a node, and an agent that restarted
lost what it was holding. That is the checkpoint being honest, not broken.

**Every task lands on one node.** Expected with locality on and a mesh that is
not busy: the first task pins the data, and after that no other node is
cheaper. Set `locality = 0.0` in the controller's `[scheduler_weights]` to see
the work spread out and the byte count go up.

## Why the payloads are random

An earlier version of this script published 4 MiB of zeros and reported 1 MiB
moved, which looked like a very good result and was not one: a zeroed dataset
is the same chunk repeated, so chunk dedup collapsed it. Random bytes measure
what this is supposed to measure.
