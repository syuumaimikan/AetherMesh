# 16 · A nightly job that survives its own failure

Batch work has a shape: ingest, transform, roll up. It also has a failure mode
— something breaks at 3am, and the rerun at 9am repeats four hours of work that
had already succeeded.

```
        ┌─▶ transform ─┐
ingest ─┼─▶ transform ─┼─▶ rollup        (rollup only on role=rollup)
        └─▶ transform ─┘
```

## Run it

The controller needs somewhere to record what finished:

```bash
aether-controller --config controller.toml
```

`controller.toml` needs one line — see [`../controller.toml`](../controller.toml)
for the documented version:

```toml
checkpoint_path = "./nightly.jsonl"
```

Then, with a few plain agents up:

```bash
python pipeline.py
```

```
3 node(s), 0 of them able to run the rollup

run 'nightly' stopped: step 4 could not be placed: no node available for task 40b6d121…

Whatever finished before this is recorded. Start a node that
can run the rollup and run this again:

  aether-agent --controller 127.0.0.1:7000 \
      --identity-path ./rollup.id --label role=rollup
```

The machine that owns the warehouse credentials was not in the mesh. Start it:

```bash
aether-agent --controller 127.0.0.1:7000 --identity-path ./rollup.id --label role=rollup
```

And run **the same command** again:

```
4 node(s), 1 of them able to run the rollup

run 'nightly': success=True
  resumed  [0, 1, 2, 3]   (finished by an earlier run, not run again)
  ran      [4]
  skipped  []
    [ok ] stage 4 on 4af2fcf3   0.0 ms

  32 bytes moved this run (0.00 x the 4 MiB ingest)
```

Four stages resumed, one ran. The 4 MiB ingest was not repeated and was not
re-sent, because it is still sitting on the node that produced it.

## Why the fix is operational, not a code change

Notice what was repaired: a **node**, not the workflow. That is deliberate, and
it is the sharp edge of this feature.

A run name is recorded against a fingerprint of the workflow — every step's
kind, payload, inputs and dependencies. Change any of them and resuming under
the same name is *refused*, because skipping stage 3 on the strength of some
other graph's stage 3 is the one failure here that produces a confident wrong
answer instead of an error.

So: a broken machine, a missing label, a node that ran out of disk — resume.
A repaired *pipeline* is a new pipeline, and wants a new run name.

## The 32 bytes

Three transforms, one rollup that reads all three, and only 32 bytes crossed
the wire. The transforms are the same work over the same input, so they produce
byte-identical output, so they have the same content address — three inputs,
one dataset, one transfer. Nothing special-cased that; it falls out of
addressing data by its hash.

## Running it for real

- **The journal is not a log.** It holds step numbers and output ids, never
  outputs, and a step is only skipped if its output is still on a node. Lose
  the mesh and the next run does everything again, correctly.
- **Use one run name per logical job**, not per attempt: `nightly`, not
  `nightly-2026-08-18-attempt-2`. A fresh name every time can never resume.
- **A cron entry can just rerun this.** The second attempt is cheap when most
  of it already finished, and identical to the first when none of it did.
