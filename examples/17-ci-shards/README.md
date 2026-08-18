# 17 · Sharding a suite, and jumping the queue

The everyday reason to reach for a mesh: something loops over N things and you
would like it to loop over them on N machines. A *shard* here is a slice of a
test suite, a dataset, a corpus — the mesh does not care what is inside, so
this example uses the `cpu` builtin and needs no suite of its own.

## Run it

```bash
python shards.py
```

```
2 node(s) in the mesh

24 shards, all normal priority
  shards        24, 0 failed
  wall          63 ms
  slowest shard 56 ms
  work          120 ms across 2 node(s) -> 1.9x
  spread        {'5e7e5194': 12, '8c2b4b03': 12}
```

12 and 12 across two nodes, 1.9× of a theoretical 2.0×. The `spread` line is
the one to watch when you try this on your own mesh: if every shard lands on
one node, the client is submitting them one at a time and the mesh never has a
second task to place anywhere else.

**A connection per worker.** The client protocol matches replies to requests in
order on a connection, so a shard waiting on a node holds up every shard queued
behind it on the same socket. `shards.py` opens one connection per worker; a
web service does the same thing with a pool ([`14-web-service`](../14-web-service)).

## Somebody pushes a fix while the nightly run is going

```bash
python shards.py --contended
```

This floods the mesh with background shards, waits for a real backlog, then
submits critical and ordinary shards **at the same moment** — because one run
on its own is a number with nothing to compare it to.

```
queue reached 104 deep
  192 background shards, slowest 520 ms

  submitted at the same moment, into the same queue:
    critical  mean     90 ms   slowest    223 ms
    normal    mean    211 ms   slowest    248 ms
```

2.3× less waiting for the shards that said they were urgent, against work they
arrived behind.

### You need a mesh small enough to have a queue

On a workstation with sixteen-core agents, 192 shards do not queue — they all
just run, and priority has nothing to reorder. The example measures the queue
depth rather than assuming one, and tells you when the run proved nothing:

```
No queue ever formed, so priority had nothing to reorder and this
run proves nothing.
```

To make a backlog on one machine, start agents that take one task at a time:

```bash
aether-agent --controller 127.0.0.1:7000 --identity-path ./a1.id --max-concurrent-tasks 1
```

The numbers above are from two such agents. That is not a trick to make the
feature look good — it is how you reproduce, on a laptop, the state a real
cluster is in whenever it is busy.

## What priority is and is not

It decides **who waits**, not who gets a bigger share. A critical task does not
run faster, get more cores, or preempt anything already running; it goes to the
front of the queue for the next free node. On an idle mesh it changes nothing
whatsoever, which is the correct behaviour and worth remembering before
labelling everything `critical`.

Ageing works against starvation from the other side: every 30 seconds a waiting
task climbs a level, so `background` means *later*, never *never*.

## Adapting this to a real suite

Replace the `cpu` task with a WASM module that runs your shard
([`07-wasm-task`](../07-wasm-task)), publish the suite once and pass its id as
an input so it moves to each node at most once
([`06-python-pipeline`](../06-python-pipeline)), and use constraints if some
shards need a particular machine ([`09-labeled-nodes`](../09-labeled-nodes)).
The scheduling shown here does not change.
