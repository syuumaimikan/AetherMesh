# 10 · Swapping the thread pool

Python code that fans work out is written against one interface: `submit`,
`map`, `as_completed`, `shutdown`. `MeshExecutor` is that interface with the
machines behind it replaced.

```python
from concurrent.futures import ThreadPoolExecutor
with ThreadPoolExecutor(max_workers=8) as pool:
    results = list(pool.map(local_hash, payloads))
```

```python
from aethermesh import MeshExecutor
with MeshExecutor.connect(port=7100, max_workers=8) as pool:
    results = list(pool.map(pool.builtin("hash"), payloads))
```

Everything in `concurrent.futures` works on the futures it returns —
`as_completed`, `wait`, timeouts, cancelling work that has not started.

## Run it

With a controller and at least one agent up (see
[`02-two-terminals`](../02-two-terminals)):

```bash
python fanout.py --tasks 8 --workers 4 --iterations 5000000
```

```
hashing 256 KiB x 8 tasks:
threads :      2.2 ms
mesh    :     19.9 ms
          hashing is cheap per byte, so on one machine the network wins.

CPU-bound, 5,000,000 iterations x 8 tasks:
threads :   1572.0 ms   (the GIL means these did not overlap)
mesh    :      7.1 ms   (220x)
          Most of that gap is a Python loop against a native task, not
          distribution. The distributed part is that the eight threads
          did not overlap and the eight mesh tasks did.

as_completed, in finishing order:
  task 1: 34fe26dff87bbcdf…
  task 2: 86e5a7e881704ae5…
  task 3: 5a172fcdb35cfb1b…
  task 0: 86bb2b521a10612d…
```

Read both halves. The first is the mesh losing: hashing 256 KiB is a few
hundred microseconds of work and several milliseconds of round trip, so
shipping it anywhere is a bad trade. The second is not a fair fight either —
most of that 220× is a Python `for` loop against native code, and saying
otherwise would be dishonest. The part that *is* about distribution is that the
threads took 1.5 s of wall clock to do 1.5 s of serial work, because the GIL
never let them overlap, while the mesh ran them at once.

The real case for this is [`06-python-pipeline`](../06-python-pipeline):
publish a dataset once, then run many tasks over it where it already lives.

## What it will not do

```python
pool.submit(my_python_function, payload=b"x")
```

```
TypeError: MeshExecutor runs mesh tasks, not Python callables. Use
pool.builtin('hash') or pool.module('task.wasm') to get one — the mesh never
ships executable Python to a node.
```

A node runs task names and WebAssembly modules, never pickled code. That is
most of the reason a machine is safe to volunteer to a mesh, so the executor
refuses loudly rather than quietly falling back to running the function here —
a pool that silently stops being distributed is worse than one that says no.

## Building tasks

```python
with MeshExecutor.connect(port=7100, max_workers=8) as pool:
    upper = pool.module("uppercase.wasm")        # publish once, run anywhere
    dataset = pool.publish(open("features.bin", "rb").read())

    task = upper.with_inputs(dataset).where("kind=gpu", "region=eu-west")
    for output in pool.map(task, windows):
        ...
```

`with_inputs` names datasets the task reads — the mesh moves them only to nodes
that do not already hold them. `where` restricts which nodes may run it at all;
see [`09-labeled-nodes`](../09-labeled-nodes).

## Sizing the pool

`max_workers` is also the number of connections held open, because one
connection matches replies to requests by order and cannot be shared. Four to
eight is usually right; more than the number of agents rarely helps.
