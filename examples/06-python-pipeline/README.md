# 06 · Publish once, run many

The shape most real work takes: one dataset, many passes over it.

```bash
python pipeline.py --mb 32 --tasks 10
```

```
2 node(s): syuum, syuum
published 32 MiB in 240 ms as e6f67b5e96c5aeb4…
  task   0: 738772b8d416cbcd… on 4f3cb68b in   69.4 ms
  task   1: 041af6163aa8209d… on 4f3cb68b in    6.7 ms
  task   2: 677f6472b8973a8f… on 4f3cb68b in    6.7 ms
  task   9: 0391c0086f21c80a… on 4f3cb68b in    6.7 ms

first task :    69.4 ms   (includes moving the data)
median     :     6.7 ms   (data already there)
total      :   129.8 ms for 10 tasks
```

The first task pays 69 ms to put 32 MiB on a node. The other nine pay 6.7 ms,
because the data is already there and the scheduler keeps sending work to it.
That ten-to-one gap is the entire product, on one line of output.

## Using it from your own code

```python
from aethermesh import AetherMesh

with AetherMesh.connect(port=7100) as mesh:
    data = mesh.publish(open("features.bin", "rb").read())

    for window in range(24):
        result = mesh.run("hash", str(window).encode(), inputs=[data.data_id])
        print(result.output.hex(), result.node_id[:8])
```

`publish` is separate from `run` for exactly this reason. Hand the same
`data_id` to as many tasks as you like; the mesh works out which nodes already
have it and which need a copy.

## Fitting it into what you already use

The SDK has no dependencies and returns plain `bytes`, so it composes with
whatever is already in the process:

```python
import numpy as np

array = np.load("frames.npy")
data = mesh.publish(array.tobytes())          # numpy → mesh
digest = mesh.run("hash", b"", inputs=[data.data_id]).output
```

```python
# FastAPI: one connection for the process, not one per request
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app):
    app.state.mesh = AetherMesh.connect(port=7100)
    yield
    app.state.mesh.close()
```

For pandas, torch, or anything else holding a buffer: give `publish` the bytes,
keep the `data_id`, and let the mesh decide where the work goes.
