# 14 · A web service on the mesh

[`05-web-app`](../05-web-app) is the smallest bridge that works: one page, two
endpoints, one connection. This is the same idea shaped like something you
would deploy — and the difference that matters is not the extra endpoints, it
is the **pool**.

## Run it

```bash
cd ../03-many-agents && ./run.sh 3 && cd -
```

```bash
node server.mjs
```

Then open <http://127.0.0.1:8081>, or use the endpoints directly:

| | |
|---|---|
| `GET /healthz` | whether the *mesh* can do work, not whether this process is up |
| `GET /api/nodes` | who is in the mesh, what they hold |
| `GET /api/stats` | traffic, counters, queue |
| `GET /api/recent` | the last 20 tasks the whole mesh ran |
| `POST /api/hash` | hash an upload |
| `POST /api/work?iterations=…&batch=1` | CPU work, at interactive or batch priority |

## Where does what I sent get received?

Press **Run CPU work** and the page answers exactly that. Four hops, and the
last one is the machine your work actually ran on:

```
RESULT
2.84 ms of work, output 80a9a14fafed71a9

WHERE IT WAS RECEIVED
┌ browser     your machine
│               picked the data
│ bridge      syuum:8081 (this node process, pid 20556)
│               held the mesh connection, 4.2 ms round trip
│ controller  127.0.0.1:7100
│               chose the node and moved the data if it had to
└ agent       syuum (127.0.0.1:7001)
                ran it in 2.84 ms — this is where your work executed
```

Nothing in that list is guessed. The node id comes back with the result, and
the bridge turns it into a hostname by asking the mesh who that is.

| hop | what it is | what it receives |
|---|---|---|
| **browser** | your page | nothing — it only sends |
| **bridge** | `server.mjs`, this Node process | the HTTP request and the uploaded bytes |
| **controller** | `aether-controller`, client API on 7100 | the task and its inputs, over length-prefixed JSON |
| **agent** | `aether-agent` on some machine | the task itself, over the bincode protocol — **and this is where it runs** |

The browser never talks to the controller and never talks to an agent. It
cannot: a page has no raw TCP. It talks to the bridge, which is also where
your mesh token lives and where the task kinds are fixed — the page chooses
data, never code.

### Watching it arrive from the other side

The same task, asked for from the mesh rather than from the page:

```bash
curl -s http://127.0.0.1:8081/api/recent
```

```
cpu    2.84 ms on 72e01f11 19s ago  output='...O..q.'
```

And in the terminal dashboard, which is watching the whole mesh and knows
nothing about this web service:

```
┌ Recent tasks (mesh) ──────────────────────────────────────────────────┐
│✓ cpu            2.8 ms 72e01f11  23s    ...O..q.                      │
└───────────────────────────────────────────────────────────────────────┘
```

Same node id, same duration, same output. That is the loop closed: submitted
from a browser, executed on an agent, and visible from anywhere that can reach
the mesh.

The output shows as dots there because it is binary — the `cpu` task returns
eight bytes of accumulator, and previews replace anything unprintable. The page
shows the same bytes as hex: `80a9a14fafed71a9`.

## One connection is one queue

The client protocol matches replies to requests **in order** on a connection.
So a request waiting on a slow task holds up every request queued behind it on
that socket. It looks exactly like the mesh being slow, and it is not: it is
the socket being busy.

```bash
node load.mjs 32 50000000
```

Measured against the same three-agent mesh, 32 concurrent requests:

| `POOL_SIZE` | wall | median request | slowest | nodes used | parallelism |
|---|---:|---:|---:|---:|---:|
| 1 | 242 ms | 126 ms | 216 ms | **1** | 0.8× |
| 8 | **72 ms** | **37 ms** | 53 ms | **3** | 2.9× |

Same mesh, same work, 3.4× the throughput. Look at the **nodes used** column
for the reason: with one connection the service hands the mesh one task at a
time, so there is never a second task for the scheduler to place anywhere else.
A serialised client makes a parallel mesh look like one machine.

[`pool.mjs`](pool.mjs) is the whole fix — a fixed set of connections, borrowed
one at a time. Callers borrow rather than take: a request that forgets to give
a connection back shrinks the pool permanently, and that failure shows up an
hour later as "the site got slow".

## Someone waiting on a page beats a nightly job

`POST /api/work?batch=1` submits at `background` priority; without it the
request is `high`. That only matters once more work has arrived than there are
nodes — an idle mesh has no queue and nothing to reorder — which is exactly
when a person is staring at a spinner.

```js
mesh.run("cpu", payload, [], [], batch ? "background" : "high")
```

## Errors a caller can act on

A task that *ran* and failed is the caller's problem; a task that could not be
placed is ours. Collapsing both into `500` makes a client retry the one thing
retrying cannot fix, so:

| | |
|---|---|
| `422` | the task ran and failed — bad input, wrong module |
| `503` | no node would take it, or the queue refused it — retry |
| `413` | the upload was too large for this service |

`/healthz` returns `503` when the mesh has no connected node, because a
service that answers "healthy" while unable to do any work is worse than one
that admits it.

## What this still does not do

The browser cannot choose a task kind, and should not be able to: the page
picks data, the server picks code. Nothing here authenticates the *user* —
that is your application's job, and it belongs in front of these routes. And
the pool has a fixed size; a real deployment would want it configurable per
instance and a queue depth limit in front of it, so that a burst becomes a
`503` rather than a growing pile of pending requests.
