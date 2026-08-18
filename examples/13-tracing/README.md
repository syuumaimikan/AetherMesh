# 13 · Following one task

`/metrics` tells you how much: bytes moved, tasks run, queue depth. It cannot
tell you what happened to *one* task, because by the time a counter has moved,
the task it describes is anonymous. That is what traces are for.

Needs a controller built with the feature, which is off by default:

```bash
cargo build --release -p aether-controller --features otel
```

## Run it

There is a collector here — twenty lines, no dependencies — so you can see the
export working without installing a tracing backend:

```bash
python collector.py
```

Then start the controller and the agent pointing at it — both, because the
interesting part is that they end up in the same trace:

```bash
./target/release/aether-controller --otlp-endpoint http://127.0.0.1:4318/v1/traces
```

```bash
./target/release/aether-agent --otlp-endpoint http://127.0.0.1:4318/v1/traces
```

Submit some work — [`../12-verify`](../12-verify) will do:

```bash
python ../12-verify/verify.py traffic
```

```
aether-controller  send_inputs     20023 us  trace=5d9501f2 {'node_id': '064408ba…', 'inputs': '1'}
aether-controller  dispatch         1090 us  trace=5d9501f2 {'node_id': '064408ba…'}
aether-controller  submit          21160 us  trace=5d9501f2 {'kind': 'hash', 'attempts': '1'}

aether-controller  send_inputs         3 us  trace=1efe42cd {'node_id': '064408ba…', 'inputs': '1'}
aether-controller  dispatch          891 us  trace=1efe42cd {'node_id': '064408ba…'}
aether-controller  submit            913 us  trace=1efe42cd {'kind': 'hash', 'attempts': '1'}
```

Four identical tasks over one 4 MiB dataset. The first took 21 ms and the rest
took 1 ms, and the trace says why without anybody having to guess: 20 of those
21 ms were `send_inputs`, moving the data to the node. After that the data is
there and `send_inputs` takes three microseconds.

A counter would have shown you 4 MiB moved and four tasks run. It could not
have told you which task paid for the move.

## Against a real collector

The endpoint is an ordinary OTLP/HTTP one, so anything that speaks it works —
point `--otlp-endpoint` at your collector's `/v1/traces` and drop `collector.py`.
The payload is JSON rather than protobuf, which is why the toy collector above
can be twenty lines.

Two knobs, deliberately separate:

- `RUST_LOG` — how noisy the console is
- `AETHERMESH_TRACE` — what gets exported

Turning the terminal down to `warn` should not silently stop your tracing, and
with one shared filter it would: an instrumented span is disabled before any
exporter sees it.

## One trace, two machines

The controller sends its trace context along with the assignment, so the node
that runs the task joins the trace instead of starting one:

```
aether-controller  dispatch   1232 us  trace=2ab06f53 {'task_id': '38911df6…'}
aether-agent       execute    1043 us  trace=2ab06f53 {'task_id': '38911df6…'}
```

Same `trace`, two services. And a number that neither process could produce on
its own: the controller waited 1232 us, the node worked for 1043 us of it, so
190 us went on the wire and the queue. Ask "is the network slow or is the work
slow" without this and you are guessing.

The header is W3C `traceparent`, sent as an ordinary string on the assignment.
An agent that was not built with `--features otel` ignores it; a controller
that was not exporting sends `None`; a header the node cannot parse leaves its
span as a root and the task still runs. Tracing is never allowed to be the
reason work fails.

## What is not here yet

Data transfers are timed as one span (`send_inputs`) rather than per chunk, so
a slow transfer does not yet say *which* chunk was slow. The client API is not
instrumented either: a trace starts when the controller accepts the task, not
when your program asked for it.
