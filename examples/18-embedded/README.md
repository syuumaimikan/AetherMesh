# 18 · A mesh inside your own program

Everything `aether-controller` does is a library first — the registry, the
catalog, the scheduler, the dispatch loop. A Rust service that wants to spread
its own work across machines can hold those directly: no controller process to
deploy, no client API port to secure, no JSON in the middle.

The code is a cargo example so it cannot rot:
[`crates/aether-controller/examples/embedded.rs`](../../crates/aether-controller/examples/embedded.rs).

## Run it

```bash
cargo run -p aether-controller --example embedded
```

It prints the address it is listening on. Point a few ordinary agents at it —
these are the real binary, nothing here is simulated:

```bash
aether-agent --controller 127.0.0.1:50578 --identity-path ./a1.id
```

```
3 node(s) registered

published 4194304 bytes as 4ce5ea7cd418ca55…

16 tasks in 91.3384ms, 0 failed
  spread {"49bc939a": 6, "868c8bd3": 5, "c002bc19": 5}
  12582912 bytes moved for a 4 MiB dataset, 13 transfers skipped
```

Sixteen tasks over one dataset: 12 MiB moved, which is 4 MiB to each of the
three nodes that ran work. Not once per task — once per machine that needed it.

## The four pieces

```rust
let state = MeshState::new();                       // registry, connections, catalog
let (listener, addr) = bind("127.0.0.1:0".parse()?).await?;
tokio::spawn(serve(listener, state.clone(), SecurityConfig::open()));

let controller = Arc::new(Controller::new(
    AdvancedScheduler::new(state.catalog.clone()),  // where work goes
    NetworkTransport::new(state.connections.clone()),
    state.catalog.clone(),                          // where data is
));
```

`serve` is a task, not a process. `submit` takes `&self`, so an `Arc` is all
you need to dispatch from every request handler at once — that is the ordinary
use, not a batch mode.

Use `SecurityConfig::open()` only where the agent port is unreachable from
anywhere you do not control; [`08-secure-mesh`](../08-secure-mesh) is the one
that turns on tokens and TLS.

## What writing this example found

The first run of it looked like this:

```
16 tasks in 118.5ms, 0 failed
  20971520 bytes moved for a 4 MiB dataset
```

Twenty megabytes, eleven retries, and agent logs full of `no manifest received
for data …`. Sixteen tasks had been dispatched at once, all wanting the same
input on the same node, and every one of them started sending it. The chunk
streams interleaved, the agent rejected chunks belonging to a manifest it had
not seen yet, and the controller retried elsewhere.

Transfers are single-flighted now — one dataset to one node at a time, with the
tasks behind it waiting and then finding the catalog already says it is there.
Same run afterwards: 10.4 ms, 3 MiB, no retries. There is a regression test
that fails without it.

That bug had been reachable for as long as concurrent dispatch has existed. It
took writing down "here is how you would use this from your own service" to
walk into it, which is a fair argument for examples being part of the code
rather than decoration on it.

## When not to do this

Reach for the binary instead if you want the mesh usable from other languages,
several independent services submitting to one mesh, or an operator who can
restart the control plane without redeploying your application. Embedding ties
the mesh's lifetime to your process — which is exactly right for a batch job,
and exactly wrong for a shared cluster.
