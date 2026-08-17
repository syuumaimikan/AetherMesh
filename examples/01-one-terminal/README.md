# 01 · A whole mesh in one terminal

The smallest thing that is still real: a controller, an agent, and a task,
without opening a second window.

```bash
cargo run -p aether-controller --example one_terminal
```

```
mesh ready: 1 node
echo    -> "hello"          in 0.3 ms
hash    -> 9f2b…            in 0.5 ms
cpu     -> 100000 rounds    in 1.9 ms
```

## What just happened

The example starts the control plane and one worker inside the same process —
the same code paths a real deployment uses, over real sockets on localhost —
then submits three built-in tasks and prints their results.

Nothing here is special-cased for being in one process. Split it across two
terminals ([`02-two-terminals`](../02-two-terminals)) or two machines
([`04-two-devices`](../04-two-devices)) and the same code runs unchanged.

## Where to look in the code

- `crates/aether-controller/examples/one_terminal.rs` — the whole thing, ~60 lines.
- `Controller::submit` is the one call that matters; everything else is setup.
