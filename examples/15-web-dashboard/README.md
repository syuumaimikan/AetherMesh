# 15 · The dashboard, in a browser

[`crates/aether-tui`](../../crates/aether-tui) is the terminal version of this.
Same three questions — what is the mesh made of, what has it saved, what has it
just run — served to anyone with a browser and no terminal.

## Run it

```bash
node server.mjs
```

Open <http://127.0.0.1:8082>. Then give it something to show, from anywhere:

```bash
node ../../sdk/typescript/examples/wasm.ts ../../uppercase.wasm "hello from the dashboard"
```

A real page, read out of a real browser:

```
AetherMesh                                                            live
NOT MOVED          ON THE WIRE        NODES              TASKS
0 B                233 B              3/3                113
0 transfers        ratio 1.000        none waiting       0 failed
skipped · 0 chunks · 0 retries                           6 datasets held

    KIND   MS     NODE      AGO   OUTPUT
✓   wasm   15.4   6e34d8cc  33s   HELLO FROM THE DASHBOARD
✓   cpu     6.7   70ebda25  2m    ..*...8t
✓   cpu     6.2   1cba9b2e  2m    ..*...8t
```

That `wasm` row was submitted from a different terminal by a TypeScript script.
The dashboard is watching the **mesh**, not its own traffic.

## One poll, many viewers

The server holds a single mesh connection, polls it once a second, and pushes
the same reading to everyone watching over **server-sent events**:

```
mesh ──1 poll/s──▶ server ──SSE──▶ browser
                          ──SSE──▶ browser
                          ──SSE──▶ browser
```

Fifty dashboards should cost the controller one reading per second, not fifty.
A monitoring page that scales its load with its audience becomes the reason the
thing it monitors is busy.

SSE rather than WebSockets because this only ever goes one way. It is six lines
in the browser, needs no library, and reconnects by itself when the server
restarts — which you can watch happen by killing this process and leaving the
tab open.

## Two things that are easy to get wrong

**A dashboard that goes blank on a hiccup is worse than a stale one.** When a
poll fails, the last good reading stays on screen and the header says `stale`
with the reason. Blank screens send people to check whether the mesh died; a
stale marker tells them what actually happened.

**The output column is `textContent`, never `innerHTML`.** A preview is the
front of a task's output, and a task's output is arbitrary bytes from whatever
someone ran. The controller already replaces unprintable bytes so it cannot
drive a terminal; here the browser's own escaping is what stops it becoming an
injection into your dashboard. Data on a screen is still data.

## What it does not do

No authentication, and it exposes mesh internals to anyone who can reach the
port — put it behind whatever your organisation uses, or bind it to localhost
and tunnel. There are no controls either: this watches, it does not submit.
[`14-web-service`](../14-web-service) is the one that accepts work.
