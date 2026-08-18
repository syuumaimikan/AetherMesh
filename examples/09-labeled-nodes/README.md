# 09 · Not every machine will do

Cost decides where work is *cheapest*. It cannot decide that the GPU job must
not land on a CPU box, or that this dataset may not leave its region. That is
what labels are for.

## Run it

Three agents on one machine, each claiming to be something different:

```bash
cargo run -p aether-controller -- --listen 127.0.0.1:7000 --client-listen 127.0.0.1:7100 &

cargo run -p aether-agent -- --identity-path ./cpu.id  --advertise 127.0.0.1:7001 \
  --label kind=cpu    --label region=eu-west &
cargo run -p aether-agent -- --identity-path ./gpu.id  --advertise 127.0.0.1:7002 \
  --label kind=gpu    --label region=eu-west &
cargo run -p aether-agent -- --identity-path ./east.id --advertise 127.0.0.1:7003 \
  --label kind=cpu    --label region=us-east &
```

```powershell
# PowerShell
Start-Process cargo "run -p aether-controller -- --listen 127.0.0.1:7000 --client-listen 127.0.0.1:7100"
Start-Process cargo "run -p aether-agent -- --identity-path ./cpu.id  --advertise 127.0.0.1:7001 --label kind=cpu --label region=eu-west"
Start-Process cargo "run -p aether-agent -- --identity-path ./gpu.id  --advertise 127.0.0.1:7002 --label kind=gpu --label region=eu-west"
Start-Process cargo "run -p aether-agent -- --identity-path ./east.id --advertise 127.0.0.1:7003 --label kind=cpu --label region=us-east"
```

Each agent needs its own `--identity-path`, or the three of them register as one
node.

Then:

```bash
python place.py
```

```
3 node(s):
  1d69685f  syuum        kind=cpu region=us-east
  27c8e173  syuum        kind=cpu region=eu-west
  e37f7a7c  syuum        kind=gpu region=eu-west

anywhere                 -> 27c8e173
kind=gpu                 -> e37f7a7c
region=eu-west           -> 27c8e173
region!=eu-west          -> 1d69685f
kind=gpu, region=us-east -> refused: no node available for task dce56685-…
```

The last line is the point. There is a machine free, and the task does not run
on it. A constraint is a filter, not a preference — silently falling back to
"close enough" is how regulated data ends up in the wrong country.

## Writing the constraints

Three forms, in every SDK and the client API:

| Form | Means |
|---|---|
| `gpu=true` | the node carries `gpu` with exactly that value |
| `gpu` | the node carries `gpu`, whatever the value |
| `region!=us-east` | the node does not carry `region=us-east` — including nodes with no `region` at all |

```python
mesh.run("hash", payload, constraints=["kind=gpu", "region=eu-west"])
```

```ts
await mesh.run("hash", payload, [], ["kind=gpu", "region=eu-west"]);
```

```go
mesh.Run("hash", payload, nil, "kind=gpu", "region=eu-west")
```

## Where labels come from

`--label key=value`, repeatable, or a `labels` list in the agent's TOML:

```toml
labels = ["kind=gpu", "region=eu-west", "arch=arm64"]
```

Flags add to the file rather than replacing it: the file says what the machine
is, the flags say what this run of it is.

Nothing verifies a label. An agent claiming `kind=gpu` on a machine with no GPU
will be sent GPU work and will fail it. Labels are a routing mechanism, not an
inventory system — the truth still comes from whoever writes the config.
