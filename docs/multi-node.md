# Running AetherMesh across three real machines

The reference setup is deliberately heterogeneous: a Windows desktop, a
Raspberry Pi, and a free-tier cloud VM. If the mesh behaves on those three, it
behaves on a rack.

> **Security note.** The commands below are plaintext and unauthenticated, which
> is fine on a LAN or a VPN (Tailscale/WireGuard). Before the controller port is
> reachable from anywhere else, turn on TLS and a token — see
> [Securing the mesh](#securing-the-mesh) at the end.

## Topology

```
              controller (cloud VM, public-ish address)
                      :7000
        ┌───────────────┼───────────────┐
   agent (desktop)  agent (Raspberry Pi)  agent (VM itself)
```

The controller only needs one reachable TCP port. Agents dial out, so they work
behind NAT without any inbound rules.

## 1. Controller — cloud VM

```bash
cargo build --release -p aether-controller
RUST_LOG=info ./target/release/aether-controller --listen 0.0.0.0:7000
```

Open the port to your own networks only, e.g. on a systemd host:

```bash
sudo ufw allow from 203.0.113.0/24 to any port 7000 proto tcp
```

## 2. Agent — Windows desktop

```powershell
cargo build --release -p aether-agent
$env:RUST_LOG = "info"
.\target\release\aether-agent.exe --controller 198.51.100.10:7000 --heartbeat-secs 5
```

## 3. Agent — Raspberry Pi

Cross-compile from the desktop (fastest) …

```bash
rustup target add aarch64-unknown-linux-gnu
cargo build --release -p aether-agent --target aarch64-unknown-linux-gnu
scp target/aarch64-unknown-linux-gnu/release/aether-agent pi@raspberrypi.local:~
```

… or build on the Pi directly (slower, but no toolchain setup):

```bash
cargo build --release -p aether-agent
```

Then run it, telling the mesh how slow its link is so the scheduler and the
compression policy make sensible choices:

```bash
RUST_LOG=info ./aether-agent --controller 198.51.100.10:7000 --heartbeat-secs 5
```

Keep it alive across reboots with a unit file:

```ini
# /etc/systemd/system/aether-agent.service
[Unit]
Description=AetherMesh agent
After=network-online.target

[Service]
ExecStart=/home/pi/aether-agent --controller 198.51.100.10:7000
Restart=always
RestartSec=5
Environment=RUST_LOG=info

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable --now aether-agent
```

## 4. Check the mesh

The controller logs one line per node:

```
INFO aether_controller::server: node registered node_id=9c5e43a0-… hostname=raspberrypi
INFO aether_controller::server: node registered node_id=1f0a77c2-… hostname=desktop
```

With `RUST_LOG=debug` you also see heartbeats, data transfers, and task results.
If a node goes quiet, the health monitor evicts it after
`--heartbeat-timeout-secs` (default 30) and its data locations are forgotten.

## 5. Submit work

Task submission is a library call today — there is no submit CLI yet. Write a
small binary against `aether-controller`:

```rust
use aether_controller::{
    Controller, MeshState, NetworkTransport, RetryPolicy, SecurityConfig, bind, serve,
};
use aether_core::{Task, task::kind};
use aether_scheduler::AdvancedScheduler;

let state = MeshState::new();
let (listener, _addr) = bind("0.0.0.0:7000".parse()?).await?;
tokio::spawn(serve(listener, state.clone(), SecurityConfig::open()));

let mut controller = Controller::new(
    AdvancedScheduler::new(state.catalog.clone()),
    NetworkTransport::new(state.connections.clone()),
    state.catalog.clone(),
)
.with_retry(RetryPolicy::default());

// Registry snapshot: the controller schedules over what it knows.
for info in state.registry.lock().unwrap().nodes() {
    controller.registry_mut().register(info);
}

let dataset = controller.publish(std::fs::read("input.bin")?);
let task = Task::new(kind::HASH, Vec::new()).with_inputs(vec![dataset.id]);
println!("{:?}", controller.submit(task).await?);
```

## What to expect

| Observation | Why |
|---|---|
| The first task carrying a dataset is slow; later ones are fast | The dataset is transferred once, then reused from the node's store |
| The Pi gets fewer tasks under load | Its CPU usage feeds the score |
| A dataset already on the Pi pulls work *to* the Pi | The locality bonus outweighs the compute cost once the data is large |
| Killing an agent does not fail the next task | The task is re-dispatched to another node |

## Securing the mesh

Build both binaries with the `tls` feature, then give the controller a
certificate and a token.

```bash
# On the controller host, once.
aether-controller generate-cert --host mesh.example.com
```

```toml
# controller.toml
listen = "0.0.0.0:7000"
auth_token = "a-long-random-string"
tls_cert_path = "cert.pem"
tls_key_path = "key.pem"
```

```bash
aether-controller --config controller.toml
```

Copy `cert.pem` (only the certificate, never the key) to each agent and point it
there. The agent verifies the controller against that file, so a self-signed
certificate is fine as long as it is the one you distributed:

```bash
AETHERMESH_TOKEN=a-long-random-string \
  aether-agent --controller mesh.example.com:7000 --tls-ca cert.pem
```

Registrations with a wrong or missing token are refused and counted:

```
WARN aether_controller::server: rejecting node node_id=… error=invalid token
```

The agent keeps its node id in `<data dir>/aethermesh/node-id`, so a restart —
or a reboot — rejoins the mesh as the same node rather than a new one.

## Troubleshooting

| Symptom | Check |
|---|---|
| Agent exits immediately | Controller address wrong or port filtered — `nc -vz host 7000` |
| Node registers, then disappears | Heartbeat timeout: the agent process was killed or the link dropped |
| `input … is not present on this node` | The node was evicted and its store lost; the next attempt re-sends the data |
| Nothing is compressed | Fast link, small payload, or data that does not shrink — all three are by design |
| `controller refused the registration: invalid token` | Token mismatch between `controller.toml` and the agent's `AETHERMESH_TOKEN` |
| TLS handshake fails | The agent's `--tls-ca` is not the controller's certificate, or the name in `--controller` is not one the certificate covers |
| `this build has no TLS support` | Rebuild with `--features tls` |
