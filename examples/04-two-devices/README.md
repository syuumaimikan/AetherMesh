# 04 · Two devices

A laptop and a Raspberry Pi in one mesh. Any two machines that can reach each
other work; the Pi is here because it is the case that actually teaches you
something — a slow link and a small CPU are exactly what the scheduler is for.

```
laptop  192.168.1.10          raspberry pi  192.168.1.42
├── controller  :7000 :7100   └── agent ──────────────┐
└── agent                                             │
        ▲─────────────────────────────────────────────┘
```

## 1 · On the machine that runs the controller

```bash
cargo build --release -p aether-controller -p aether-agent
RUST_LOG=info ./target/release/aether-controller \
    --listen 0.0.0.0:7000 --client-listen 127.0.0.1:7100
```

`0.0.0.0` for agents so the Pi can reach it; `127.0.0.1` for the client API so
only programs on this machine can submit work.

Check the firewall lets 7000 through **from your LAN only**:

```bash
sudo ufw allow from 192.168.1.0/24 to any port 7000 proto tcp   # Linux
```

```powershell
New-NetFirewallRule -DisplayName "AetherMesh agents" -Direction Inbound `
  -LocalPort 7000 -Protocol TCP -RemoteAddress 192.168.1.0/24 -Action Allow
```

## 2 · Build the agent for the Pi

Cross-compiling from the laptop is fastest:

```bash
rustup target add aarch64-unknown-linux-gnu
cargo build --release -p aether-agent --target aarch64-unknown-linux-gnu
scp target/aarch64-unknown-linux-gnu/release/aether-agent pi@raspberrypi.local:~
```

Or build on the Pi itself — slower, no toolchain to set up:

```bash
cargo build --release -p aether-agent
```

## 3 · Join the Pi

```bash
RUST_LOG=info ./aether-agent --controller 192.168.1.10:7000 --heartbeat-secs 5
```

The controller says so:

```
INFO aether_controller::server: node registered node_id=1f0a77c2-… hostname=raspberrypi
```

## 4 · Watch it decide

```bash
python sdk/python/examples/hash.py
```

Then try the two cases that show the scheduler working:

**Big data, one node.** Publish 100 MB and submit ten tasks over it. Every task
lands on the node that received the data — moving 100 MB across the LAN costs
more than the CPU difference between the two machines.

**Small data, busy node.** Load the laptop (`--kind cpu --cpu-iterations 40000000`),
and work moves to the Pi even though it is slower, because the transfer is
small enough not to matter.

That is the whole idea, visible on two machines you own.

## Things that go wrong

| Symptom | Cause |
|---|---|
| Agent exits immediately | Wrong address, or port 7000 filtered — check with `nc -vz 192.168.1.10 7000` |
| Node registers then vanishes | Heartbeat timeout: the Pi slept, or Wi-Fi dropped |
| Both machines show the same hostname | You copied the identity file too; delete it on one and restart |
| Tasks never go to the Pi | Its CPU or its link is genuinely worse — check `probe_interval_secs` measurements in the log |

Before this leaves your LAN, turn on TLS and tokens:
[`08-secure-mesh`](../08-secure-mesh).
