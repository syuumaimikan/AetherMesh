# 08 · A mesh you can put on a real network

Everything so far ran open on localhost. This is the version that survives
leaving the machine: TLS on both listeners, a token per node, and — for the
mesh that has to hold up — a client certificate per node.

Copy this one before anything else goes on a network you do not own.

## 1 · A CA and a server certificate

```bash
cargo build --release -p aether-controller -p aether-agent --features tls
./target/release/aether-controller generate-cert --with-ca --host mesh.example.com
```

Four files: `ca.pem`, `ca.key`, `cert.pem`, `key.pem`. The CA key never leaves
this machine; `ca.pem` is what you hand out.

## 2 · A certificate per node

```bash
./target/release/aether-controller issue-client-cert --name rpi4 \
    --cert-path rpi4.pem --key-path rpi4.key
```

One per machine. Revoking a node is deleting its certificate from the CA you
trust — no re-keying of the rest of the mesh.

## 3 · The controller

```toml
# controller.toml
listen        = "0.0.0.0:7000"
client_listen = "0.0.0.0:7100"

tls_cert_path      = "cert.pem"
tls_key_path       = "key.pem"
tls_client_ca_path = "ca.pem"     # ← this line turns on mutual TLS

heartbeat_timeout_secs = 30
probe_interval_secs    = 60

[node_tokens]
rpi4    = "…"      # openssl rand -hex 32
desktop = "…"
bridge  = "…"      # the web bridge from example 05 gets its own
```

```bash
./target/release/aether-controller --config controller.toml
```

```
INFO aether_controller::tls: requiring client certificates ca=ca.pem
INFO aether_controller: controller listening addr=0.0.0.0:7000 auth=true tls=true
INFO aether_controller: client API listening client_addr=0.0.0.0:7100 tls=true
```

## 4 · An agent

```bash
AETHERMESH_TOKEN=… ./aether-agent \
    --controller mesh.example.com:7000 \
    --tls-ca ca.pem \
    --tls-client-cert rpi4.pem \
    --tls-client-key rpi4.key
```

## 5 · A client

```python
mesh = AetherMesh.connect(
    host="mesh.example.com", port=7100,
    token=os.environ["AETHERMESH_TOKEN"],
    tls_ca_path="ca.pem",
)
```

## What each layer stops

| Layer | Stops |
|---|---|
| TLS | Reading or altering traffic on the wire |
| Client certificates | Anyone without a certificate, before they can even present a token |
| Per-node tokens | A leaked credential from being every credential — revoke one line |
| Channel tokens (automatic) | A mesh member attaching a data connection in another node's name |

The last one is not configuration; the controller issues a per-registration
secret and only that node's own connections can use it. It exists because a
shared mesh token says "you may join", not "you are that node".

## Checking it actually refuses things

```bash
# no certificate → the handshake fails, before any token is sent
./aether-agent --controller mesh.example.com:7000 --tls-ca ca.pem

# wrong token → refused at registration
AETHERMESH_TOKEN=guess ./aether-agent --controller … --tls-client-cert rpi4.pem --tls-client-key rpi4.key
```

```
WARN aether_controller::server: rejecting node error=invalid token
```

Both cases are covered by tests in `crates/aether-agent/tests/security.rs`, so
they stay refused.

## Still worth doing

- Keep the client API on an interface only your own services reach, or put it
  behind a reverse proxy that authenticates users.
- Rotate node tokens by editing `[node_tokens]` and restarting the controller;
  agents reconnect on their own.
- Read [`docs/security.md`](../../docs/security.md) for the threat model and
  what is deliberately out of scope.
