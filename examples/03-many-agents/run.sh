#!/bin/sh
# Starts a controller and N agents on this machine, each with its own identity.
#
#   ./run.sh 4
set -eu

AGENTS="${1:-4}"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
STATE="${TMPDIR:-/tmp}/aethermesh-example"
mkdir -p "$STATE"

BIN="$ROOT/target/release"
if [ ! -x "$BIN/aether-controller" ]; then
    echo "building..."
    (cd "$ROOT" && cargo build --release -p aether-controller -p aether-agent)
fi

RUST_LOG="${RUST_LOG:-info}"
export RUST_LOG

"$BIN/aether-controller" --listen 127.0.0.1:7000 --client-listen 127.0.0.1:7100 \
    >"$STATE/controller.log" 2>&1 &
echo $! >"$STATE/controller.pid"
sleep 1

: >"$STATE/agents.pid"
i=0
while [ "$i" -lt "$AGENTS" ]; do
    # Separate identity files: agents sharing one would all claim to be the
    # same node, and the mesh would look like one machine reconnecting.
    "$BIN/aether-agent" \
        --controller 127.0.0.1:7000 \
        --heartbeat-secs 2 \
        --identity-path "$STATE/node-$i" \
        >"$STATE/agent-$i.log" 2>&1 &
    echo $! >>"$STATE/agents.pid"
    i=$((i + 1))
done

sleep 2
echo "controller + $AGENTS agents running; logs in $STATE"
echo "submit work:  python $ROOT/sdk/python/examples/hash.py"
echo "stop:         $(dirname "$0")/stop.sh"
