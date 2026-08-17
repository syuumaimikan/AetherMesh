#!/bin/sh
# Stops what run.sh started.
set -eu

STATE="${TMPDIR:-/tmp}/aethermesh-example"

if [ -f "$STATE/agents.pid" ]; then
    while read -r pid; do kill "$pid" 2>/dev/null || true; done <"$STATE/agents.pid"
    rm -f "$STATE/agents.pid"
fi
if [ -f "$STATE/controller.pid" ]; then
    kill "$(cat "$STATE/controller.pid")" 2>/dev/null || true
    rm -f "$STATE/controller.pid"
fi

echo "stopped"
