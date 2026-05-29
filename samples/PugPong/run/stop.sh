#!/usr/bin/env bash
# Kills any processes recorded by a previous run-demo.sh that didn't
# clean up (e.g. the script was SIGKILL'd or the terminal closed). Reads
# pids from ./output/pids — won't touch other daemon / godot processes
# the user has running.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PIDS_FILE="$SCRIPT_DIR/output/pids"

if [ ! -f "$PIDS_FILE" ]; then
    echo "no pids file at $PIDS_FILE — nothing to stop"
    exit 0
fi

while read -r pid; do
    [ -z "$pid" ] && continue
    if kill -0 "$pid" 2>/dev/null; then
        echo "  kill $pid"
        kill "$pid" 2>/dev/null || true
    fi
done < "$PIDS_FILE"

sleep 1

while read -r pid; do
    [ -z "$pid" ] && continue
    if kill -0 "$pid" 2>/dev/null; then
        echo "  kill -9 $pid"
        kill -9 "$pid" 2>/dev/null || true
    fi
done < "$PIDS_FILE"

rm -f "$PIDS_FILE"
echo "done."
