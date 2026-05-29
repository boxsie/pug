#!/usr/bin/env bash
# Brings up the full PugPong demo end-to-end:
#   3 ensembled daemons + the matchmaker + 2 Godot clients.
# All runtime artefacts (daemon data dirs, logs, the matchmaker addr handoff,
# and a pids file for stop.sh) live under ./output/ which is gitignored.
#
# Topology: the matchmaker runs on its OWN daemon (M); each Godot client gets
# its own daemon (A, B). This mirrors the studio-hosted shape — a well-known
# matchmaker peer, players elsewhere — and keeps the demo honest: every client
# reaches the matchmaker cross-daemon, so neither one silently takes a
# same-daemon local-delivery shortcut. (An earlier 2-daemon layout co-located
# the matchmaker with client A, which masked cross-daemon bugs on A's side.)
#
# Ctrl-C tears the lot down cleanly. If processes orphan (e.g. the script
# is killed with SIGKILL), run ./stop.sh to clean up using the pids file.
#
# Knobs (env vars):
#   ENSEMBLE_BIN          path to the ensembled binary
#                         default: ../../../../ensemble/bin/ensemble
#   GODOT_BIN             godot binary (default: 'godot' on PATH)
#   DAEMON_M_PORT         matchmaker daemon (default 9090)
#   DAEMON_A_PORT         client A daemon  (default 9091)
#   DAEMON_B_PORT         client B daemon  (default 9092)

set -euo pipefail

# --- Paths --------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PUGPONG_DIR="$(dirname "$SCRIPT_DIR")"
PUG_DIR="$(dirname "$(dirname "$PUGPONG_DIR")")"
OUTPUT_DIR="$SCRIPT_DIR/output"

# --- Config -------------------------------------------------------------
ENSEMBLE_BIN="${ENSEMBLE_BIN:-$PUG_DIR/../ensemble/bin/ensemble}"
GODOT_BIN="${GODOT_BIN:-godot}"
DAEMON_M_PORT="${DAEMON_M_PORT:-9090}"
DAEMON_A_PORT="${DAEMON_A_PORT:-9091}"
DAEMON_B_PORT="${DAEMON_B_PORT:-9092}"

# --- Prep ---------------------------------------------------------------
mkdir -p "$OUTPUT_DIR/daemon-M" "$OUTPUT_DIR/daemon-A" "$OUTPUT_DIR/daemon-B"
PIDS_FILE="$OUTPUT_DIR/pids"
ADDR_FILE="$OUTPUT_DIR/matchmaker.addr"
: > "$PIDS_FILE"
rm -f "$ADDR_FILE"

PIDS=()
record_pid() { PIDS+=("$1"); echo "$1" >> "$PIDS_FILE"; }

cleanup() {
    echo
    echo "→ tearing down…"
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
        fi
    done
    sleep 1
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill -9 "$pid" 2>/dev/null || true
        fi
    done
    rm -f "$PIDS_FILE"
}
trap cleanup EXIT INT TERM

# --- Prereq checks ------------------------------------------------------
[ -x "$ENSEMBLE_BIN" ] || { echo "ENSEMBLE_BIN not executable: $ENSEMBLE_BIN" >&2; exit 1; }
command -v "$GODOT_BIN" >/dev/null 2>&1 || { echo "godot not on PATH (set GODOT_BIN)" >&2; exit 1; }
command -v dotnet >/dev/null 2>&1 || { echo "dotnet not on PATH" >&2; exit 1; }

# --- Helpers ------------------------------------------------------------
wait_for_port() {
    local port="$1" name="$2" timeout=30 elapsed=0
    while [ "$elapsed" -lt "$timeout" ]; do
        if (echo > /dev/tcp/127.0.0.1/"$port") >/dev/null 2>&1; then
            echo "  $name listening on :$port (${elapsed}s)"
            return 0
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done
    echo "  $name FAILED to bind :$port within ${timeout}s — check the log" >&2
    return 1
}

# --- Daemons ------------------------------------------------------------
echo "→ daemon M (matchmaker, port $DAEMON_M_PORT, data $OUTPUT_DIR/daemon-M)…"
"$ENSEMBLE_BIN" --headless \
    --signaling=loopback \
    --api-addr "127.0.0.1:$DAEMON_M_PORT" \
    --data-dir "$OUTPUT_DIR/daemon-M" \
    >"$OUTPUT_DIR/daemon-M.log" 2>&1 &
record_pid $!

echo "→ daemon A (client A, port $DAEMON_A_PORT, data $OUTPUT_DIR/daemon-A)…"
"$ENSEMBLE_BIN" --headless \
    --signaling=loopback \
    --api-addr "127.0.0.1:$DAEMON_A_PORT" \
    --data-dir "$OUTPUT_DIR/daemon-A" \
    >"$OUTPUT_DIR/daemon-A.log" 2>&1 &
record_pid $!

echo "→ daemon B (client B, port $DAEMON_B_PORT, data $OUTPUT_DIR/daemon-B)…"
"$ENSEMBLE_BIN" --headless \
    --signaling=loopback \
    --api-addr "127.0.0.1:$DAEMON_B_PORT" \
    --data-dir "$OUTPUT_DIR/daemon-B" \
    >"$OUTPUT_DIR/daemon-B.log" 2>&1 &
record_pid $!

wait_for_port "$DAEMON_M_PORT" "daemon M"
wait_for_port "$DAEMON_A_PORT" "daemon A"
wait_for_port "$DAEMON_B_PORT" "daemon B"

# Under --signaling=loopback the registry stands up synchronously inside
# daemon Start (Ready() is pre-closed) — by the time gRPC is listening,
# RegisterServiceAsync will succeed. No bootstrap wait needed.

# --- Matchmaker ---------------------------------------------------------
echo "→ matchmaker (against daemon M)…"
(
    cd "$PUGPONG_DIR/Matchmaker"
    ENSEMBLE_GRPC_ADDR="http://127.0.0.1:$DAEMON_M_PORT" \
    PUGPONG_ADDR_FILE="$ADDR_FILE" \
        dotnet run -c Debug 2>&1 \
        | tee "$OUTPUT_DIR/matchmaker.log" \
        >/dev/null
) &
record_pid $!

echo "  waiting for matchmaker E-address handoff…"
timeout=90; elapsed=0
while [ ! -s "$ADDR_FILE" ] && [ "$elapsed" -lt "$timeout" ]; do
    sleep 1
    elapsed=$((elapsed + 1))
done
if [ ! -s "$ADDR_FILE" ]; then
    echo "  matchmaker FAILED to publish E-address within ${timeout}s" >&2
    echo "  → tail $OUTPUT_DIR/matchmaker.log" >&2
    exit 1
fi
MATCHMAKER_ADDR=$(cat "$ADDR_FILE")
echo "  matchmaker addr: $MATCHMAKER_ADDR"

# --- Godot clients ------------------------------------------------------
echo "→ client A (daemon A)…"
ENSEMBLE_GRPC_ADDR="http://127.0.0.1:$DAEMON_A_PORT" \
PUGPONG_MATCHMAKER_ADDR="$MATCHMAKER_ADDR" \
    "$GODOT_BIN" --path "$PUGPONG_DIR/Client" \
    >"$OUTPUT_DIR/client-A.log" 2>&1 &
record_pid $!

echo "→ client B (daemon B)…"
ENSEMBLE_GRPC_ADDR="http://127.0.0.1:$DAEMON_B_PORT" \
PUGPONG_MATCHMAKER_ADDR="$MATCHMAKER_ADDR" \
    "$GODOT_BIN" --path "$PUGPONG_DIR/Client" \
    >"$OUTPUT_DIR/client-B.log" 2>&1 &
record_pid $!

cat <<EOF

✓ all up. Ctrl-C to stop everything.
  daemon M:    tail -f $OUTPUT_DIR/daemon-M.log
  daemon A:    tail -f $OUTPUT_DIR/daemon-A.log
  daemon B:    tail -f $OUTPUT_DIR/daemon-B.log
  matchmaker:  tail -f $OUTPUT_DIR/matchmaker.log
  client A:    tail -f $OUTPUT_DIR/client-A.log
  client B:    tail -f $OUTPUT_DIR/client-B.log

EOF

# Block until any child exits (then cleanup runs via trap).
wait -n
