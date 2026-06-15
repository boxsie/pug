#!/usr/bin/env bash
# Brings up the PugPong demo against a REMOTE matchmaker:
#   2 ensembled daemons (one per Godot client) + 2 Godot clients.
# The matchmaker runs on some remote peer (you host it yourself) and is reached
# over Tor via its E-address (PUGPONG_MATCHMAKER_ADDR below). All runtime
# artefacts (daemon data dirs, logs, a pids file for stop.sh) live under
# ./output/ which is gitignored.
#
# Topology: each Godot client gets its own daemon (A, B), both on Tor signaling
# so they can reach the matchmaker's onion service and discover each other for
# the post-match P2P link. The matchmaker is a well-known remote peer — exactly
# the studio-hosted shape.
#
# NOTE: Tor signaling means each daemon spends ~30-60s bootstrapping Tor before
# it can reach the matchmaker. The Godot clients show "discovering…" until then;
# Play works once both daemons are registry-ready. (The old loopback topology
# ran a local matchmaker but couldn't reach an onion one, so Tor is unavoidable
# for this WAN demo.)
#
# Ctrl-C tears the lot down cleanly. If processes orphan (e.g. the script is
# killed with SIGKILL), run ./stop.sh to clean up using the pids file.
#
# Knobs (env vars):
#   ENSEMBLE_BIN             path to the ensembled binary
#                            default: ../../../../ensemble/bin/ensemble
#   GODOT_BIN                godot binary (default: 'godot' on PATH)
#   TOR_PATH                 tor binary for the daemons (default: /usr/bin/tor)
#   PUGPONG_MATCHMAKER_ADDR  matchmaker E-address to dial (REQUIRED — you host
#                            the matchmaker; there is no default)
#   SEED_NODES               comma-separated v3 onion bootstrap seed(s) for DHT
#                            discovery (REQUIRED). The binary ships NO default
#                            seeds, so without this a Tor daemon has an empty
#                            routing table and can't find the matchmaker.
#   DAEMON_A_PORT            client A daemon gRPC port (default 9091)
#   DAEMON_B_PORT            client B daemon gRPC port (default 9092)

set -euo pipefail

# --- Paths --------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PUGPONG_DIR="$(dirname "$SCRIPT_DIR")"
PUG_DIR="$(dirname "$(dirname "$PUGPONG_DIR")")"
OUTPUT_DIR="$SCRIPT_DIR/output"

# --- Config -------------------------------------------------------------
ENSEMBLE_BIN="${ENSEMBLE_BIN:-$PUG_DIR/../ensemble/bin/ensemble}"
GODOT_BIN="${GODOT_BIN:-godot}"
TOR_PATH="${TOR_PATH:-/usr/bin/tor}"
DAEMON_A_PORT="${DAEMON_A_PORT:-9091}"
DAEMON_B_PORT="${DAEMON_B_PORT:-9092}"

# The matchmaker E-address to dial. You host the matchmaker yourself (e.g. an
# ensembled daemon running the PugPong matchmaker service); set this to its
# E-address. No default — the demo can't guess where your matchmaker lives.
MATCHMAKER_ADDR="${PUGPONG_MATCHMAKER_ADDR:-}"

# DHT bootstrap seed(s) — a v3 onion of a reachable ensemble node (typically the
# node that hosts your matchmaker). The binary has no built-in default seeds, so
# each client daemon must be handed one or its routing table stays empty and the
# matchmaker lookup fails (dht: lookup failed, queried=0). Set SEED_NODES to
# your own seed(s).
SEED_NODES="${SEED_NODES:-}"

# Both are required — bail early with a clear message rather than dialing nothing.
if [ -z "$MATCHMAKER_ADDR" ]; then
    echo "PUGPONG_MATCHMAKER_ADDR is required (the E-address of your matchmaker)." >&2
    echo "  e.g. PUGPONG_MATCHMAKER_ADDR=E... SEED_NODES=<onion> ./run-demo.sh" >&2
    exit 1
fi
if [ -z "$SEED_NODES" ]; then
    echo "SEED_NODES is required (a v3 onion bootstrap seed for DHT discovery)." >&2
    echo "  e.g. PUGPONG_MATCHMAKER_ADDR=E... SEED_NODES=<onion> ./run-demo.sh" >&2
    exit 1
fi

# --- Prep ---------------------------------------------------------------
mkdir -p "$OUTPUT_DIR/daemon-A" "$OUTPUT_DIR/daemon-B"
PIDS_FILE="$OUTPUT_DIR/pids"
: > "$PIDS_FILE"

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
command -v "$TOR_PATH" >/dev/null 2>&1 || { echo "tor not found at TOR_PATH=$TOR_PATH (install tor or set TOR_PATH)" >&2; exit 1; }

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

# --- Daemons (one per client, Tor signaling to reach the remote matchmaker) ---
echo "→ daemon A (client A, port $DAEMON_A_PORT, data $OUTPUT_DIR/daemon-A)…"
"$ENSEMBLE_BIN" --headless \
    --signaling=tor \
    --tor-path "$TOR_PATH" \
    --seed-nodes "$SEED_NODES" \
    --api-addr "127.0.0.1:$DAEMON_A_PORT" \
    --data-dir "$OUTPUT_DIR/daemon-A" \
    >"$OUTPUT_DIR/daemon-A.log" 2>&1 &
record_pid $!

echo "→ daemon B (client B, port $DAEMON_B_PORT, data $OUTPUT_DIR/daemon-B)…"
"$ENSEMBLE_BIN" --headless \
    --signaling=tor \
    --tor-path "$TOR_PATH" \
    --seed-nodes "$SEED_NODES" \
    --api-addr "127.0.0.1:$DAEMON_B_PORT" \
    --data-dir "$OUTPUT_DIR/daemon-B" \
    >"$OUTPUT_DIR/daemon-B.log" 2>&1 &
record_pid $!

wait_for_port "$DAEMON_A_PORT" "daemon A"
wait_for_port "$DAEMON_B_PORT" "daemon B"

# gRPC is listening, but under Tor signaling each daemon still needs ~30-60s to
# bootstrap Tor + the DHT before it can reach the matchmaker. The Godot clients
# poll readiness and show "discovering…" until then — Play works once both are
# registry-ready.
echo "  matchmaker (remote): $MATCHMAKER_ADDR"

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
  matchmaker:  remote — $MATCHMAKER_ADDR
  daemon A:    tail -f $OUTPUT_DIR/daemon-A.log
  daemon B:    tail -f $OUTPUT_DIR/daemon-B.log
  client A:    tail -f $OUTPUT_DIR/client-A.log
  client B:    tail -f $OUTPUT_DIR/client-B.log

  (daemons take ~30-60s to bootstrap Tor before Play connects.)

EOF

# Block until any child exits (then cleanup runs via trap).
wait -n
