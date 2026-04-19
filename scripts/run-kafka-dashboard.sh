#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IBEX_BIN="${IBEX_BIN:-$ROOT/build-release/tools/ibex}"
PLUGIN_PATH="${PLUGIN_PATH:-$ROOT/build-release/tools}"
LOG_DIR="${LOG_DIR:-/tmp/ibex-kafka-dashboard}"

mkdir -p "$LOG_DIR"

if [[ ! -x "$IBEX_BIN" ]]; then
  echo "ibex binary not found: $IBEX_BIN" >&2
  exit 1
fi

STARTED_PID=""

start_stream() {
  local name="$1"
  local script_path="$2"
  local log_file="$LOG_DIR/${name}.log"
  : >"$log_file"
  (
    cd "$ROOT"
    printf ':load %s\n' "$script_path" | "$IBEX_BIN" --plugin-path "$PLUGIN_PATH"
  ) >"$log_file" 2>&1 &
  STARTED_PID="$!"
}

start_stream summary examples/kafka_ticks.ibex
summary_pid="$STARTED_PID"
start_stream ohlc examples/kafka_ohlc.ibex
ohlc_pid="$STARTED_PID"

cleanup() {
  kill "$summary_pid" "$ohlc_pid" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

cat <<EOF
Ibex Kafka dashboard streams are running.

Summary websocket:
  ws://127.0.0.1:8765

OHLC websocket:
  ws://127.0.0.1:8766

Dashboard:
  demo/kafka/ws_dashboard.html

Logs:
  $LOG_DIR/summary.log
  $LOG_DIR/ohlc.log

Press Ctrl-C to stop both Ibex processes.
EOF

wait "$summary_pid" "$ohlc_pid"
