#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
IBEX_BIN="${IBEX_BIN:-$ROOT/build-release/tools/ibex}"
PLUGIN_PATH="${PLUGIN_PATH:-$ROOT/build-release/tools}"
LOG_DIR="${LOG_DIR:-/tmp/ibex-kafka-avro-dashboard}"

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

wait_for_listen() {
  local port="$1"
  local pid="$2"
  local name="$3"
  local log_file="$LOG_DIR/${name}.log"

  for _ in $(seq 1 80); do
    if ! kill -0 "$pid" >/dev/null 2>&1; then
      echo "${name} stream exited before binding port ${port}" >&2
      tail -n 40 "$log_file" >&2 || true
      return 1
    fi
    if ss -ltn 2>/dev/null | grep -q ":${port}[[:space:]]"; then
      return 0
    fi
    sleep 0.25
  done

  echo "${name} stream did not bind port ${port} within the expected time" >&2
  tail -n 40 "$log_file" >&2 || true
  return 1
}

start_stream summary demo/kafka/kafka_ticks_avro.ibex
summary_pid="$STARTED_PID"
start_stream ohlc demo/kafka/kafka_ohlc_avro.ibex
ohlc_pid="$STARTED_PID"

cleanup() {
  kill "$summary_pid" "$ohlc_pid" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

wait_for_listen 8775 "$summary_pid" summary
wait_for_listen 8776 "$ohlc_pid" ohlc

cat <<EOF
Ibex Kafka Avro dashboard streams are running.

Summary websocket:
  ws://127.0.0.1:8775

OHLC websocket:
  ws://127.0.0.1:8776

Dashboard:
  demo/kafka/ws_dashboard_avro.html

Logs:
  $LOG_DIR/summary.log
  $LOG_DIR/ohlc.log

Press Ctrl-C to stop both Ibex processes.
EOF

wait "$summary_pid" "$ohlc_pid"
