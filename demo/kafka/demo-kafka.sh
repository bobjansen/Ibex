#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="$ROOT/demo/kafka/docker-compose.yml"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required" >&2
  exit 1
fi

if ! docker compose version >/dev/null 2>&1; then
  echo "docker compose plugin is required" >&2
  exit 1
fi

docker compose -f "$COMPOSE_FILE" up -d

cat <<'EOF'

Redpanda demo is starting.

Broker:
  localhost:19092

Schema Registry:
  http://localhost:18081

Topic:
  ticks

Avro topic:
  ticks_avro

Dashboard websocket:
  ws://127.0.0.1:8765

OHLC websocket:
  ws://127.0.0.1:8766

Avro dashboard websocket:
  ws://127.0.0.1:8775

Avro OHLC websocket:
  ws://127.0.0.1:8776

Start both Ibex websocket streams:
  demo/kafka/run-kafka-dashboard.sh

Start both Ibex Avro websocket streams:
  demo/kafka/run-kafka-avro-dashboard.sh

Open dashboard:
  demo/kafka/ws_dashboard.html

Open Avro dashboard:
  demo/kafka/ws_dashboard_avro.html

Or watch the summary feed in the terminal:
  python3 demo/kafka/ws_client.py

Or watch the OHLC feed in the terminal:
  python3 demo/kafka/ohlc_ws_client.py

Notes:
  - The checked-in demo does not impose a 10s runtime limit.
  - Any short timeout previously shown was only used for one-off verification.
  - 'poll_timeout_ms=100' in the Kafka source is just the consumer poll wait,
    not a lifetime timeout for the stream.

To stop the demo:
  docker compose -f demo/kafka/docker-compose.yml down

To follow producer logs:
  docker compose -f demo/kafka/docker-compose.yml logs -f tick-producer

EOF
