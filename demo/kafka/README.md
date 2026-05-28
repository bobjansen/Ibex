# Kafka streaming demo

An end-to-end demo of Ibex consuming a live Kafka feed and pushing aggregated
results to a browser dashboard over WebSockets.

## What it does

A Docker stack (Redpanda + Schema Registry) runs two synthetic tick producers
that publish random equity trades:

- `ticks` — JSON messages of the form
  `{"ts":...,"symbol":"AAPL","venue":"XNAS","price":172.53,"size":900}`
- `ticks_avro` — the same data as Avro, with the schema in [`tick.avsc`](tick.avsc)
  registered in the Schema Registry

Ibex reads these topics with `kafka_recv` / `kafka_recv_avro`, runs a streaming
transform, and serves the output on a WebSocket via `ws_send`. Two jobs run per
format:

| Job | Ibex script | Transform | WebSocket |
| --- | --- | --- | --- |
| JSON summary | [`kafka_ticks.ibex`](kafka_ticks.ibex) | grouped trade counts by `symbol`, `venue` | `ws://127.0.0.1:8765` |
| JSON OHLC | [`kafka_ohlc.ibex`](kafka_ohlc.ibex) | 5-second OHLC bars by `symbol` | `ws://127.0.0.1:8766` |
| Avro summary | [`kafka_ticks_avro.ibex`](kafka_ticks_avro.ibex) | grouped trade counts by `symbol`, `venue` | `ws://127.0.0.1:8775` |
| Avro OHLC | [`kafka_ohlc_avro.ibex`](kafka_ohlc_avro.ibex) | 5-second OHLC bars by `symbol` | `ws://127.0.0.1:8776` |

The dashboards ([`ws_dashboard.html`](ws_dashboard.html),
[`ws_dashboard_avro.html`](ws_dashboard_avro.html)) connect to these feeds and
render the live tables.

## Requirements

- Docker with the Compose plugin (`docker compose`)
- A release build of Ibex at `build-release/tools/ibex` (see the repo root
  README for build instructions). Override with `IBEX_BIN` / `PLUGIN_PATH` if it
  lives elsewhere.

## How to run

Run all commands from the repository root.

1. Start the Kafka stack and producers:

   ```bash
   demo/kafka/demo-kafka.sh
   ```

   This brings up Redpanda on `localhost:19092`, the Schema Registry on
   `http://localhost:18081`, and both tick producers.

2. In another terminal, start the Ibex streams. For JSON:

   ```bash
   demo/kafka/run-kafka-dashboard.sh
   ```

   or for Avro:

   ```bash
   demo/kafka/run-kafka-avro-dashboard.sh
   ```

   Each launcher runs both the summary and OHLC jobs and keeps them alive until
   you press Ctrl-C. Logs go to `/tmp/ibex-kafka-dashboard/` (or
   `/tmp/ibex-kafka-avro-dashboard/`).

3. Open the dashboard in a browser — `ws_dashboard.html` for JSON or
   `ws_dashboard_avro.html` for Avro.

   Or watch the feeds in the terminal instead:

   ```bash
   python3 demo/kafka/ws_client.py            # summary on 8765
   python3 demo/kafka/ohlc_ws_client.py       # OHLC on 8766
   python3 demo/kafka/ws_client.py --port 8775   # Avro summary
   python3 demo/kafka/ohlc_ws_client.py --port 8776   # Avro OHLC
   ```

## Stopping

Press Ctrl-C in the launcher terminal to stop the Ibex streams, then tear down
the stack:

```bash
docker compose -f demo/kafka/docker-compose.yml down
```

To follow producer output:

```bash
docker compose -f demo/kafka/docker-compose.yml logs -f tick-producer
```
