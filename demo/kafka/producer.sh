#!/usr/bin/env bash
set -euo pipefail

BROKERS="${BROKERS:-redpanda:9092}"
TOPIC="${TOPIC:-ticks}"
TICKS_PER_BATCH="${TICKS_PER_BATCH:-8}"
SLEEP_SECONDS="${SLEEP_SECONDS:-0.20}"
LOG_EVERY_BATCHES="${LOG_EVERY_BATCHES:-10}"

symbols=(AAPL MSFT NVDA AMZN GOOGL META TSLA)
venues=(XNAS BATS EDGX)
prices=(17250 40820 91240 18410 17635 50215 24380)
batch_count=0
tick_count=0

rpk topic create "$TOPIC" --brokers "$BROKERS" >/dev/null 2>&1 || true
echo "tick-producer: publishing to ${TOPIC} via ${BROKERS} (${TICKS_PER_BATCH} ticks/batch, sleep ${SLEEP_SECONDS}s)"

while true; do
  batch_file="$(mktemp)"
  trap 'rm -f "$batch_file"' EXIT

  for ((i=0; i<TICKS_PER_BATCH; ++i)); do
    idx=$((RANDOM % ${#symbols[@]}))
    venue_idx=$((RANDOM % ${#venues[@]}))
    move=$((RANDOM % 31 - 15))
    size=$((100 + RANDOM % 4900))
    prices[$idx]=$((prices[$idx] + move))
    if (( prices[$idx] < 1000 )); then
      prices[$idx]=1000
    fi

    whole=$((prices[$idx] / 100))
    frac=$((prices[$idx] % 100))
    ts="$(date +%s%N)"

    printf '{"ts":%s,"symbol":"%s","venue":"%s","price":%d.%02d,"size":%d}\n' \
      "$ts" "${symbols[$idx]}" "${venues[$venue_idx]}" "$whole" "$frac" "$size" >>"$batch_file"
  done

  rpk topic produce "$TOPIC" --brokers "$BROKERS" <"$batch_file" >/dev/null
  batch_count=$((batch_count + 1))
  tick_count=$((tick_count + TICKS_PER_BATCH))
  if (( batch_count % LOG_EVERY_BATCHES == 0 )); then
    echo "tick-producer: sent ${tick_count} ticks across ${batch_count} batches"
  fi
  rm -f "$batch_file"
  trap - EXIT
  sleep "$SLEEP_SECONDS"
done
