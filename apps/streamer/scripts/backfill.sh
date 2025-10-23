#!/bin/bash

set -e

if [ -z "$1" ]; then
  echo "Usage: $0 <start_epoch> [end_epoch]"
  echo "Example: $0 500 600"
  exit 1
fi

START_EPOCH=$1
END_EPOCH=${2:-$(date +%s | awk '{print int($1/432000)}')}

echo "🔄 Backfilling data from epoch $START_EPOCH to $END_EPOCH..."

JETSTREAMER_THREADS=${JETSTREAMER_THREADS:-16}
JETSTREAMER_CLICKHOUSE_MODE=${JETSTREAMER_CLICKHOUSE_MODE:-auto}

for ((epoch=$START_EPOCH; epoch<=$END_EPOCH; epoch++)); do
  echo "📦 Processing epoch $epoch..."
  
  JETSTREAMER_THREADS=$JETSTREAMER_THREADS \
  JETSTREAMER_CLICKHOUSE_MODE=$JETSTREAMER_CLICKHOUSE_MODE \
  cargo run --release -- $epoch
  
  echo "✅ Completed epoch $epoch"
done

echo "✅ Backfill complete!"

