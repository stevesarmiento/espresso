#!/bin/bash

set -e

echo "🚀 Starting Jetstreamer in production mode..."

# Configuration
THREADS=${JETSTREAMER_THREADS:-8}
CLICKHOUSE_MODE=${JETSTREAMER_CLICKHOUSE_MODE:-auto}
LOG_LEVEL=${LOG_LEVEL:-info}

# Get the current epoch or start from a specific point
CURRENT_EPOCH=$(date +%s | awk '{print int($1/432000)}')
START_EPOCH=${START_EPOCH:-$CURRENT_EPOCH}

echo "📊 Configuration:"
echo "  - Threads: $THREADS"
echo "  - Starting Epoch: $START_EPOCH"
echo "  - ClickHouse Mode: $CLICKHOUSE_MODE"
echo "  - Log Level: $LOG_LEVEL"

# Run with continuous mode
JETSTREAMER_THREADS=$THREADS \
JETSTREAMER_CLICKHOUSE_MODE=$CLICKHOUSE_MODE \
LOG_LEVEL=$LOG_LEVEL \
cargo run --release -- $START_EPOCH

