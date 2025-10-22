#!/bin/bash

set -e

echo "🚀 Starting development environment..."

# Check if ClickHouse is running
if ! curl -s http://localhost:8123 > /dev/null 2>&1; then
  echo "⚠️  ClickHouse is not running. Starting it..."
  cd infrastructure/docker
  docker compose up -d clickhouse
  cd ../..
  echo "⏳ Waiting for ClickHouse to be ready..."
  sleep 5
fi

echo "✅ ClickHouse is running"

# Start Next.js dev server
echo "🌐 Starting Next.js dev server..."
pnpm dev

