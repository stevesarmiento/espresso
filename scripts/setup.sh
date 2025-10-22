#!/bin/bash

set -e

echo "🚀 Setting up Solana Analytics Platform..."

# Check prerequisites
command -v node >/dev/null 2>&1 || { echo "❌ Node.js is required but not installed. Aborting." >&2; exit 1; }
command -v pnpm >/dev/null 2>&1 || { echo "❌ pnpm is required but not installed. Aborting." >&2; exit 1; }
command -v docker >/dev/null 2>&1 || { echo "⚠️  Docker not found. You'll need it to run ClickHouse locally." >&2; }

echo "✅ Prerequisites check passed"

# Install Node dependencies
echo "📦 Installing dependencies..."
pnpm install

# Copy environment file if it doesn't exist
if [ ! -f apps/web/.env.local ]; then
  echo "📝 Creating .env.local from example..."
  cp apps/web/.env.example apps/web/.env.local
  echo "⚠️  Please update apps/web/.env.local with your ClickHouse credentials"
fi

# Start ClickHouse with Docker Compose
if command -v docker >/dev/null 2>&1; then
  echo "🐳 Starting ClickHouse..."
  cd infrastructure/docker
  docker compose up -d clickhouse
  cd ../..
  echo "✅ ClickHouse started on http://localhost:8123"
else
  echo "⚠️  Docker not available. Please start ClickHouse manually."
fi

echo ""
echo "✅ Setup complete!"
echo ""
echo "Next steps:"
echo "  1. Update apps/web/.env.local with your configuration"
echo "  2. Start the streamer: pnpm start:streamer"
echo "  3. Start the web app: pnpm dev"
echo ""

