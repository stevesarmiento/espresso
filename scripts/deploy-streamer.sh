#!/bin/bash

set -e

echo "🚂 Deploying Jetstreamer to Railway..."

# Check if Railway CLI is installed
if ! command -v railway >/dev/null 2>&1; then
  echo "❌ Railway CLI not found. Install it with: npm install -g @railway/cli"
  exit 1
fi

# Check if logged in
if ! railway whoami >/dev/null 2>&1; then
  echo "❌ Not logged in to Railway. Run: railway login"
  exit 1
fi

echo "✅ Railway CLI ready"

# Build and deploy
echo "📦 Building and deploying..."

cd apps/streamer

# Railway will use the Dockerfile at infrastructure/docker/Dockerfile.streamer
railway up --dockerfile ../../infrastructure/docker/Dockerfile.streamer

echo "✅ Deployment complete!"
echo ""
echo "Check your deployment: railway open"

