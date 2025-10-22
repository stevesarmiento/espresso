# Development Guide

## Prerequisites

- Node.js 20+
- Rust 1.75+
- pnpm 8+
- Docker & Docker Compose

## Initial Setup

1. **Clone the repository**
   ```bash
   git clone <your-repo-url>
   cd espresso
   ```

2. **Run setup script**
   ```bash
   chmod +x scripts/setup.sh
   ./scripts/setup.sh
   ```

3. **Configure environment variables**
   ```bash
   # Edit apps/web/.env.local with your ClickHouse credentials
   vim apps/web/.env.local
   ```

## Local Development

### Start ClickHouse

```bash
cd infrastructure/docker
docker compose up -d clickhouse
```

### Start the Next.js Frontend

```bash
pnpm dev
# or
./scripts/dev.sh
```

Visit http://localhost:3000

### Start the Jetstreamer Backend

```bash
pnpm start:streamer
# or
cd apps/streamer
./scripts/start-production.sh
```

## Project Structure

```
.
├── apps/
│   ├── web/                    # Next.js frontend
│   └── streamer/              # Jetstreamer (Rust)
├── packages/
│   ├── types/                 # Shared TypeScript types
│   └── config/                # Shared configs
├── infrastructure/
│   └── docker/                # Docker configs
├── docs/                      # Documentation
└── scripts/                   # Helper scripts
```

## Available Commands

### Root Level

- `pnpm dev` - Start Next.js dev server
- `pnpm build` - Build all packages
- `pnpm lint` - Run linters
- `pnpm type-check` - TypeScript type checking

### Web App

```bash
cd apps/web
pnpm dev          # Development server
pnpm build        # Production build
pnpm start        # Start production server
pnpm lint         # Run ESLint
pnpm type-check   # TypeScript check
```

### Streamer

```bash
cd apps/streamer
cargo build --release                    # Build release
cargo run -- <epoch>                     # Run specific epoch
./scripts/start-production.sh            # Production mode
./scripts/backfill.sh <start> [end]     # Backfill data
```

## Database Schema

The Jetstreamer creates the following tables in ClickHouse:

- `program_invocations` - Program transaction metrics
- `jetstreamer_slot_status` - Slot and TPS data
- `transactions` - Individual transaction records

## Adding New Features

### Add a New Page

1. Create page in `apps/web/app/your-page/page.tsx`
2. Add navigation link in `apps/web/components/layout/header.tsx`
3. Create API route if needed in `apps/web/app/api/your-endpoint/route.ts`

### Add a New Query

1. Define query in `apps/web/lib/clickhouse/queries.ts`
2. Create React hook in `apps/web/hooks/use-your-hook.ts`
3. Use in component

### Add a Custom Plugin

1. Create plugin in `apps/streamer/plugins/your-plugin/`
2. Implement the plugin trait
3. Register in `apps/streamer/src/main.rs`

## Troubleshooting

### ClickHouse Connection Issues

```bash
# Check if ClickHouse is running
curl http://localhost:8123

# View logs
docker logs solana-analytics-clickhouse

# Restart
cd infrastructure/docker
docker compose restart clickhouse
```

### Build Errors

```bash
# Clean and reinstall
pnpm clean
rm -rf node_modules
pnpm install

# Rebuild Rust
cd apps/streamer
cargo clean
cargo build --release
```

### Port Already in Use

```bash
# Find process using port 3000
lsof -i :3000
kill -9 <PID>
```

## Testing

```bash
# Run all tests
pnpm test

# Run specific workspace tests
pnpm --filter web test
```

## Code Style

- TypeScript: ESLint + Prettier
- Rust: `cargo fmt` + `cargo clippy`

```bash
# Format code
pnpm format
cd apps/streamer && cargo fmt
```

## Contributing

1. Create a feature branch
2. Make your changes
3. Run linters and tests
4. Submit a pull request

## Resources

- [Next.js Documentation](https://nextjs.org/docs)
- [ClickHouse Documentation](https://clickhouse.com/docs)
- [Jetstreamer Documentation](../apps/streamer/README.md)

