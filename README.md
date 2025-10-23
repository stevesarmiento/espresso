# Solana Analytics Platform

A community-facing analytics platform for Solana blockchain data, powered by Jetstreamer.

## 🏗️ Architecture

This monorepo contains:

- **`apps/web`** - Next.js frontend for the community-facing analytics dashboard
- **`apps/streamer`** - Jetstreamer backend (Rust) for real-time blockchain data ingestion
- **`packages/types`** - Shared TypeScript types
- **`packages/config`** - Shared configuration (ESLint, TypeScript)
- **`infrastructure`** - Docker configurations and deployment files
- **`docs`** - Comprehensive documentation

## 🚀 Quick Start

### Prerequisites

- Node.js 20+
- Rust 1.75+
- pnpm 8+
- **ClickHouse** (choose one):
  - Docker Desktop (for local development)
  - [ClickHouse Cloud](https://clickhouse.cloud) (free tier available)
  - Railway/hosted instance

### Local Development

1. **Clone the repository**
   ```bash
   git clone <your-repo>
   cd solana-analytics-platform
   ```

2. **Install dependencies**
   ```bash
   pnpm install
   ```

3. **Set up environment variables**
   ```bash
   cp .env.example .env.local
   # Edit .env.local with your ClickHouse credentials
   ```

4. **Set up ClickHouse**
   
   **Option A: Local with Docker**
   ```bash
   cd infrastructure/docker
   docker compose up -d clickhouse
   cd ../..
   ```
   
   **Option B: ClickHouse Cloud** (no Docker needed)
   ```bash
   # Sign up at https://clickhouse.cloud
   # Create a free tier service
   # Update apps/web/.env.local with your connection details
   ```
   
   **Option C: Railway**
   ```bash
   railway add
   # Select ClickHouse, then update .env.local with connection details
   ```

5. **Start the Jetstreamer backend**
   ```bash
   pnpm start:streamer
   ```

6. **Start the Next.js frontend** (in a new terminal)
   ```bash
   pnpm dev
   ```

7. **Open your browser**
   ```
   http://localhost:3000
   ```

## 📁 Project Structure

```
.
├── apps/
│   ├── web/                    # Next.js frontend
│   │   ├── app/               # App Router pages & API routes
│   │   ├── components/        # React components
│   │   ├── lib/               # Utilities & ClickHouse client
│   │   ├── hooks/             # Custom React hooks
│   │   └── types/             # TypeScript types
│   └── streamer/              # Jetstreamer (Rust)
│       ├── jetstreamer-firehose/
│       ├── jetstreamer-plugin/
│       ├── jetstreamer-utils/
│       └── src/
├── packages/
│   ├── types/                 # Shared TypeScript types
│   └── config/                # Shared configs
├── infrastructure/
│   └── docker/                # Docker configs
├── docs/                      # Documentation
└── scripts/                   # Helper scripts
```

## 🛠️ Development

### Available Scripts

- `pnpm dev` - Start Next.js dev server
- `pnpm build` - Build all packages
- `pnpm build:web` - Build Next.js app
- `pnpm build:streamer` - Build Rust streamer
- `pnpm lint` - Run linters
- `pnpm type-check` - TypeScript type checking

### Tech Stack

**Frontend:**
- Next.js 14 (App Router)
- React 18
- TypeScript
- Tailwind CSS
- Shadcn UI
- TanStack Query
- Recharts

**Backend:**
- Rust
- Jetstreamer
- ClickHouse

## 🚢 Deployment

### Frontend (Vercel)
```bash
cd apps/web
vercel deploy
```

### Backend (Railway)
```bash
railway up
```

See [DEPLOYMENT.md](./docs/DEPLOYMENT.md) for detailed deployment instructions.

## 📖 Documentation

- [API Documentation](./docs/API.md)
- [Architecture Overview](./docs/ARCHITECTURE.md)
- [Development Guide](./docs/DEVELOPMENT.md)
- [Deployment Guide](./docs/DEPLOYMENT.md)

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📄 License

MIT License - see LICENSE file for details

## 🔗 Links

- [Jetstreamer Documentation](./apps/streamer/README.md)
- [Next.js Documentation](https://nextjs.org/docs)
- [ClickHouse Documentation](https://clickhouse.com/docs)

