# System Architecture

## Overview

The Solana Analytics Platform is a monorepo containing a real-time blockchain analytics system powered by Jetstreamer for data ingestion and Next.js for the frontend.

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Users / Browsers                        │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                   Next.js Frontend (Vercel)                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │   Pages      │  │  API Routes  │  │  Components  │     │
│  │   /programs  │  │  /api/*      │  │  UI Library  │     │
│  │   /network   │  │              │  │              │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                  ClickHouse Database                         │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  • program_invocations                              │   │
│  │  • jetstreamer_slot_status                          │   │
│  │  • transactions                                      │   │
│  └─────────────────────────────────────────────────────┘   │
└────────────────────────┬────────────────────────────────────┘
                         ▲
                         │
┌────────────────────────┴────────────────────────────────────┐
│              Jetstreamer Backend (Railway)                   │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  Rust Processing Pipeline                            │  │
│  │  • Epoch Processing                                   │  │
│  │  • Program Tracking Plugin                           │  │
│  │  • Data Aggregation                                   │  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│              Solana Blockchain (Archives)                    │
└─────────────────────────────────────────────────────────────┘
```

## Components

### 1. Frontend (Next.js)

**Location**: `apps/web/`

**Technology Stack**:
- Next.js 14 (App Router)
- React 18
- TypeScript
- Tailwind CSS + Shadcn UI
- TanStack Query (React Query)
- Recharts

**Responsibilities**:
- User interface
- API routes for data fetching
- Client-side state management
- Data visualization
- Rate limiting

**Key Features**:
- Server-side rendering (SSR)
- API routes with built-in caching
- Responsive design
- Real-time data updates via polling

### 2. Backend (Jetstreamer)

**Location**: `apps/streamer/`

**Technology Stack**:
- Rust 1.75+
- Jetstreamer framework
- ClickHouse client

**Responsibilities**:
- Fetch blockchain data from Solana archives
- Process transactions and slots
- Aggregate program metrics
- Write to ClickHouse

**Processing Flow**:
```
Epoch Request
    ↓
Download Archive
    ↓
Parse Blocks & Transactions
    ↓
Plugin Processing (Program Tracking)
    ↓
Aggregate Metrics
    ↓
Write to ClickHouse
```

### 3. Database (ClickHouse)

**Purpose**: OLAP database for fast analytical queries

**Schema**:

#### program_invocations
```sql
CREATE TABLE program_invocations (
    program_id FixedString(32),
    timestamp DateTime,
    count UInt64,
    error_count UInt64,
    total_cus UInt64,
    min_cus UInt64,
    max_cus UInt64
) ENGINE = MergeTree()
ORDER BY (program_id, timestamp);
```

#### jetstreamer_slot_status
```sql
CREATE TABLE jetstreamer_slot_status (
    slot UInt64,
    indexed_at DateTime,
    transaction_count UInt32,
    success_count UInt32,
    error_count UInt32
) ENGINE = MergeTree()
ORDER BY slot;
```

#### transactions
```sql
CREATE TABLE transactions (
    signature String,
    slot UInt64,
    block_time UInt64,
    success Bool,
    error String,
    fee UInt64,
    compute_units_consumed UInt64,
    programs_invoked Array(String)
) ENGINE = MergeTree()
ORDER BY (slot, signature);
```

### 4. Shared Packages

**packages/types/**: Shared TypeScript types
**packages/config/**: Shared configuration (ESLint, TypeScript)

## Data Flow

### Ingestion Flow (Jetstreamer → ClickHouse)

```
1. Jetstreamer starts with epoch number
2. Downloads epoch archive from Solana
3. Parses blocks and transactions
4. Program tracking plugin aggregates metrics
5. Batch insert into ClickHouse
6. Repeat for next epoch
```

### Query Flow (User → Frontend → ClickHouse)

```
1. User requests data (e.g., /programs)
2. Next.js API route receives request
3. Rate limiting check
4. Query ClickHouse with aggregations
5. Transform data to API format
6. Return JSON response
7. Frontend displays with React components
```

## Deployment Architecture

### Production Setup

```
┌──────────────────┐
│   Vercel CDN     │  Frontend (Next.js)
│   (Global Edge)  │  - Static assets
│                  │  - API routes
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ ClickHouse Cloud │  Database
│   (Regional)     │  - US/EU regions
│                  │  - Auto-scaling
└────────┬─────────┘
         ▲
         │
┌────────┴─────────┐
│    Railway       │  Backend (Jetstreamer)
│   (Containers)   │  - Docker deployment
│                  │  - Auto-restart
└──────────────────┘
```

## Scaling Considerations

### Frontend

- **Vercel**: Automatic edge caching and scaling
- **API Routes**: Cached at CDN edge
- **Rate Limiting**: In-memory (consider Redis for multi-instance)

### Backend

- **Horizontal**: Run multiple Jetstreamer instances for different epoch ranges
- **Vertical**: Increase CPU/memory for faster processing
- **Parallel**: Use `JETSTREAMER_THREADS` to control parallelism

### Database

- **ClickHouse**:
  - Use materialized views for complex aggregations
  - Partition tables by date
  - Use distributed tables for multi-node setups
  - Add indexes for frequently queried columns

## Performance Optimizations

### Frontend

1. **Server Components**: Default to React Server Components
2. **API Caching**: Use Next.js cache with `revalidate`
3. **Query Caching**: TanStack Query with stale time
4. **Code Splitting**: Dynamic imports for heavy components

### Backend

1. **Batch Processing**: Aggregate before inserting
2. **Connection Pooling**: Reuse ClickHouse connections
3. **Parallel Processing**: Multi-threaded epoch processing

### Database

1. **Query Optimization**:
   - Use `sum()` for aggregations
   - Leverage `ORDER BY` key in queries
   - Use `toStartOfHour()` for time-based grouping

2. **Indexes**:
   - Primary key on `(program_id, timestamp)`
   - Skip indexes for text columns

3. **Partitioning**:
   ```sql
   PARTITION BY toYYYYMM(timestamp)
   ```

## Security

### Frontend

- Environment variable validation (Zod)
- Input sanitization
- Rate limiting per IP
- CORS configuration

### Backend

- Secure ClickHouse connection (TLS)
- No exposed ports (internal only)
- Read-only database credentials for frontend

### Database

- Network isolation
- IP whitelisting
- Strong passwords
- Regular backups

## Monitoring & Observability

### Logs

- **Frontend**: Vercel logs
- **Backend**: Railway logs + stdout
- **Database**: ClickHouse system logs

### Metrics

- **Frontend**: Vercel Analytics (Core Web Vitals)
- **Backend**: Processing speed, error rates
- **Database**: Query performance, disk usage

### Alerts

- API error rate > 5%
- Database latency > 1s
- Streamer crashes

## Future Enhancements

1. **Real-time Updates**: WebSocket support
2. **Authentication**: User accounts and API keys
3. **Custom Dashboards**: User-created views
4. **More Plugins**: DeFi, NFT tracking
5. **Data Export**: CSV/JSON downloads
6. **GraphQL API**: Alternative to REST
7. **Mobile App**: React Native

## Development Workflow

```
Developer
    ↓
Git Push → GitHub
    ↓
    ├─→ Vercel (Auto-deploy frontend)
    └─→ Railway (Manual deploy backend)
```

## Cost Breakdown

### Monthly Estimates

- **Vercel**: Free tier (Hobby) or $20/month (Pro)
- **Railway**: $5-20/month (Jetstreamer container)
- **ClickHouse Cloud**: $50-200/month (depends on data volume)

**Total**: ~$55-240/month for production

## Technical Decisions

### Why Next.js?

- Built-in API routes
- Excellent developer experience
- Server components for performance
- Easy Vercel deployment

### Why Rust/Jetstreamer?

- High performance for data processing
- Memory safety
- Direct blockchain archive access
- Efficient parallel processing

### Why ClickHouse?

- Optimized for analytical queries
- Fast aggregations on large datasets
- Time-series data handling
- Excellent compression

### Why Monorepo?

- Shared types between frontend/backend
- Unified dependency management
- Easier development workflow
- Single source of truth

## References

- [Next.js Documentation](https://nextjs.org/docs)
- [Jetstreamer Repository](https://github.com/deanmlittle/jetstreamer)
- [ClickHouse Documentation](https://clickhouse.com/docs)
- [Vercel Platform](https://vercel.com)
- [Railway Documentation](https://docs.railway.app)

