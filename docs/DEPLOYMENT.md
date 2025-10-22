# Deployment Guide

## Overview

The Solana Analytics Platform consists of two main components:

1. **Frontend (Next.js)** - Deploy to Vercel
2. **Backend (Jetstreamer)** - Deploy to Railway

## Prerequisites

- Vercel account
- Railway account
- ClickHouse Cloud account (or self-hosted ClickHouse)

## Deploying the Frontend (Vercel)

### Option 1: Vercel CLI

1. **Install Vercel CLI**
   ```bash
   npm install -g vercel
   ```

2. **Login to Vercel**
   ```bash
   vercel login
   ```

3. **Deploy**
   ```bash
   cd apps/web
   vercel
   ```

4. **Set Environment Variables**
   ```bash
   vercel env add CLICKHOUSE_HOST
   vercel env add CLICKHOUSE_USER
   vercel env add CLICKHOUSE_PASSWORD
   vercel env add CLICKHOUSE_DATABASE
   ```

### Option 2: GitHub Integration

1. Connect your repository to Vercel
2. Configure project:
   - **Root Directory**: `apps/web`
   - **Build Command**: `cd ../.. && pnpm install && pnpm --filter web build`
   - **Output Directory**: `.next`

3. Add environment variables in Vercel dashboard:
   ```
   CLICKHOUSE_HOST=https://your-clickhouse.cloud
   CLICKHOUSE_USER=default
   CLICKHOUSE_PASSWORD=your-password
   CLICKHOUSE_DATABASE=default
   ```

## Deploying the Backend (Railway)

### Setup ClickHouse

**Option A: ClickHouse Cloud**

1. Sign up at https://clickhouse.cloud
2. Create a new service
3. Note the connection details

**Option B: Railway ClickHouse**

```bash
railway add
# Select ClickHouse from the database options
```

### Deploy Jetstreamer

1. **Install Railway CLI**
   ```bash
   npm install -g @railway/cli
   ```

2. **Login**
   ```bash
   railway login
   ```

3. **Initialize Project**
   ```bash
   railway init
   ```

4. **Set Environment Variables**
   ```bash
   railway variables set JETSTREAMER_CLICKHOUSE_DSN=<your-clickhouse-url>
   railway variables set JETSTREAMER_CLICKHOUSE_MODE=remote
   railway variables set JETSTREAMER_THREADS=8
   railway variables set LOG_LEVEL=info
   railway variables set START_EPOCH=500
   ```

5. **Deploy**
   ```bash
   ./scripts/deploy-streamer.sh
   # or manually:
   cd apps/streamer
   railway up --dockerfile ../../infrastructure/docker/Dockerfile.streamer
   ```

### Railway Configuration File

Create `railway.json` in the root:

```json
{
  "$schema": "https://railway.app/railway.schema.json",
  "build": {
    "builder": "DOCKERFILE",
    "dockerfilePath": "infrastructure/docker/Dockerfile.streamer"
  },
  "deploy": {
    "startCommand": "jetstreamer 500",
    "restartPolicyType": "ON_FAILURE",
    "restartPolicyMaxRetries": 10
  }
}
```

## Environment Variables

### Frontend (Next.js)

| Variable | Description | Required |
|----------|-------------|----------|
| `CLICKHOUSE_HOST` | ClickHouse HTTP endpoint | Yes |
| `CLICKHOUSE_USER` | ClickHouse username | Yes |
| `CLICKHOUSE_PASSWORD` | ClickHouse password | No |
| `CLICKHOUSE_DATABASE` | Database name | Yes |
| `RATE_LIMIT_REQUESTS_PER_MINUTE` | API rate limit | No |
| `NEXT_PUBLIC_APP_URL` | Public app URL | No |

### Backend (Jetstreamer)

| Variable | Description | Default |
|----------|-------------|---------|
| `JETSTREAMER_CLICKHOUSE_DSN` | ClickHouse connection string | Required |
| `JETSTREAMER_CLICKHOUSE_MODE` | `auto`, `embedded`, or `remote` | `auto` |
| `JETSTREAMER_THREADS` | Number of processing threads | `8` |
| `LOG_LEVEL` | Logging level | `info` |
| `START_EPOCH` | Starting epoch for processing | Required |

## Post-Deployment

### 1. Verify Frontend

```bash
curl https://your-app.vercel.app/api/health
```

Expected response:
```json
{
  "status": "healthy",
  "timestamp": "...",
  "services": {
    "database": {
      "status": "healthy",
      "latency": 50
    }
  }
}
```

### 2. Verify Backend

Check Railway logs:
```bash
railway logs
```

### 3. Monitor ClickHouse

```bash
# Check data ingestion
clickhouse-client --query "SELECT count() FROM program_invocations"
clickhouse-client --query "SELECT count() FROM jetstreamer_slot_status"
```

## Scaling

### Frontend

Vercel automatically scales based on traffic.

### Backend

**Railway:**

1. Go to your project settings
2. Adjust resources (CPU/Memory)
3. Consider running multiple instances for different epoch ranges

**Self-hosted:**

Use Kubernetes with the provided manifests:

```bash
kubectl apply -f infrastructure/kubernetes/
```

## Monitoring

### Logs

**Frontend:**
```bash
vercel logs <deployment-url>
```

**Backend:**
```bash
railway logs
```

### Metrics

Set up monitoring:

1. **Vercel Analytics** - Built-in
2. **Railway Metrics** - Built-in dashboard
3. **ClickHouse Monitoring** - Use Grafana + Prometheus

## Backup & Recovery

### ClickHouse Backup

```bash
# Manual backup
clickhouse-client --query "BACKUP TABLE program_invocations TO Disk('backups', 'backup.zip')"

# Restore
clickhouse-client --query "RESTORE TABLE program_invocations FROM Disk('backups', 'backup.zip')"
```

### Automated Backups

Set up daily backups in ClickHouse Cloud or use:

```bash
# Cron job
0 2 * * * /path/to/backup-script.sh
```

## Troubleshooting

### Frontend Issues

**Problem**: 500 errors on API routes

**Solution**:
1. Check ClickHouse connection
2. Verify environment variables
3. Check Vercel function logs

### Backend Issues

**Problem**: Streamer crashes

**Solution**:
1. Check memory usage
2. Reduce `JETSTREAMER_THREADS`
3. Check ClickHouse connectivity

### Database Issues

**Problem**: Slow queries

**Solution**:
1. Add indexes
2. Optimize queries
3. Increase ClickHouse resources

## Cost Optimization

1. **Vercel**: Free tier supports most use cases
2. **Railway**: $5-20/month for streamer
3. **ClickHouse Cloud**: Start with smallest tier (~$50/month)

## Security

1. Use strong passwords
2. Enable IP whitelisting on ClickHouse
3. Use environment variables for secrets
4. Enable HTTPS (automatic on Vercel)
5. Regularly update dependencies

## Updates

### Frontend

```bash
git push
# Vercel auto-deploys from main branch
```

### Backend

```bash
./scripts/deploy-streamer.sh
# or
railway up
```

## Rollback

### Vercel

```bash
vercel rollback
```

### Railway

Use Railway dashboard to rollback to previous deployment.

## Support

- [Vercel Documentation](https://vercel.com/docs)
- [Railway Documentation](https://docs.railway.app)
- [ClickHouse Cloud Support](https://clickhouse.com/support)

