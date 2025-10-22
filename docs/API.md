# API Documentation

## Base URL

- **Development**: `http://localhost:3000/api`
- **Production**: `https://your-app.vercel.app/api`

## Authentication

Currently no authentication required. Rate limiting applies to all endpoints.

## Rate Limiting

- **Limit**: 60 requests per minute per IP
- **Headers**:
  - `X-RateLimit-Limit`: Maximum requests per window
  - `X-RateLimit-Remaining`: Requests remaining
  - `X-RateLimit-Reset`: Reset time (ISO 8601)

## Response Format

### Success Response

```json
{
  "success": true,
  "data": { ... },
  "timestamp": "2024-01-15T10:30:00Z"
}
```

### Error Response

```json
{
  "success": false,
  "error": "Error message"
}
```

## Endpoints

### Health Check

Check API and database health.

**Endpoint**: `GET /api/health`

**Response**:
```json
{
  "status": "healthy",
  "timestamp": "2024-01-15T10:30:00Z",
  "services": {
    "database": {
      "status": "healthy",
      "latency": 50
    }
  }
}
```

### Programs

#### Get Top Programs

Get the most active programs.

**Endpoint**: `GET /api/programs`

**Query Parameters**:
- `range` (optional): Time range - `1h`, `24h`, `7d` (default: `24h`)
- `limit` (optional): Max results (default: 50, max: 100)

**Example Request**:
```bash
curl "http://localhost:3000/api/programs?range=24h&limit=10"
```

**Response**:
```json
{
  "success": true,
  "data": [
    {
      "program_id": "11111111111111111111111111111111",
      "total_transactions": 1250000,
      "total_errors": 125,
      "success_rate": 99.99,
      "avg_compute_units": 150000,
      "first_seen": "2024-01-14T10:00:00Z",
      "last_seen": "2024-01-15T10:30:00Z"
    }
  ],
  "timeRange": "24h",
  "count": 10
}
```

#### Get Program Details

Get detailed metrics for a specific program.

**Endpoint**: `GET /api/programs/:id`

**Path Parameters**:
- `id`: Program ID (Base58 encoded, 44 characters)

**Query Parameters**:
- `range` (optional): Time range - `24h`, `7d`, `30d` (default: `7d`)

**Example Request**:
```bash
curl "http://localhost:3000/api/programs/11111111111111111111111111111111?range=7d"
```

**Response**:
```json
{
  "success": true,
  "data": {
    "program_id": "11111111111111111111111111111111",
    "total_transactions": 8750000,
    "total_errors": 875,
    "success_rate": 99.99,
    "avg_compute_units": 150000,
    "min_compute_units": 50000,
    "max_compute_units": 500000,
    "first_seen": "2024-01-08T10:00:00Z",
    "last_seen": "2024-01-15T10:30:00Z",
    "hourly_stats": [
      {
        "hour": "2024-01-15T10:00:00Z",
        "transaction_count": 50000,
        "error_count": 5,
        "avg_compute_units": 150000
      }
    ]
  },
  "timeRange": "7d"
}
```

### Network

#### Get Network Stats

Get current network statistics.

**Endpoint**: `GET /api/network/stats`

**Example Request**:
```bash
curl "http://localhost:3000/api/network/stats"
```

**Response**:
```json
{
  "success": true,
  "data": {
    "total_slots": 12000,
    "total_transactions": 18000000,
    "avg_tps": 2500,
    "max_tps": 5000,
    "timestamp": "2024-01-15T10:30:00Z"
  },
  "timestamp": "2024-01-15T10:30:00Z"
}
```

#### Get TPS History

Get historical TPS data.

**Endpoint**: `GET /api/network/tps`

**Query Parameters**:
- `hours` (optional): Number of hours (default: 24, max: 168)

**Example Request**:
```bash
curl "http://localhost:3000/api/network/tps?hours=24"
```

**Response**:
```json
{
  "success": true,
  "data": [
    {
      "timestamp": "2024-01-15T10:00:00Z",
      "tps": 2500,
      "slot": 12345678
    }
  ],
  "hours": 24,
  "count": 1440
}
```

### Transactions

#### Search Transactions

Search for transactions by criteria.

**Endpoint**: `POST /api/transactions`

**Request Body**:
```json
{
  "program_id": "11111111111111111111111111111111",
  "start_slot": 100000,
  "end_slot": 200000,
  "limit": 20
}
```

**Parameters**:
- `signature` (optional): Transaction signature
- `program_id` (optional): Program ID
- `start_slot` (optional): Starting slot number
- `end_slot` (optional): Ending slot number
- `limit` (optional): Max results (default: 20, max: 50)

**Example Request**:
```bash
curl -X POST "http://localhost:3000/api/transactions" \
  -H "Content-Type: application/json" \
  -d '{"program_id":"11111111111111111111111111111111","limit":10}'
```

**Response**:
```json
{
  "success": true,
  "data": [
    {
      "signature": "5J8...",
      "slot": 150000,
      "block_time": 1705315800,
      "success": true,
      "error": null,
      "fee": 5000,
      "compute_units_consumed": 150000,
      "programs_invoked": ["11111111111111111111111111111111"]
    }
  ],
  "count": 10,
  "has_more": true
}
```

#### Get Transaction Details

Get details for a specific transaction.

**Endpoint**: `GET /api/transactions/:signature`

**Path Parameters**:
- `signature`: Transaction signature (Base58 encoded)

**Example Request**:
```bash
curl "http://localhost:3000/api/transactions/5J8..."
```

**Response**:
```json
{
  "success": true,
  "data": {
    "signature": "5J8...",
    "slot": 150000,
    "block_time": 1705315800,
    "success": true,
    "error": null,
    "fee": 5000,
    "compute_units_consumed": 150000,
    "programs_invoked": ["11111111111111111111111111111111"]
  }
}
```

## Error Codes

| Status | Description |
|--------|-------------|
| 200 | Success |
| 400 | Bad Request - Invalid parameters |
| 404 | Not Found - Resource doesn't exist |
| 429 | Too Many Requests - Rate limit exceeded |
| 500 | Internal Server Error |
| 503 | Service Unavailable - Database down |

## Common Error Responses

### Invalid Parameters (400)

```json
{
  "success": false,
  "error": "Invalid parameters"
}
```

### Rate Limit Exceeded (429)

```json
{
  "success": false,
  "error": "Too many requests"
}
```

### Resource Not Found (404)

```json
{
  "success": false,
  "error": "Program not found"
}
```

### Server Error (500)

```json
{
  "success": false,
  "error": "Failed to fetch programs"
}
```

## Usage Examples

### JavaScript/TypeScript

```typescript
async function getTopPrograms() {
  const response = await fetch('/api/programs?range=24h');
  const data = await response.json();
  
  if (data.success) {
    return data.data;
  } else {
    throw new Error(data.error);
  }
}
```

### Python

```python
import requests

def get_top_programs():
    response = requests.get('http://localhost:3000/api/programs?range=24h')
    data = response.json()
    
    if data['success']:
        return data['data']
    else:
        raise Exception(data['error'])
```

### cURL

```bash
# Get top programs
curl "http://localhost:3000/api/programs?range=24h"

# Get program details
curl "http://localhost:3000/api/programs/11111111111111111111111111111111"

# Get network stats
curl "http://localhost:3000/api/network/stats"

# Search transactions
curl -X POST "http://localhost:3000/api/transactions" \
  -H "Content-Type: application/json" \
  -d '{"program_id":"11111111111111111111111111111111"}'
```

## Best Practices

1. **Respect Rate Limits**: Check response headers and implement exponential backoff
2. **Cache Responses**: Use `staleTime` with React Query or similar
3. **Error Handling**: Always check `success` field before accessing `data`
4. **Pagination**: Use `limit` parameter to control response size
5. **Time Ranges**: Start with smaller ranges (1h, 24h) before fetching larger datasets

## Changelog

### v1.0.0 (2024-01-15)

- Initial API release
- Programs endpoints
- Network endpoints
- Transactions endpoints
- Health check endpoint

