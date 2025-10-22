import { NextResponse } from 'next/server';
import { healthCheck } from '@/lib/clickhouse/client';

export const dynamic = 'force-dynamic';
export const runtime = 'nodejs';

export async function GET() {
  try {
    const dbHealth = await healthCheck();

    const status = dbHealth.healthy ? 'healthy' : 'degraded';

    return NextResponse.json({
      status,
      timestamp: new Date().toISOString(),
      services: {
        database: dbHealth.healthy
          ? { status: 'healthy', latency: dbHealth.latency }
          : { status: 'down', error: dbHealth.error },
      },
    });
  } catch (error) {
    console.error('Health check error:', error);

    return NextResponse.json(
      {
        status: 'down',
        timestamp: new Date().toISOString(),
        error: error instanceof Error ? error.message : 'Unknown error',
      },
      { status: 503 }
    );
  }
}

