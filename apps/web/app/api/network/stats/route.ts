import { NextRequest, NextResponse } from 'next/server';
import { getNetworkStats } from '@/lib/clickhouse/queries';
import { rateLimit, rateLimitHeaders } from '@/lib/utils/rate-limit';

export const dynamic = 'force-dynamic';
export const runtime = 'nodejs';

export async function GET(request: NextRequest) {
  // Rate limiting
  const limitResult = await rateLimit(request);
  if (!limitResult.success) {
    return NextResponse.json(
      { success: false, error: 'Too many requests' },
      {
        status: 429,
        headers: rateLimitHeaders(limitResult),
      }
    );
  }

  try {
    const stats = await getNetworkStats();

    return NextResponse.json(
      {
        success: true,
        data: stats,
        timestamp: new Date().toISOString(),
      },
      { headers: rateLimitHeaders(limitResult) }
    );
  } catch (error) {
    console.error('Error fetching network stats:', error);

    return NextResponse.json(
      { success: false, error: 'Failed to fetch network stats' },
      { status: 500 }
    );
  }
}

