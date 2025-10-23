import { NextRequest, NextResponse } from 'next/server';
import { getTPSHistory } from '@/lib/clickhouse/queries';
import { rateLimit, rateLimitHeaders } from '@/lib/utils/rate-limit';
import { z } from 'zod';

export const dynamic = 'force-dynamic';
export const runtime = 'nodejs';

const hoursSchema = z.coerce.number().int().positive().max(168).default(24);

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
    const searchParams = request.nextUrl.searchParams;
    const hours = hoursSchema.parse(searchParams.get('hours'));

    const tpsData = await getTPSHistory(hours);

    return NextResponse.json(
      {
        success: true,
        data: tpsData,
        hours,
        count: tpsData.length,
      },
      { headers: rateLimitHeaders(limitResult) }
    );
  } catch (error) {
    console.error('Error fetching TPS history:', error);

    if (error instanceof Error && error.name === 'ZodError') {
      return NextResponse.json(
        { success: false, error: 'Invalid hours parameter' },
        { status: 400 }
      );
    }

    return NextResponse.json(
      { success: false, error: 'Failed to fetch TPS history' },
      { status: 500 }
    );
  }
}

