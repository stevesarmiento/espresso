import { NextRequest, NextResponse } from 'next/server';
import { getProgramById, getProgramHourlyStats } from '@/lib/clickhouse/queries';
import { rateLimit, rateLimitHeaders } from '@/lib/utils/rate-limit';
import { programIdSchema, timeRangeSchema } from '@/lib/utils/validation';

export const dynamic = 'force-dynamic';
export const runtime = 'nodejs';

export async function GET(
  request: NextRequest,
  { params }: { params: { id: string } }
) {
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
    // Normalize and validate program ID (uppercase hex)
    const normalizedId = params.id.toUpperCase();
    const programId = programIdSchema.parse(normalizedId);
    
    const searchParams = request.nextUrl.searchParams;
    const timeRange = timeRangeSchema.catch('7d').parse(searchParams.get('range'));

    // Fetch program details and hourly stats
    const [program, hourlyStats] = await Promise.all([
      getProgramById(programId, timeRange),
      getProgramHourlyStats(programId, 24),
    ]);

    if (!program) {
      return NextResponse.json(
        { success: false, error: 'Program not found' },
        { status: 404 }
      );
    }

    return NextResponse.json(
      {
        success: true,
        data: {
          ...program,
          hourly_stats: hourlyStats,
        },
        timeRange,
      },
      { headers: rateLimitHeaders(limitResult) }
    );
  } catch (error) {
    console.error('Error fetching program details:', error);

    if (error instanceof Error && error.name === 'ZodError') {
      return NextResponse.json(
        { success: false, error: 'Invalid program ID' },
        { status: 400 }
      );
    }

    return NextResponse.json(
      { success: false, error: 'Failed to fetch program details' },
      { status: 500 }
    );
  }
}

