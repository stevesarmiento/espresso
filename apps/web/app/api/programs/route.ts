import { NextRequest, NextResponse } from 'next/server';
import { getTopPrograms } from '@/lib/clickhouse/queries';
import { rateLimit, rateLimitHeaders } from '@/lib/utils/rate-limit';
import { programSearchSchema } from '@/lib/utils/validation';

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
    const searchParams = request.nextUrl.searchParams;
    const limitParam = searchParams.get('limit');
    const params = programSearchSchema.parse({
      range: searchParams.get('range') || '24h',
      limit: limitParam ? parseInt(limitParam) : undefined,
    });

    const programs = await getTopPrograms(params.range, params.limit);

    return NextResponse.json(
      {
        success: true,
        data: programs,
        timeRange: params.range,
        count: programs.length,
      },
      { headers: rateLimitHeaders(limitResult) }
    );
  } catch (error) {
    console.error('Error fetching programs:', error);
    
    if (error instanceof Error && error.name === 'ZodError') {
      return NextResponse.json(
        { success: false, error: 'Invalid parameters' },
        { status: 400 }
      );
    }

    return NextResponse.json(
      { success: false, error: 'Failed to fetch programs' },
      { status: 500 }
    );
  }
}

