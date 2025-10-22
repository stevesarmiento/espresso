import { NextRequest, NextResponse } from 'next/server';
import { searchTransactions } from '@/lib/clickhouse/queries';
import { rateLimit, rateLimitHeaders } from '@/lib/utils/rate-limit';
import { transactionSearchSchema } from '@/lib/utils/validation';

export const dynamic = 'force-dynamic';
export const runtime = 'nodejs';

export async function POST(request: NextRequest) {
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
    const body = await request.json();
    const params = transactionSearchSchema.parse(body);

    const transactions = await searchTransactions({
      programId: params.program_id,
      startSlot: params.start_slot,
      endSlot: params.end_slot,
      limit: params.limit,
    });

    return NextResponse.json(
      {
        success: true,
        data: transactions,
        count: transactions.length,
        has_more: transactions.length === params.limit,
      },
      { headers: rateLimitHeaders(limitResult) }
    );
  } catch (error) {
    console.error('Error searching transactions:', error);

    if (error instanceof Error && error.name === 'ZodError') {
      return NextResponse.json(
        { success: false, error: 'Invalid search parameters' },
        { status: 400 }
      );
    }

    return NextResponse.json(
      { success: false, error: 'Failed to search transactions' },
      { status: 500 }
    );
  }
}

