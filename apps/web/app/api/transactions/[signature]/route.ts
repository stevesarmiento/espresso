import { NextRequest, NextResponse } from 'next/server';
import { getTransactionBySignature } from '@/lib/clickhouse/queries';
import { rateLimit, rateLimitHeaders } from '@/lib/utils/rate-limit';
import { signatureSchema } from '@/lib/utils/validation';

export const dynamic = 'force-dynamic';
export const runtime = 'nodejs';

export async function GET(
  request: NextRequest,
  { params }: { params: { signature: string } }
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
    // Validate signature
    const signature = signatureSchema.parse(params.signature);

    const transaction = await getTransactionBySignature(signature);

    if (!transaction) {
      return NextResponse.json(
        { success: false, error: 'Transaction not found' },
        { status: 404 }
      );
    }

    return NextResponse.json(
      {
        success: true,
        data: transaction,
      },
      { headers: rateLimitHeaders(limitResult) }
    );
  } catch (error) {
    console.error('Error fetching transaction:', error);

    if (error instanceof Error && error.name === 'ZodError') {
      return NextResponse.json(
        { success: false, error: 'Invalid transaction signature' },
        { status: 400 }
      );
    }

    return NextResponse.json(
      { success: false, error: 'Failed to fetch transaction' },
      { status: 500 }
    );
  }
}

