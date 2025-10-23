import { NextRequest } from 'next/server';

interface RateLimitResult {
  success: boolean;
  limit: number;
  remaining: number;
  reset: number;
}

// Simple in-memory rate limiter
// For production, consider using Redis or Upstash
class RateLimiter {
  private requests: Map<string, { count: number; resetAt: number }>;
  private limit: number;
  private window: number; // in milliseconds

  constructor(limit: number = 60, windowInMinutes: number = 1) {
    this.requests = new Map();
    this.limit = limit;
    this.window = windowInMinutes * 60 * 1000;
    
    // Clean up expired entries every minute
    setInterval(() => this.cleanup(), 60000);
  }

  private cleanup() {
    const now = Date.now();
    for (const [key, value] of this.requests.entries()) {
      if (value.resetAt < now) {
        this.requests.delete(key);
      }
    }
  }

  check(identifier: string): RateLimitResult {
    const now = Date.now();
    const record = this.requests.get(identifier);

    if (!record || record.resetAt < now) {
      // Create new record
      this.requests.set(identifier, {
        count: 1,
        resetAt: now + this.window,
      });

      return {
        success: true,
        limit: this.limit,
        remaining: this.limit - 1,
        reset: now + this.window,
      };
    }

    if (record.count >= this.limit) {
      // Rate limit exceeded
      return {
        success: false,
        limit: this.limit,
        remaining: 0,
        reset: record.resetAt,
      };
    }

    // Increment count
    record.count++;
    this.requests.set(identifier, record);

    return {
      success: true,
      limit: this.limit,
      remaining: this.limit - record.count,
      reset: record.resetAt,
    };
  }
}

// Global rate limiter instance
const limiter = new RateLimiter(
  parseInt(process.env.RATE_LIMIT_REQUESTS_PER_MINUTE || '60'),
  1
);

export async function rateLimit(request: NextRequest): Promise<RateLimitResult> {
  // Get identifier from IP or fallback to a header
  const forwarded = request.headers.get('x-forwarded-for');
  const ip = forwarded ? forwarded.split(',')[0] : 'anonymous';
  
  return limiter.check(ip);
}

export function rateLimitHeaders(result: RateLimitResult): Record<string, string> {
  return {
    'X-RateLimit-Limit': result.limit.toString(),
    'X-RateLimit-Remaining': result.remaining.toString(),
    'X-RateLimit-Reset': new Date(result.reset).toISOString(),
  };
}

