import { createClient, ClickHouseClient } from '@clickhouse/client';

let clickhouseClient: ClickHouseClient | null = null;

export function getClickHouseClient(): ClickHouseClient {
  if (!clickhouseClient) {
    const host = process.env.CLICKHOUSE_HOST;
    
    if (!host) {
      throw new Error('CLICKHOUSE_HOST environment variable is required');
    }

    clickhouseClient = createClient({
      host,
      username: process.env.CLICKHOUSE_USER || 'default',
      password: process.env.CLICKHOUSE_PASSWORD || '',
      database: process.env.CLICKHOUSE_DATABASE || 'default',
      clickhouse_settings: {
        async_insert: 1,
        wait_for_async_insert: 0,
      },
      request_timeout: 30000, // 30 seconds
    });
  }

  return clickhouseClient;
}

export const clickhouse = getClickHouseClient();

export async function testConnection(): Promise<boolean> {
  try {
    const result = await clickhouse.query({
      query: 'SELECT 1 as result',
      format: 'JSONEachRow',
    });
    
    const data = await result.json();
    return data.length > 0;
  } catch (error) {
    console.error('ClickHouse connection test failed:', error);
    return false;
  }
}

export async function healthCheck(): Promise<{
  healthy: boolean;
  latency?: number;
  error?: string;
}> {
  const startTime = Date.now();
  
  try {
    const result = await clickhouse.query({
      query: 'SELECT 1 as result',
      format: 'JSONEachRow',
    });
    
    await result.json();
    const latency = Date.now() - startTime;
    
    return { healthy: true, latency };
  } catch (error) {
    return {
      healthy: false,
      error: error instanceof Error ? error.message : 'Unknown error',
    };
  }
}

