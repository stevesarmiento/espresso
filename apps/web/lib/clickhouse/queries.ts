import { clickhouse } from './client';
import type { TimeInterval } from './types';
import type {
  ProgramStats,
  ProgramDetail,
  NetworkStats,
  TPSData,
  TransactionData,
  HourlyStats,
} from '@repo/types';

export async function getTopPrograms(
  timeRange: '1h' | '24h' | '7d' = '24h',
  limit: number = 50
): Promise<ProgramStats[]> {
  const intervalMap: Record<string, TimeInterval> = {
    '1h': '1 HOUR',
    '24h': '24 HOUR',
    '7d': '7 DAY',
  };
  
  const interval = intervalMap[timeRange];

  try {
    const result = await clickhouse.query({
      query: `
        SELECT 
          program_id,
          sum(count) as total_transactions,
          sum(error_count) as total_errors,
          round(100 * (1 - sum(error_count) / sum(count)), 2) as success_rate,
          round(avg(total_cus / count), 2) as avg_compute_units,
          min(timestamp) as first_seen,
          max(timestamp) as last_seen
        FROM program_invocations
        WHERE timestamp > now() - INTERVAL {interval:String}
        GROUP BY program_id
        ORDER BY total_transactions DESC
        LIMIT {limit:UInt32}
      `,
      query_params: {
        interval,
        limit,
      },
      format: 'JSONEachRow',
    });

    return await result.json<ProgramStats>();
  } catch (error) {
    console.error('Error fetching top programs:', error);
    throw new Error('Failed to fetch top programs');
  }
}

export async function getProgramById(
  programId: string,
  timeRange: '24h' | '7d' | '30d' = '7d'
): Promise<ProgramDetail | null> {
  const intervalMap: Record<string, TimeInterval> = {
    '24h': '24 HOUR',
    '7d': '7 DAY',
    '30d': '30 DAY',
  };
  
  const interval = intervalMap[timeRange];

  try {
    const result = await clickhouse.query({
      query: `
        SELECT 
          program_id,
          sum(count) as total_transactions,
          sum(error_count) as total_errors,
          round(100 * (1 - sum(error_count) / sum(count)), 2) as success_rate,
          round(avg(total_cus / count), 2) as avg_compute_units,
          min(min_cus) as min_compute_units,
          max(max_cus) as max_compute_units,
          min(timestamp) as first_seen,
          max(timestamp) as last_seen
        FROM program_invocations
        WHERE program_id = {programId:String}
          AND timestamp > now() - INTERVAL {interval:String}
        GROUP BY program_id
      `,
      query_params: {
        programId,
        interval,
      },
      format: 'JSONEachRow',
    });

    const data = await result.json<ProgramDetail>();
    return data[0] || null;
  } catch (error) {
    console.error('Error fetching program by ID:', error);
    throw new Error('Failed to fetch program details');
  }
}

export async function getProgramHourlyStats(
  programId: string,
  hours: number = 24
): Promise<HourlyStats[]> {
  try {
    const result = await clickhouse.query({
      query: `
        SELECT 
          toStartOfHour(timestamp) as hour,
          sum(count) as transaction_count,
          sum(error_count) as error_count,
          round(avg(total_cus / count), 2) as avg_compute_units
        FROM program_invocations
        WHERE program_id = {programId:String}
          AND timestamp > now() - INTERVAL {hours:UInt32} HOUR
        GROUP BY hour
        ORDER BY hour DESC
      `,
      query_params: {
        programId,
        hours,
      },
      format: 'JSONEachRow',
    });

    return await result.json<HourlyStats>();
  } catch (error) {
    console.error('Error fetching program hourly stats:', error);
    throw new Error('Failed to fetch program hourly stats');
  }
}

export async function getNetworkStats(): Promise<NetworkStats> {
  try {
    const result = await clickhouse.query({
      query: `
        SELECT 
          count(DISTINCT slot) as total_slots,
          sum(transaction_count) as total_transactions,
          round(avg(transaction_count), 2) as avg_tps,
          max(transaction_count) as max_tps,
          max(indexed_at) as timestamp
        FROM jetstreamer_slot_status
        WHERE indexed_at > now() - INTERVAL 1 HOUR
      `,
      format: 'JSONEachRow',
    });

    const data = await result.json<NetworkStats>();
    return data[0] || {
      total_slots: 0,
      total_transactions: 0,
      avg_tps: 0,
      max_tps: 0,
    };
  } catch (error) {
    console.error('Error fetching network stats:', error);
    throw new Error('Failed to fetch network stats');
  }
}

export async function getTPSHistory(
  hours: number = 24
): Promise<TPSData[]> {
  try {
    const result = await clickhouse.query({
      query: `
        SELECT 
          indexed_at as timestamp,
          transaction_count as tps,
          slot
        FROM jetstreamer_slot_status
        WHERE indexed_at > now() - INTERVAL {hours:UInt32} HOUR
        ORDER BY indexed_at DESC
        LIMIT 1000
      `,
      query_params: {
        hours,
      },
      format: 'JSONEachRow',
    });

    return await result.json<TPSData>();
  } catch (error) {
    console.error('Error fetching TPS history:', error);
    throw new Error('Failed to fetch TPS history');
  }
}

export async function getTransactionBySignature(
  signature: string
): Promise<TransactionData | null> {
  try {
    const result = await clickhouse.query({
      query: `
        SELECT 
          signature,
          slot,
          block_time,
          success,
          error,
          fee,
          compute_units_consumed,
          programs_invoked
        FROM transactions
        WHERE signature = {signature:String}
        LIMIT 1
      `,
      query_params: {
        signature,
      },
      format: 'JSONEachRow',
    });

    const data = await result.json<TransactionData>();
    return data[0] || null;
  } catch (error) {
    console.error('Error fetching transaction:', error);
    throw new Error('Failed to fetch transaction');
  }
}

export async function searchTransactions(params: {
  programId?: string;
  startSlot?: number;
  endSlot?: number;
  limit?: number;
}): Promise<TransactionData[]> {
  const { programId, startSlot, endSlot, limit = 20 } = params;

  let whereConditions: string[] = [];
  const queryParams: Record<string, any> = { limit };

  if (programId) {
    whereConditions.push('has(programs_invoked, {programId:String})');
    queryParams.programId = programId;
  }

  if (startSlot) {
    whereConditions.push('slot >= {startSlot:UInt64}');
    queryParams.startSlot = startSlot;
  }

  if (endSlot) {
    whereConditions.push('slot <= {endSlot:UInt64}');
    queryParams.endSlot = endSlot;
  }

  const whereClause = whereConditions.length > 0 
    ? `WHERE ${whereConditions.join(' AND ')}`
    : '';

  try {
    const result = await clickhouse.query({
      query: `
        SELECT 
          signature,
          slot,
          block_time,
          success,
          error,
          fee,
          compute_units_consumed,
          programs_invoked
        FROM transactions
        ${whereClause}
        ORDER BY slot DESC
        LIMIT {limit:UInt32}
      `,
      query_params: queryParams,
      format: 'JSONEachRow',
    });

    return await result.json<TransactionData>();
  } catch (error) {
    console.error('Error searching transactions:', error);
    throw new Error('Failed to search transactions');
  }
}

