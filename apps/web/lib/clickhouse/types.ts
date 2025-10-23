// ClickHouse-specific type mappings

export interface ClickHouseProgramInvocation {
  program_id: string;
  timestamp: string;
  count: number;
  error_count: number;
  total_cus: number;
  min_cus: number;
  max_cus: number;
}

export interface ClickHouseSlotStatus {
  slot: number;
  indexed_at: string;
  transaction_count: number;
  success_count: number;
  error_count: number;
}

export interface ClickHouseTransactionRecord {
  signature: string;
  slot: number;
  block_time: number;
  success: boolean;
  error: string | null;
  fee: number;
  compute_units_consumed: number;
  programs: string[];
}

export interface QueryResult<T> {
  data: T[];
  rows: number;
  statistics?: {
    elapsed: number;
    rows_read: number;
    bytes_read: number;
  };
}

export type TimeInterval = '1 HOUR' | '24 HOUR' | '7 DAY' | '30 DAY';

export interface QueryOptions {
  timeout?: number;
  maxRows?: number;
  format?: 'JSONEachRow' | 'JSON' | 'CSV';
}

