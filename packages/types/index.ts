// Shared TypeScript types for the Solana Analytics Platform

export interface ProgramStats {
  program_id: string;
  total_transactions: number;
  total_errors: number;
  success_rate: number;
  avg_compute_units: number;
  min_compute_units?: number;
  max_compute_units?: number;
  first_seen?: string;
  last_seen?: string;
}

export interface ProgramDetail extends ProgramStats {
  hourly_stats?: HourlyStats[];
  recent_transactions?: TransactionSummary[];
}

export interface HourlyStats {
  hour: string;
  transaction_count: number;
  error_count: number;
  avg_compute_units: number;
}

export interface NetworkStats {
  total_slots: number;
  total_transactions: number;
  avg_tps: number;
  max_tps: number;
  timestamp?: string;
}

export interface TPSData {
  timestamp: string;
  tps: number;
  slot: number;
}

export interface TransactionData {
  signature: string;
  slot: number;
  block_time?: number;
  success: boolean;
  error?: string;
  fee: number;
  compute_units_consumed?: number;
  programs_invoked: string[];
}

export interface TransactionSummary {
  signature: string;
  slot: number;
  success: boolean;
  timestamp: string;
}

export interface TransactionSearchParams {
  signature?: string;
  program_id?: string;
  start_slot?: number;
  end_slot?: number;
  limit?: number;
}

export interface APIResponse<T> {
  success: boolean;
  data?: T;
  error?: string;
  timestamp?: string;
}

export interface PaginatedResponse<T> {
  success: boolean;
  data: T[];
  pagination: {
    total: number;
    page: number;
    page_size: number;
    has_next: boolean;
  };
}

export type TimeRange = '1h' | '24h' | '7d' | '30d';

export interface ChartDataPoint {
  timestamp: string;
  value: number;
  label?: string;
}

