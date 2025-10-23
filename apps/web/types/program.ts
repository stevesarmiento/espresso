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

export interface TransactionSummary {
  signature: string;
  slot: number;
  success: boolean;
  timestamp: string;
}

export interface ProgramListResponse {
  programs: ProgramStats[];
  count: number;
  timeRange: string;
}

