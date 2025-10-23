export interface TransactionData {
  signature: string;
  slot: number;
  block_time?: number;
  success: boolean;
  error?: string | null;
  fee: number;
  compute_units_consumed?: number;
  programs_invoked: string[];
}

export interface TransactionSearchParams {
  signature?: string;
  program_id?: string;
  start_slot?: number;
  end_slot?: number;
  limit?: number;
}

export interface TransactionSearchResult {
  transactions: TransactionData[];
  total: number;
  has_more: boolean;
}

