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

export interface NetworkHealth {
  status: 'healthy' | 'degraded' | 'down';
  latency: number;
  last_check: string;
}

export interface SlotInfo {
  slot: number;
  timestamp: string;
  transaction_count: number;
  success_count: number;
  error_count: number;
}

