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

export type LoadingState = 'idle' | 'loading' | 'success' | 'error';

export interface ErrorState {
  message: string;
  code?: string;
  details?: unknown;
}

