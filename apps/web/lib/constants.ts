export const APP_NAME = 'Solana Analytics Platform';
export const APP_DESCRIPTION = 'Real-time Solana blockchain analytics powered by Jetstreamer';

export const TIME_RANGES = {
  '1h': { label: '1 Hour', interval: '1 HOUR' },
  '24h': { label: '24 Hours', interval: '24 HOUR' },
  '7d': { label: '7 Days', interval: '7 DAY' },
  '30d': { label: '30 Days', interval: '30 DAY' },
} as const;

export const QUERY_STALE_TIME = 60 * 1000; // 1 minute
export const QUERY_CACHE_TIME = 5 * 60 * 1000; // 5 minutes

export const MAX_PROGRAM_LIST_SIZE = 100;
export const MAX_TRANSACTION_LIST_SIZE = 50;

export const SOLANA_EXPLORER_URL = 'https://explorer.solana.com';
export const SOLANA_FM_URL = 'https://solana.fm';

export const DEFAULT_RATE_LIMIT = 60; // requests per minute

