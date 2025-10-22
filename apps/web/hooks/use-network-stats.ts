import { useQuery } from '@tanstack/react-query';
import type { NetworkStats, TPSData } from '@/types';

async function fetchNetworkStats(): Promise<NetworkStats> {
  const res = await fetch('/api/network/stats');
  if (!res.ok) {
    throw new Error('Failed to fetch network stats');
  }
  const json = await res.json();
  return json.data;
}

async function fetchTPSHistory(hours: number = 24): Promise<TPSData[]> {
  const res = await fetch(`/api/network/tps?hours=${hours}`);
  if (!res.ok) {
    throw new Error('Failed to fetch TPS history');
  }
  const json = await res.json();
  return json.data;
}

export function useNetworkStats(refetchInterval: number = 30000) {
  return useQuery({
    queryKey: ['network-stats'],
    queryFn: fetchNetworkStats,
    staleTime: 30 * 1000, // 30 seconds
    refetchInterval,
  });
}

export function useTPSHistory(hours: number = 24) {
  return useQuery({
    queryKey: ['tps-history', hours],
    queryFn: () => fetchTPSHistory(hours),
    staleTime: 60 * 1000,
    refetchInterval: 60 * 1000,
  });
}

