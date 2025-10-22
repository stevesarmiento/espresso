import { useQuery } from '@tanstack/react-query';
import type { ProgramStats, ProgramDetail, TimeRange } from '@/types';

async function fetchPrograms(timeRange: TimeRange): Promise<ProgramStats[]> {
  const res = await fetch(`/api/programs?range=${timeRange}`);
  if (!res.ok) {
    throw new Error('Failed to fetch programs');
  }
  const json = await res.json();
  return json.data;
}

async function fetchProgramById(
  programId: string,
  timeRange: TimeRange
): Promise<ProgramDetail> {
  const res = await fetch(`/api/programs/${programId}?range=${timeRange}`);
  if (!res.ok) {
    throw new Error('Failed to fetch program details');
  }
  const json = await res.json();
  return json.data;
}

export function usePrograms(timeRange: TimeRange = '24h') {
  return useQuery({
    queryKey: ['programs', timeRange],
    queryFn: () => fetchPrograms(timeRange),
    staleTime: 60 * 1000, // 1 minute
    refetchInterval: 60 * 1000, // Refresh every minute
  });
}

export function useProgramDetail(programId: string, timeRange: TimeRange = '7d') {
  return useQuery({
    queryKey: ['program', programId, timeRange],
    queryFn: () => fetchProgramById(programId, timeRange),
    staleTime: 60 * 1000,
    refetchInterval: 60 * 1000,
    enabled: !!programId,
  });
}

