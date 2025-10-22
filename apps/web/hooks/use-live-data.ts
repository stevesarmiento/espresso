import { useQuery } from '@tanstack/react-query';
import { useState, useEffect } from 'react';

interface UseLiveDataOptions<T> {
  queryKey: string[];
  queryFn: () => Promise<T>;
  intervalMs?: number;
  enabled?: boolean;
}

export function useLiveData<T>({
  queryKey,
  queryFn,
  intervalMs = 30000,
  enabled = true,
}: UseLiveDataOptions<T>) {
  return useQuery({
    queryKey,
    queryFn,
    refetchInterval: intervalMs,
    staleTime: intervalMs / 2,
    enabled,
  });
}

export function usePolling<T>(
  fetchFn: () => Promise<T>,
  intervalMs: number = 30000
) {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<Error | null>(null);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    let isMounted = true;
    let intervalId: NodeJS.Timeout;

    async function fetchData() {
      try {
        const result = await fetchFn();
        if (isMounted) {
          setData(result);
          setError(null);
          setIsLoading(false);
        }
      } catch (err) {
        if (isMounted) {
          setError(err instanceof Error ? err : new Error('Unknown error'));
          setIsLoading(false);
        }
      }
    }

    // Initial fetch
    fetchData();

    // Set up polling
    intervalId = setInterval(fetchData, intervalMs);

    return () => {
      isMounted = false;
      clearInterval(intervalId);
    };
  }, [fetchFn, intervalMs]);

  return { data, error, isLoading };
}

export function useAutoRefresh(
  refreshFn: () => void,
  intervalMs: number = 60000,
  enabled: boolean = true
) {
  useEffect(() => {
    if (!enabled) return;

    const intervalId = setInterval(refreshFn, intervalMs);

    return () => {
      clearInterval(intervalId);
    };
  }, [refreshFn, intervalMs, enabled]);
}

