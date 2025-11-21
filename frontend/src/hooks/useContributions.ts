import { useCallback } from 'react';
import { contributionApi, Contribution } from '../services/api';
import { useCachedQuery } from './useCachedQuery';

interface UseContributionsResult {
  contributions: Contribution[];
  loading: boolean;
  error: string | null;
  refresh: () => Promise<void>;
}

export function useContributions(
  candidateId?: string,
  committeeId?: string,
  minDate?: string,
  maxDate?: string,
  cycle?: number,
  limit: number = 10000
): UseContributionsResult {
  const fetchContributions = useCallback(
    (signal: AbortSignal) => contributionApi.get({
      candidate_id: candidateId,
      committee_id: committeeId,
      min_date: minDate,
      max_date: maxDate,
      limit: limit,
    }, signal),
    [candidateId, committeeId, minDate, maxDate, limit]
  );

  const { data, loading, error, refresh } = useCachedQuery<Contribution[]>({
    queryKey: `${candidateId || ''}-${committeeId || ''}-${minDate || 'all'}-${maxDate || 'all'}-${cycle || 'all'}-${limit}`,
    fetcher: fetchContributions,
    enabled: Boolean(candidateId || committeeId),
  });

  return {
    contributions: data ?? [],
    loading,
    error,
    refresh,
  };
}

