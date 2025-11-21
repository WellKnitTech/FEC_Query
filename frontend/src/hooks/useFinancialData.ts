import { useCallback, useMemo } from 'react';
import { candidateApi, FinancialSummary } from '../services/api';
import { useCachedQuery } from './useCachedQuery';

interface UseFinancialDataResult {
  financials: FinancialSummary[];
  latest: FinancialSummary | null;
  selected: FinancialSummary | null;
  loading: boolean;
  error: string | null;
  availableCycles: number[];
  refresh: () => Promise<void>;
}

export function useFinancialData(
  candidateId: string | undefined,
  cycle?: number | undefined
): UseFinancialDataResult {
  const fetchFinancials = useCallback(
    (signal: AbortSignal) => candidateApi.getFinancials(candidateId!, cycle, signal),
    [candidateId, cycle]
  );

  const { data, loading, error, refresh } = useCachedQuery<FinancialSummary[]>({
    queryKey: candidateId ? `financials:${candidateId}:${cycle ?? 'all'}` : 'financials:none',
    fetcher: fetchFinancials,
    enabled: Boolean(candidateId),
  });

  const financials = data ?? [];

  // Sort financials by cycle descending (newest first)
  const sortedFinancials = useMemo(() => {
    return [...financials].sort((a, b) => {
      const cycleA = a.cycle ?? 0;
      const cycleB = b.cycle ?? 0;
      return cycleB - cycleA;
    });
  }, [financials]);

  // Get latest financial (first in sorted array)
  const latest = useMemo(() => {
    return sortedFinancials.length > 0 ? sortedFinancials[0] : null;
  }, [sortedFinancials]);

  // Get selected financial (by cycle, or latest if no cycle specified)
  const selected = useMemo(() => {
    if (cycle === undefined) {
      return latest;
    }
    return sortedFinancials.find(f => f.cycle === cycle) || null;
  }, [sortedFinancials, cycle, latest]);

  // Extract available cycles
  const availableCycles = useMemo(() => {
    return sortedFinancials
      .map(f => f.cycle)
      .filter((c): c is number => c !== undefined)
      .sort((a, b) => b - a);
  }, [sortedFinancials]);

  return {
    financials: sortedFinancials,
    latest,
    selected,
    loading,
    error,
    availableCycles,
    refresh,
  };
}

