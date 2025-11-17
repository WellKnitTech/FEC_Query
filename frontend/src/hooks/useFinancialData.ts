import { useState, useEffect, useMemo } from 'react';
import { candidateApi, FinancialSummary } from '../services/api';

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
  const [financials, setFinancials] = useState<FinancialSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const fetchFinancials = async (signal?: AbortSignal) => {
    if (!candidateId) {
      setLoading(false);
      return;
    }

    setLoading(true);
    setError(null);
    try {
      const data = await candidateApi.getFinancials(candidateId, cycle, signal);
      if (!signal?.aborted) {
        setFinancials(data);
      }
    } catch (err: any) {
      if (err.name === 'AbortError' || signal?.aborted) {
        return;
      }
      if (!signal?.aborted) {
        setError(err?.response?.data?.detail || err?.message || 'Failed to load financial data');
      }
    } finally {
      if (!signal?.aborted) {
        setLoading(false);
      }
    }
  };

  useEffect(() => {
    const abortController = new AbortController();
    fetchFinancials(abortController.signal);
    return () => {
      abortController.abort();
    };
  }, [candidateId, cycle]);

  const refresh = async () => {
    await fetchFinancials();
  };

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

