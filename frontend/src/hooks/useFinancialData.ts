import { useState, useEffect, useMemo, useRef } from 'react';
import { candidateApi, FinancialSummary } from '../services/api';
import { cacheManager, CacheNamespaces } from '../utils/cacheManager';

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
  const cacheKeyRef = useRef<string | null>(null);

  const fetchFinancials = async (signal?: AbortSignal, forceRefresh = false) => {
    if (!candidateId) {
      setLoading(false);
      return;
    }

    const cacheKey = `${candidateId}-${cycle ?? 'all'}`;
    cacheKeyRef.current = cacheKey;

    // Check cache first (unless forcing refresh)
    if (!forceRefresh) {
      const cachedData = cacheManager.get<FinancialSummary[]>(CacheNamespaces.financials, cacheKey);
      if (cachedData && cachedData.length > 0) {
        setFinancials(cachedData);
        setLoading(false);
        setError(null);
        return;
      }
    }

    setLoading(true);
    setError(null);
    try {
      const data = await candidateApi.getFinancials(candidateId, cycle, signal);
      if (!signal?.aborted && cacheKeyRef.current === cacheKey) {
        setFinancials(data);

        // Update cache
        cacheManager.set(CacheNamespaces.financials, cacheKey, data);

        // Also cache individual cycles if we fetched all
        if (cycle === undefined && data.length > 0) {
          data.forEach((financial) => {
            if (financial.cycle !== undefined) {
              cacheManager.set(CacheNamespaces.financials, `${candidateId}-${financial.cycle}`, [financial]);
            }
          });
        }
      }
    } catch (err: any) {
      if (err.name === 'AbortError' || signal?.aborted) {
        return;
      }
      if (!signal?.aborted && cacheKeyRef.current === cacheKey) {
        setError(err?.response?.data?.detail || err?.message || 'Failed to load financial data');
      }
    } finally {
      if (!signal?.aborted && cacheKeyRef.current === cacheKey) {
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
    await fetchFinancials(undefined, true);
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

