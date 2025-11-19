import { useState, useEffect, useRef } from 'react';
import { contributionApi, Contribution } from '../services/api';

interface UseContributionsResult {
  contributions: Contribution[];
  loading: boolean;
  error: string | null;
  refresh: () => Promise<void>;
}

// Cache for contribution data by candidate/committee and filters
const contributionsCache = new Map<string, Contribution[]>();

export function useContributions(
  candidateId?: string,
  committeeId?: string,
  minDate?: string,
  maxDate?: string,
  cycle?: number,
  limit: number = 10000
): UseContributionsResult {
  const [contributions, setContributions] = useState<Contribution[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const cacheKeyRef = useRef<string | null>(null);

  const fetchContributions = async (signal?: AbortSignal, forceRefresh = false) => {
    if (!candidateId && !committeeId) {
      setLoading(false);
      return;
    }

    const cacheKey = `${candidateId || ''}-${committeeId || ''}-${minDate || 'all'}-${maxDate || 'all'}-${cycle || 'all'}-${limit}`;
    cacheKeyRef.current = cacheKey;

    // Check cache first (unless forcing refresh)
    if (!forceRefresh) {
      const cached = contributionsCache.get(cacheKey);
      if (cached && cached.length > 0) {
        setContributions(cached);
        setLoading(false);
        setError(null);
        return;
      }
    }

    setLoading(true);
    setError(null);
    try {
      const data = await contributionApi.get({
        candidate_id: candidateId,
        committee_id: committeeId,
        min_date: minDate,
        max_date: maxDate,
        limit: limit,
      }, signal);
      
      if (!signal?.aborted && cacheKeyRef.current === cacheKey) {
        setContributions(data);
        
        // Update cache
        contributionsCache.set(cacheKey, data);
      }
    } catch (err: any) {
      if (err.name === 'AbortError' || signal?.aborted) {
        return;
      }
      if (!signal?.aborted && cacheKeyRef.current === cacheKey) {
        setError(err?.response?.data?.detail || err?.message || 'Failed to load contributions');
      }
    } finally {
      if (!signal?.aborted && cacheKeyRef.current === cacheKey) {
        setLoading(false);
      }
    }
  };

  useEffect(() => {
    const abortController = new AbortController();
    fetchContributions(abortController.signal);
    return () => {
      abortController.abort();
    };
  }, [candidateId, committeeId, minDate, maxDate, cycle, limit]);

  const refresh = async () => {
    await fetchContributions(undefined, true);
  };

  return {
    contributions,
    loading,
    error,
    refresh,
  };
}

