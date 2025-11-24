import { useCallback } from 'react';
import { contributionApi, Contribution } from '../services/api';
import { useCachedQuery } from './useCachedQuery';
import { cacheManager, CacheNamespaces } from '../utils/cacheManager';

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
      const cached = cacheManager.get<Contribution[]>(CacheNamespaces.contributions, cacheKey);
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
        cacheManager.set(CacheNamespaces.contributions, cacheKey, data);
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
    contributions: data ?? [],
    loading,
    error,
    refresh,
  };
}

