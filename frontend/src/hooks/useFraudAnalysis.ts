import { useCallback } from 'react';
import { fraudApi, FraudAnalysis } from '../services/api';
import { useCachedQuery } from './useCachedQuery';
import { cacheManager, CacheNamespaces } from '../utils/cacheManager';

interface UseFraudAnalysisResult {
  analysis: FraudAnalysis | null;
  loading: boolean;
  error: string | null;
  refresh: () => Promise<void>;
}

export function useFraudAnalysis(
  candidateId: string | undefined,
  minDate?: string,
  maxDate?: string,
  useAggregation: boolean = true,
  refreshToken: number = 0
): UseFraudAnalysisResult {
  const fetchAnalysis = useCallback(
    (signal: AbortSignal) =>
      useAggregation
        ? fraudApi.analyzeWithAggregation(candidateId!, minDate, maxDate, true, signal)
        : fraudApi.analyze(candidateId!, minDate, maxDate, signal),
    [candidateId, minDate, maxDate, useAggregation]
  );

  const { data, loading, error, refresh } = useCachedQuery<FraudAnalysis>({
    queryKey: `${candidateId || ''}-${minDate || 'all'}-${maxDate || 'all'}-${useAggregation}`,
    fetcher: fetchAnalysis,
    enabled: Boolean(candidateId),
  });
  const [analysis, setAnalysis] = useState<FraudAnalysis | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const cacheKeyRef = useRef<string | null>(null);

  const fetchAnalysis = async (signal?: AbortSignal, forceRefresh = false) => {
    if (!candidateId) {
      setLoading(false);
      return;
    }

    const cacheKey = `${candidateId}-${minDate || 'all'}-${maxDate || 'all'}-${useAggregation}`;
    cacheKeyRef.current = cacheKey;

    // Check cache first (unless forcing refresh)
    if (!forceRefresh) {
      const cached = cacheManager.get<FraudAnalysis>(CacheNamespaces.fraudAnalysis, cacheKey);
      if (cached) {
        setAnalysis(cached);
        setLoading(false);
        setError(null);
        return;
      }
    }

    setLoading(true);
    setError(null);
    try {
      const data = useAggregation
        ? await fraudApi.analyzeWithAggregation(candidateId, minDate, maxDate, true, signal)
        : await fraudApi.analyze(candidateId, minDate, maxDate, signal);
      
      if (!signal?.aborted && cacheKeyRef.current === cacheKey) {
        setAnalysis(data);
        
        // Update cache
        cacheManager.set(CacheNamespaces.fraudAnalysis, cacheKey, data);
      }
    } catch (err: any) {
      if (err.name === 'AbortError' || signal?.aborted) {
        return;
      }
      if (!signal?.aborted && cacheKeyRef.current === cacheKey) {
        setError(err?.response?.data?.detail || err?.message || 'Failed to load fraud analysis');
      }
    } finally {
      if (!signal?.aborted && cacheKeyRef.current === cacheKey) {
        setLoading(false);
      }
    }
  };

  useEffect(() => {
    const abortController = new AbortController();
    fetchAnalysis(abortController.signal);
    return () => {
      abortController.abort();
    };
  }, [candidateId, minDate, maxDate, useAggregation, refreshToken]);

  const refresh = async () => {
    await fetchAnalysis(undefined, true);
  };

  return {
    analysis: data,
    loading,
    error,
    refresh,
  };
}

