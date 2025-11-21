import { useCallback } from 'react';
import { fraudApi, FraudAnalysis } from '../services/api';
import { useCachedQuery } from './useCachedQuery';

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
  useAggregation: boolean = true
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

  return {
    analysis: data,
    loading,
    error,
    refresh,
  };
}

