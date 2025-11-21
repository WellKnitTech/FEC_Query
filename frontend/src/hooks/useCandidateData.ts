import { useCallback } from 'react';
import { candidateApi, Candidate } from '../services/api';
import { useCachedQuery } from './useCachedQuery';

interface UseCandidateDataResult {
  candidate: Candidate | null;
  loading: boolean;
  error: string | null;
  refresh: () => Promise<void>;
}

export function useCandidateData(candidateId: string | undefined): UseCandidateDataResult {
  const fetchCandidate = useCallback((signal: AbortSignal) => candidateApi.getById(candidateId!, signal), [candidateId]);

  const { data, loading, error, refresh } = useCachedQuery<Candidate>({
    queryKey: candidateId ? `candidate:${candidateId}` : 'candidate:none',
    fetcher: fetchCandidate,
    enabled: Boolean(candidateId),
  });

  return { candidate: data, loading, error, refresh };
}

