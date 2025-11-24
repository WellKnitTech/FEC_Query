import { useState, useEffect } from 'react';
import { candidateApi, Candidate } from '../services/api';

interface UseCandidateDataResult {
  candidate: Candidate | null;
  loading: boolean;
  error: string | null;
  refresh: () => Promise<void>;
}

// Shared cache for candidate data to enable instant header/contact rendering
const candidateCache = new Map<string, Candidate>();

export function useCandidateData(candidateId: string | undefined): UseCandidateDataResult {
  const [candidate, setCandidate] = useState<Candidate | null>(() => {
    if (candidateId) {
      return candidateCache.get(candidateId) ?? null;
    }
    return null;
  });
  const [loading, setLoading] = useState(!candidate);
  const [error, setError] = useState<string | null>(null);

  const fetchCandidate = async (signal?: AbortSignal) => {
    if (!candidateId) {
      setLoading(false);
      return;
    }

    setLoading(true);
    setError(null);
    try {
      const data = await candidateApi.getById(candidateId, signal);
      if (!signal?.aborted) {
        candidateCache.set(candidateId, data);
        setCandidate(data);
      }
    } catch (err: any) {
      if (err.name === 'AbortError' || signal?.aborted) {
        return;
      }
      if (!signal?.aborted) {
        setError(err?.response?.data?.detail || err?.message || 'Failed to load candidate data');
      }
    } finally {
      if (!signal?.aborted) {
        setLoading(false);
      }
    }
  };

  useEffect(() => {
    const abortController = new AbortController();
    fetchCandidate(abortController.signal);
    return () => {
      abortController.abort();
    };
  }, [candidateId]);

  const refresh = async () => {
    await fetchCandidate();
  };

  return { candidate, loading, error, refresh };
}

