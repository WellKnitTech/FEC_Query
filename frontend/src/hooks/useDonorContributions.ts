import { useState, useRef, useCallback } from 'react';
import { contributionApi, Contribution, AggregatedDonor } from '../services/api';

interface UseDonorContributionsResult {
  contributions: Contribution[];
  aggregatedDonors: AggregatedDonor[];
  selectedContributor: string | null;
  loading: boolean;
  error: string | null;
  searchProgress: string;
  viewAggregated: boolean;
  loadContributions: (
    contributorName: string,
    filters: {
      minDate?: string;
      maxDate?: string;
      minAmount?: number;
      maxAmount?: number;
    }
  ) => Promise<void>;
  toggleView: (
    contributorName: string,
    filters: {
      minDate?: string;
      maxDate?: string;
      minAmount?: number;
      maxAmount?: number;
    }
  ) => Promise<void>;
  setViewAggregated: (value: boolean) => void;
  clearContributions: () => void;
  cancelLoad: () => void;
}

export function useDonorContributions(): UseDonorContributionsResult {
  const [contributions, setContributions] = useState<Contribution[]>([]);
  const [aggregatedDonors, setAggregatedDonors] = useState<AggregatedDonor[]>([]);
  const [selectedContributor, setSelectedContributor] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [searchProgress, setSearchProgress] = useState<string>('');
  const [viewAggregated, setViewAggregated] = useState<boolean>(false);
  const loadAbortControllerRef = useRef<AbortController | null>(null);
  const timeoutIdRef = useRef<NodeJS.Timeout | null>(null);

  const cancelLoad = useCallback(() => {
    if (loadAbortControllerRef.current) {
      loadAbortControllerRef.current.abort();
      loadAbortControllerRef.current = null;
    }
    if (timeoutIdRef.current) {
      clearTimeout(timeoutIdRef.current);
      timeoutIdRef.current = null;
    }
    setLoading(false);
    setSearchProgress('');
  }, []);

  const clearContributions = useCallback(() => {
    cancelLoad();
    setContributions([]);
    setAggregatedDonors([]);
    setSelectedContributor(null);
    setError(null);
    setSearchProgress('');
  }, [cancelLoad]);

  const loadContributions = useCallback(
    async (
      contributorName: string,
      filters: {
        minDate?: string;
        maxDate?: string;
        minAmount?: number;
        maxAmount?: number;
      }
    ) => {
      // Cancel any existing load
      cancelLoad();

      // Create new AbortController with timeout
      const abortController = new AbortController();
      const timeoutId = setTimeout(() => {
        abortController.abort();
      }, 30000); // 30 second timeout

      loadAbortControllerRef.current = abortController;
      timeoutIdRef.current = timeoutId;
      setSelectedContributor(contributorName);
      setLoading(true);
      setError(null);
      setSearchProgress('Loading contributions...');

      try {
        if (viewAggregated) {
          const aggregated = await contributionApi.getAggregatedDonors(
            {
              contributor_name: contributorName,
              min_date: filters.minDate,
              max_date: filters.maxDate,
              min_amount: filters.minAmount,
              max_amount: filters.maxAmount,
              limit: 1000,
            },
            abortController.signal
          );
          if (!abortController.signal.aborted) {
            setAggregatedDonors(aggregated);
          }
        } else {
          const data = await contributionApi.get(
            {
              contributor_name: contributorName,
              min_date: filters.minDate,
              max_date: filters.maxDate,
              min_amount: filters.minAmount,
              max_amount: filters.maxAmount,
              limit: 1000,
            },
            abortController.signal
          );
          if (!abortController.signal.aborted) {
            setContributions(data);
          }
        }
      } catch (err: any) {
        if (abortController.signal.aborted) {
          setError('Request was cancelled or timed out.');
        } else if (err?.name === 'AbortError' || err?.code === 'ECONNABORTED') {
          setError('Request timed out. Please try again.');
        } else {
          setError(err?.response?.data?.detail || err?.message || 'Failed to load contributions');
        }
      } finally {
        if (timeoutIdRef.current === timeoutId) {
          clearTimeout(timeoutId);
          timeoutIdRef.current = null;
        }
        setSearchProgress('');
        setLoading(false);
        loadAbortControllerRef.current = null;
      }
    },
    [viewAggregated, cancelLoad]
  );

  const toggleView = useCallback(
    async (
      contributorName: string,
      filters: {
        minDate?: string;
        maxDate?: string;
        minAmount?: number;
        maxAmount?: number;
      }
    ) => {
      const newView = !viewAggregated;
      setViewAggregated(newView);
      await loadContributions(contributorName, filters);
    },
    [viewAggregated, loadContributions]
  );

  return {
    contributions,
    aggregatedDonors,
    selectedContributor,
    loading,
    error,
    searchProgress,
    viewAggregated,
    loadContributions,
    toggleView,
    setViewAggregated,
    clearContributions,
    cancelLoad,
  };
}

