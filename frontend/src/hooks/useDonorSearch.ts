import { useState, useRef, useCallback } from 'react';
import { contributionApi, UniqueContributor } from '../services/api';

interface UseDonorSearchResult {
  uniqueContributors: UniqueContributor[];
  loading: boolean;
  error: string | null;
  searchProgress: string;
  showUniqueContributors: boolean;
  search: (searchTerm: string) => Promise<void>;
  cancelSearch: () => void;
  clearResults: () => void;
}

export function useDonorSearch(): UseDonorSearchResult {
  const [uniqueContributors, setUniqueContributors] = useState<UniqueContributor[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [searchProgress, setSearchProgress] = useState<string>('');
  const [showUniqueContributors, setShowUniqueContributors] = useState(false);
  const searchAbortControllerRef = useRef<AbortController | null>(null);
  const timeoutIdRef = useRef<NodeJS.Timeout | null>(null);

  const cancelSearch = useCallback(() => {
    if (searchAbortControllerRef.current) {
      searchAbortControllerRef.current.abort();
      searchAbortControllerRef.current = null;
    }
    if (timeoutIdRef.current) {
      clearTimeout(timeoutIdRef.current);
      timeoutIdRef.current = null;
    }
    setLoading(false);
    setSearchProgress('');
  }, []);

  const clearResults = useCallback(() => {
    cancelSearch();
    setUniqueContributors([]);
    setShowUniqueContributors(false);
    setError(null);
    setSearchProgress('');
  }, [cancelSearch]);

  const search = useCallback(async (searchTerm: string) => {
    if (!searchTerm.trim()) return;

    // Cancel any existing search
    cancelSearch();

    // Create new AbortController with timeout
    const abortController = new AbortController();
    const timeoutId = setTimeout(() => {
      abortController.abort();
    }, 50000); // 50 second timeout (slightly longer than backend's 45s)

    searchAbortControllerRef.current = abortController;
    timeoutIdRef.current = timeoutId;
    setLoading(true);
    setError(null);
    setSearchProgress('Searching for donors...');

    try {
      // Get unique contributors matching the search term (fast database-only query)
      const unique = await contributionApi.getUniqueContributors(
        searchTerm.trim(),
        100,
        abortController.signal
      );

      if (abortController.signal.aborted) {
        return; // Search was cancelled
      }

      setUniqueContributors(unique);

      // Always show the list of unique contributors first
      // User will click on a donor to see their contributions
      if (unique.length > 0) {
        setShowUniqueContributors(true);
      } else {
        // No matches found in database
        setShowUniqueContributors(false);
        setError(
          `No donors found matching "${searchTerm}". Try a different search term or search contributions directly.`
        );
      }
    } catch (err: any) {
      if (abortController.signal.aborted) {
        setError(
          'Search was cancelled or timed out. Please try again with a more specific search term.'
        );
      } else if (err?.name === 'AbortError' || err?.code === 'ECONNABORTED') {
        setError(
          'Search timed out. The search may be taking too long. Please try a more specific search term.'
        );
      } else {
        setError(err?.response?.data?.detail || err?.message || 'Failed to search contributions');
      }
    } finally {
      if (timeoutIdRef.current === timeoutId) {
        clearTimeout(timeoutId);
        timeoutIdRef.current = null;
      }
      setSearchProgress('');
      setLoading(false);
      searchAbortControllerRef.current = null;
    }
  }, [cancelSearch]);

  return {
    uniqueContributors,
    loading,
    error,
    searchProgress,
    showUniqueContributors,
    search,
    cancelSearch,
    clearResults,
  };
}

