import { useState, useCallback } from 'react';
import { contributionApi, UniqueContributor, Contribution, AggregatedDonor } from '../services/api';

interface UseDonorAnalysisResult {
  // Search state
  donors: UniqueContributor[];
  selectedDonor: string | null;
  
  // Contributions state
  contributions: Contribution[];
  aggregatedDonors: AggregatedDonor[];
  viewAggregated: boolean;
  
  // UI state
  loading: boolean;
  error: string | null;
  
  // Actions
  search: (searchTerm: string) => Promise<void>;
  selectDonor: (donorName: string) => Promise<void>;
  loadContributions: (filters?: {
    minDate?: string;
    maxDate?: string;
    minAmount?: number;
    maxAmount?: number;
  }) => Promise<void>;
  toggleView: () => Promise<void>;
  clear: () => void;
}

export function useDonorAnalysis(): UseDonorAnalysisResult {
  const [donors, setDonors] = useState<UniqueContributor[]>([]);
  const [selectedDonor, setSelectedDonor] = useState<string | null>(null);
  const [contributions, setContributions] = useState<Contribution[]>([]);
  const [aggregatedDonors, setAggregatedDonors] = useState<AggregatedDonor[]>([]);
  const [viewAggregated, setViewAggregated] = useState<boolean>(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const search = useCallback(async (searchTerm: string) => {
    if (!searchTerm.trim()) return;

    setLoading(true);
    setError(null);
    setDonors([]);
    setSelectedDonor(null);
    setContributions([]);
    setAggregatedDonors([]);

    try {
      const results = await contributionApi.getUniqueContributors(searchTerm.trim(), 100);
      setDonors(results);
      
      if (results.length === 0) {
        setError(`No donors found matching "${searchTerm}". Try a different search term.`);
      }
    } catch (err: any) {
      const errorMessage = err?.response?.data?.detail || err?.message || 'Failed to search donors';
      setError(errorMessage);
      setDonors([]);
    } finally {
      setLoading(false);
    }
  }, []);

  const selectDonor = useCallback(async (donorName: string) => {
    setSelectedDonor(donorName);
    setContributions([]);
    setAggregatedDonors([]);
    setError(null);
    // Note: Contributions will be loaded separately via loadContributions
  }, []);

  const loadContributions = useCallback(async (filters?: {
    minDate?: string;
    maxDate?: string;
    minAmount?: number;
    maxAmount?: number;
  }) => {
    if (!selectedDonor) return;

    setLoading(true);
    setError(null);

    try {
      if (viewAggregated) {
        const aggregated = await contributionApi.getAggregatedDonors({
          contributor_name: selectedDonor,
          min_date: filters?.minDate,
          max_date: filters?.maxDate,
          min_amount: filters?.minAmount,
          max_amount: filters?.maxAmount,
          limit: 1000,
        });
        setAggregatedDonors(aggregated);
      } else {
        const data = await contributionApi.get({
          contributor_name: selectedDonor,
          min_date: filters?.minDate,
          max_date: filters?.maxDate,
          min_amount: filters?.minAmount,
          max_amount: filters?.maxAmount,
          limit: 1000,
        });
        setContributions(data);
      }
    } catch (err: any) {
      const errorMessage = err?.response?.data?.detail || err?.message || 'Failed to load contributions';
      setError(errorMessage);
    } finally {
      setLoading(false);
    }
  }, [selectedDonor, viewAggregated]);

  const toggleView = useCallback(async () => {
    setViewAggregated((prev) => !prev);
    // Reload contributions with new view
    await loadContributions();
  }, [loadContributions]);

  const clear = useCallback(() => {
    setDonors([]);
    setSelectedDonor(null);
    setContributions([]);
    setAggregatedDonors([]);
    setViewAggregated(false);
    setError(null);
    setLoading(false);
  }, []);

  return {
    donors,
    selectedDonor,
    contributions,
    aggregatedDonors,
    viewAggregated,
    loading,
    error,
    search,
    selectDonor,
    loadContributions,
    toggleView,
    clear,
  };
}

