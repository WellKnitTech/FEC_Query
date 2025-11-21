import { useState, useCallback } from 'react';

export interface Filters {
  minDate: string;
  maxDate: string;
  minAmount: number | undefined;
  maxAmount: number | undefined;
}

interface UseFiltersResult {
  filters: Filters;
  setFilters: (filters: Partial<Filters>) => void;
  handleDatePreset: (preset: 'last30' | 'lastYear' | 'thisCycle' | 'allTime') => void;
  handleAmountPreset: (preset: '0-100' | '100-500' | '500+' | 'all') => void;
  clearFilters: () => void;
}

export function useFilters(): UseFiltersResult {
  const [filters, setFiltersState] = useState<Filters>({
    minDate: '',
    maxDate: '',
    minAmount: undefined,
    maxAmount: undefined,
  });

  const setFilters = useCallback((newFilters: Partial<Filters>) => {
    setFiltersState((prev) => ({ ...prev, ...newFilters }));
  }, []);

  const handleDatePreset = useCallback((preset: 'last30' | 'lastYear' | 'thisCycle' | 'allTime') => {
    const today = new Date();
    const currentYear = today.getFullYear();
    const currentCycle = currentYear % 2 === 0 ? currentYear : currentYear - 1;

    switch (preset) {
      case 'last30':
        const thirtyDaysAgo = new Date(today);
        thirtyDaysAgo.setDate(today.getDate() - 30);
        setFilters({
          minDate: thirtyDaysAgo.toISOString().split('T')[0],
          maxDate: today.toISOString().split('T')[0],
        });
        break;
      case 'lastYear':
        const oneYearAgo = new Date(today);
        oneYearAgo.setFullYear(today.getFullYear() - 1);
        setFilters({
          minDate: oneYearAgo.toISOString().split('T')[0],
          maxDate: today.toISOString().split('T')[0],
        });
        break;
      case 'thisCycle':
        setFilters({
          minDate: `${currentCycle}-01-01`,
          maxDate: today.toISOString().split('T')[0],
        });
        break;
      case 'allTime':
        setFilters({
          minDate: '',
          maxDate: '',
        });
        break;
    }
  }, [setFilters]);

  const handleAmountPreset = useCallback((preset: '0-100' | '100-500' | '500+' | 'all') => {
    switch (preset) {
      case '0-100':
        setFilters({
          minAmount: 0,
          maxAmount: 100,
        });
        break;
      case '100-500':
        setFilters({
          minAmount: 100,
          maxAmount: 500,
        });
        break;
      case '500+':
        setFilters({
          minAmount: 500,
          maxAmount: undefined,
        });
        break;
      case 'all':
        setFilters({
          minAmount: undefined,
          maxAmount: undefined,
        });
        break;
    }
  }, [setFilters]);

  const clearFilters = useCallback(() => {
    setFiltersState({
      minDate: '',
      maxDate: '',
      minAmount: undefined,
      maxAmount: undefined,
    });
  }, []);

  return {
    filters,
    setFilters,
    handleDatePreset,
    handleAmountPreset,
    clearFilters,
  };
}

