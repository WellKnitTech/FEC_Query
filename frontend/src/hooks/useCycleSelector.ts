import { useState, useEffect, useMemo } from 'react';
import { FinancialSummary } from '../services/api';

interface UseCycleSelectorOptions {
  financials: FinancialSummary[];
  initialCycle?: number | undefined;
  onCycleChange?: (cycle: number | undefined) => void;
}

interface UseCycleSelectorResult {
  selectedCycle: number | undefined;
  availableCycles: number[];
  setCycle: (cycle: number | undefined) => void;
  hasMultipleCycles: boolean;
}

export function useCycleSelector({
  financials,
  initialCycle,
  onCycleChange,
}: UseCycleSelectorOptions): UseCycleSelectorResult {
  // Extract available cycles from financials, sorted descending
  const availableCycles = useMemo(() => {
    return financials
      .map(f => f.cycle)
      .filter((c): c is number => c !== undefined)
      .sort((a, b) => b - a);
  }, [financials]);

  // Determine initial cycle: use provided initialCycle, or latest available, or undefined
  const [selectedCycle, setSelectedCycleState] = useState<number | undefined>(() => {
    if (initialCycle !== undefined) {
      return initialCycle;
    }
    return availableCycles.length > 0 ? availableCycles[0] : undefined;
  });

  // Update selected cycle when available cycles change (e.g., after data loads)
  useEffect(() => {
    if (availableCycles.length > 0 && selectedCycle === undefined) {
      const newCycle = initialCycle !== undefined && availableCycles.includes(initialCycle)
        ? initialCycle
        : availableCycles[0];
      setSelectedCycleState(newCycle);
      if (onCycleChange) {
        onCycleChange(newCycle);
      }
    }
  }, [availableCycles, initialCycle, selectedCycle, onCycleChange]);

  const setCycle = (cycle: number | undefined) => {
    setSelectedCycleState(cycle);
    if (onCycleChange) {
      onCycleChange(cycle);
    }
  };

  const hasMultipleCycles = availableCycles.length > 1;

  return {
    selectedCycle,
    availableCycles,
    setCycle,
    hasMultipleCycles,
  };
}

