import { FinancialSummary } from '../services/api';

/**
 * Format a dollar amount to a human-readable string (e.g., "$1.5K", "$2.3M")
 */
export function formatCurrency(amount: number): string {
  if (amount >= 1_000_000) {
    return `$${(amount / 1_000_000).toFixed(2)}M`;
  } else if (amount >= 1_000) {
    return `$${(amount / 1_000).toFixed(1)}K`;
  } else {
    return `$${amount.toFixed(2)}`;
  }
}

/**
 * Format a dollar amount with thousands separator
 */
export function formatCurrencyFull(amount: number): string {
  return `$${amount.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

/**
 * Calculate percentage of total
 */
export function calculatePercentage(part: number, total: number): number {
  if (total === 0) return 0;
  return (part / total) * 100;
}

/**
 * Get the latest financial summary from an array (sorted by cycle descending)
 */
export function getLatestFinancial(financials: FinancialSummary[]): FinancialSummary | null {
  if (financials.length === 0) return null;
  
  const sorted = [...financials].sort((a, b) => {
    const cycleA = a.cycle ?? 0;
    const cycleB = b.cycle ?? 0;
    return cycleB - cycleA;
  });
  
  return sorted[0];
}

/**
 * Get financial summary for a specific cycle
 */
export function getFinancialByCycle(
  financials: FinancialSummary[],
  cycle: number
): FinancialSummary | null {
  return financials.find(f => f.cycle === cycle) || null;
}

/**
 * Calculate total from an array of numbers
 */
export function sum(values: number[]): number {
  return values.reduce((acc, val) => acc + val, 0);
}

/**
 * Calculate average from an array of numbers
 */
export function average(values: number[]): number {
  if (values.length === 0) return 0;
  return sum(values) / values.length;
}

