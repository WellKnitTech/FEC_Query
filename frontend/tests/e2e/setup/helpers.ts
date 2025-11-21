/**
 * Calculation validation helpers for E2E tests
 */

import { CONTRIBUTION_BINS, CALCULATION_TOLERANCE, PERCENTAGE_TOLERANCE } from './test-data';

/**
 * Compare two numbers with tolerance for floating point errors
 */
export function compareNumbers(
  actual: number,
  expected: number,
  tolerance: number = CALCULATION_TOLERANCE
): boolean {
  return Math.abs(actual - expected) <= tolerance;
}

/**
 * Extract number from text (handles currency formatting, commas, etc.)
 */
export function extractNumber(text: string | null | undefined): number {
  if (!text) return 0;
  
  // Remove currency symbols, commas, and whitespace
  const cleaned = text.replace(/[$,\s]/g, '');
  const parsed = parseFloat(cleaned);
  return isNaN(parsed) ? 0 : parsed;
}

/**
 * Calculate expected total from array of amounts
 */
export function calculateTotal(amounts: number[]): number {
  return amounts.reduce((sum, amount) => sum + (amount || 0), 0);
}

/**
 * Calculate average from total and count
 */
export function calculateAverage(total: number, count: number): number {
  if (count === 0) return 0;
  return total / count;
}

/**
 * Calculate percentage
 */
export function calculatePercentage(part: number, whole: number): number {
  if (whole === 0) return 0;
  return (part / whole) * 100;
}

/**
 * Categorize contribution amount into distribution bin
 */
export function categorizeContribution(amount: number): string {
  for (const bin of CONTRIBUTION_BINS) {
    if (amount >= bin.min && amount < bin.max) {
      return bin.label;
    }
  }
  return '$2700+';
}

/**
 * Calculate contribution distribution from amounts
 */
export function calculateDistribution(amounts: number[]): Record<string, number> {
  const distribution: Record<string, number> = {};
  
  for (const bin of CONTRIBUTION_BINS) {
    distribution[bin.label] = 0;
  }
  
  for (const amount of amounts) {
    const category = categorizeContribution(amount);
    distribution[category] = (distribution[category] || 0) + 1;
  }
  
  return distribution;
}

/**
 * Validate that numbers are approximately equal
 * Returns true if approximately equal, throws error if not
 */
export function expectApproximatelyEqual(
  actual: number,
  expected: number,
  tolerance: number = CALCULATION_TOLERANCE,
  message?: string
): boolean {
  const diff = Math.abs(actual - expected);
  const errorMessage = message || 
    `Expected ${actual} to be approximately equal to ${expected} (difference: ${diff}, tolerance: ${tolerance})`;
  
  if (diff > tolerance) {
    throw new Error(errorMessage);
  }
  return true;
}

/**
 * Validate percentage is approximately equal
 */
export function expectPercentageEqual(
  actual: number,
  expected: number,
  tolerance: number = PERCENTAGE_TOLERANCE,
  message?: string
): void {
  expectApproximatelyEqual(actual, expected, tolerance, message);
}

/**
 * Aggregate amounts by date
 */
export function aggregateByDate(
  items: Array<{ date?: string; amount: number }>
): Record<string, number> {
  const aggregated: Record<string, number> = {};
  
  for (const item of items) {
    if (item.date) {
      const dateKey = item.date.split('T')[0]; // Get YYYY-MM-DD part
      aggregated[dateKey] = (aggregated[dateKey] || 0) + (item.amount || 0);
    }
  }
  
  return aggregated;
}

/**
 * Aggregate amounts by field
 */
export function aggregateByField<T>(
  items: Array<T>,
  fieldKey: keyof T,
  amountKey: keyof T
): Record<string, number> {
  const aggregated: Record<string, number> = {};
  
  for (const item of items) {
    const fieldValue = String(item[fieldKey] || '');
    const amount = Number(item[amountKey] || 0);
    
    if (fieldValue) {
      aggregated[fieldValue] = (aggregated[fieldValue] || 0) + amount;
    }
  }
  
  return aggregated;
}

/**
 * Calculate unique count
 */
export function calculateUniqueCount<T>(items: T[], fieldKey: keyof T): number {
  const uniqueValues = new Set<string>();
  
  for (const item of items) {
    const value = String(item[fieldKey] || '');
    if (value) {
      uniqueValues.add(value);
    }
  }
  
  return uniqueValues.size;
}

/**
 * Sort by amount descending
 */
export function sortByAmount<T extends { amount?: number; total?: number }>(
  items: T[]
): T[] {
  return [...items].sort((a, b) => {
    const amountA = a.amount || a.total || 0;
    const amountB = b.amount || b.total || 0;
    return amountB - amountA;
  });
}

/**
 * Calculate cumulative totals over time
 */
export function calculateCumulativeTotals(
  items: Array<{ date?: string; amount: number }>
): Array<{ date: string; cumulative: number }> {
  const sorted = [...items].sort((a, b) => {
    const dateA = a.date ? new Date(a.date).getTime() : 0;
    const dateB = b.date ? new Date(b.date).getTime() : 0;
    return dateA - dateB;
  });
  
  const cumulative: Array<{ date: string; cumulative: number }> = [];
  let runningTotal = 0;
  
  for (const item of sorted) {
    if (item.date) {
      runningTotal += item.amount || 0;
      cumulative.push({
        date: item.date.split('T')[0],
        cumulative: runningTotal,
      });
    }
  }
  
  return cumulative;
}

/**
 * Calculate velocity (contributions per day/week)
 */
export function calculateVelocity(
  items: Array<{ date?: string }>,
  period: 'day' | 'week' = 'day'
): number {
  if (items.length === 0) return 0;
  
  const dates = items
    .map(item => item.date ? new Date(item.date) : null)
    .filter((date): date is Date => date !== null)
    .sort((a, b) => a.getTime() - b.getTime());
  
  if (dates.length === 0) return 0;
  
  const firstDate = dates[0];
  const lastDate = dates[dates.length - 1];
  const daysDiff = Math.max(1, Math.ceil((lastDate.getTime() - firstDate.getTime()) / (1000 * 60 * 60 * 24)));
  
  if (period === 'week') {
    const weeks = daysDiff / 7;
    return weeks > 0 ? items.length / weeks : items.length;
  } else {
    return items.length / daysDiff;
  }
}

