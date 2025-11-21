/**
 * Test data fixtures and constants for E2E tests
 */

/**
 * Test candidate IDs for different scenarios
 */
export const TEST_CANDIDATES = {
  // Candidate with multiple cycles
  MULTI_CYCLE: 'P80001571', // Example: Use a real candidate ID from your database
  // Candidate with single cycle
  SINGLE_CYCLE: 'H8CA05035', // Example: Use a real candidate ID
  // Candidate with no financial data
  NO_DATA: 'P00000001', // Example: Use a candidate ID with no data
} as const;

/**
 * Test cycles for validation
 */
export const TEST_CYCLES = [2020, 2022, 2024, 2026] as const;

/**
 * Contribution distribution bins (standard FEC bins)
 */
export const CONTRIBUTION_BINS = [
  { label: '$0-50', min: 0, max: 50 },
  { label: '$50-100', min: 50, max: 100 },
  { label: '$100-200', min: 100, max: 200 },
  { label: '$200-500', min: 200, max: 500 },
  { label: '$500-1000', min: 500, max: 1000 },
  { label: '$1000-2700', min: 1000, max: 2700 },
  { label: '$2700+', min: 2700, max: Infinity },
] as const;

/**
 * Tolerance for floating point comparisons (in dollars)
 */
export const CALCULATION_TOLERANCE = 0.01; // 1 cent tolerance

/**
 * Tolerance for percentage comparisons
 */
export const PERCENTAGE_TOLERANCE = 0.1; // 0.1% tolerance

/**
 * Helper to get cycle date range
 */
export function getCycleDateRange(cycle: number): { minDate: string; maxDate: string } {
  const cycleYear = cycle;
  const minDate = `${cycleYear - 1}-01-01`;
  const maxDate = `${cycleYear}-12-31`;
  return { minDate, maxDate };
}

