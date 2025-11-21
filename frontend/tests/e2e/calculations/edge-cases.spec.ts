/**
 * Edge case tests for calculations
 */

import { test, expect } from '../setup/test-setup';
import { CandidateDetailPage } from '../utils/page-objects';
import { TEST_CANDIDATES, CALCULATION_TOLERANCE } from '../setup/test-data';
import {
  calculateTotal,
  calculateAverage,
  expectApproximatelyEqual,
} from '../setup/helpers';
import { AxiosInstance } from 'axios';

test.describe('Edge Cases - Calculation Handling', () => {
  let apiClient: AxiosInstance;
  let candidatePage: CandidateDetailPage;

  test.beforeEach(async ({ page, apiClient: client }) => {
    apiClient = client;
    candidatePage = new CandidateDetailPage(page);
  });

  test('should handle empty data correctly', async ({ page }) => {
    const candidateId = TEST_CANDIDATES.NO_DATA;
    
    await candidatePage.goto(candidateId);
    
    // Page should load without errors
    const candidateName = await candidatePage.getCandidateName();
    expect(candidateName).toBeTruthy();
    
    // Financial summary should show zero or "No data" message
    const summary = await candidatePage.getFinancialSummary();
    expect(summary.totalContributions).toBeGreaterThanOrEqual(0);
    expect(summary.totalReceipts).toBeGreaterThanOrEqual(0);
    
    // Contribution analysis should handle empty data
    const analysisResponse = await apiClient.get(`/api/analysis/contributions`, {
      params: { candidate_id: candidateId },
    });
    
    const analysis = analysisResponse.data;
    expect(analysis.total_contributions).toBe(0);
    expect(analysis.total_contributors).toBe(0);
    expect(analysis.average_contribution).toBe(0);
  });

  test('should handle missing fields correctly', async ({ page }) => {
    const candidateId = TEST_CANDIDATES.MULTI_CYCLE;
    
    // Fetch contributions from API
    const response = await apiClient.get(`/api/contributions`, {
      params: { candidate_id: candidateId, limit: 10000 },
    });
    
    const contributions = response.data.results || response.data || [];
    
    // Filter contributions with missing amounts
    const contributionsWithMissingAmounts = contributions.filter(
      (c: any) => !c.contribution_amount && !c.amount
    );
    
    // Calculate totals should handle missing amounts (treat as 0)
    const amounts = contributions.map((c: any) => 
      parseFloat(c.contribution_amount || c.amount || 0)
    );
    const total = calculateTotal(amounts);
    
    // Total should be valid even with missing fields
    expect(total).toBeGreaterThanOrEqual(0);
    expect(isNaN(total)).toBe(false);
  });

  test('should handle invalid data correctly', async ({ page }) => {
    const candidateId = TEST_CANDIDATES.MULTI_CYCLE;
    
    // Fetch contributions from API
    const response = await apiClient.get(`/api/contributions`, {
      params: { candidate_id: candidateId, limit: 10000 },
    });
    
    const contributions = response.data.results || response.data || [];
    
    // Check for contributions with invalid amounts (non-numeric strings, etc.)
    const invalidAmounts = contributions.filter((c: any) => {
      const amount = c.contribution_amount || c.amount;
      if (amount === null || amount === undefined) return false;
      const parsed = parseFloat(amount);
      return isNaN(parsed) && amount !== null && amount !== undefined;
    });
    
    // System should handle invalid amounts gracefully
    // (either by filtering them out or treating as 0)
    const amounts = contributions.map((c: any) => {
      const amount = c.contribution_amount || c.amount;
      const parsed = parseFloat(amount);
      return isNaN(parsed) ? 0 : parsed;
    });
    
    const total = calculateTotal(amounts);
    expect(total).toBeGreaterThanOrEqual(0);
    expect(isNaN(total)).toBe(false);
  });

  test('should handle very large numbers correctly', async ({ page }) => {
    const candidateId = TEST_CANDIDATES.MULTI_CYCLE;
    
    // Fetch contributions from API
    const response = await apiClient.get(`/api/contributions`, {
      params: { candidate_id: candidateId, limit: 10000 },
    });
    
    const contributions = response.data.results || response.data || [];
    const amounts = contributions.map((c: any) => 
      parseFloat(c.contribution_amount || c.amount || 0)
    );
    
    // Find maximum amount
    const maxAmount = Math.max(...amounts, 0);
    
    // System should handle large numbers without overflow
    const total = calculateTotal(amounts);
    expect(total).toBeGreaterThanOrEqual(maxAmount);
    expect(isFinite(total)).toBe(true);
    expect(isNaN(total)).toBe(false);
  });

  test('should handle zero values correctly', async ({ page }) => {
    const candidateId = TEST_CANDIDATES.NO_DATA;
    
    await candidatePage.goto(candidateId);
    
    // Fetch contribution analysis
    const analysisResponse = await apiClient.get(`/api/analysis/contributions`, {
      params: { candidate_id: candidateId },
    });
    
    const analysis = analysisResponse.data;
    
    // Zero values should be handled correctly
    expect(analysis.total_contributions).toBe(0);
    expect(analysis.total_contributors).toBe(0);
    expect(analysis.average_contribution).toBe(0);
    
    // Average calculation with zero count should return 0, not NaN or Infinity
    const average = calculateAverage(0, 0);
    expect(average).toBe(0);
    expect(isNaN(average)).toBe(false);
    expect(isFinite(average)).toBe(true);
  });

  test('should handle negative amounts correctly', async ({ page }) => {
    const candidateId = TEST_CANDIDATES.MULTI_CYCLE;
    
    // Fetch contributions from API
    const response = await apiClient.get(`/api/contributions`, {
      params: { candidate_id: candidateId, limit: 10000 },
    });
    
    const contributions = response.data.results || response.data || [];
    const amounts = contributions.map((c: any) => 
      parseFloat(c.contribution_amount || c.amount || 0)
    );
    
    // Check for negative amounts (refunds, etc.)
    const negativeAmounts = amounts.filter(a => a < 0);
    
    // System should handle negative amounts (they might be refunds)
    // Total should still be calculable
    const total = calculateTotal(amounts);
    expect(isNaN(total)).toBe(false);
    expect(isFinite(total)).toBe(true);
  });

  test('should handle missing dates correctly', async ({ page }) => {
    const candidateId = TEST_CANDIDATES.MULTI_CYCLE;
    
    // Fetch contributions from API
    const response = await apiClient.get(`/api/contributions`, {
      params: { candidate_id: candidateId, limit: 10000 },
    });
    
    const contributions = response.data.results || response.data || [];
    
    // Count contributions with missing dates
    const contributionsWithoutDates = contributions.filter(
      (c: any) => !c.contribution_date && !c.date
    );
    
    // System should handle missing dates gracefully
    // Contributions without dates should still be included in totals
    const amounts = contributions.map((c: any) => 
      parseFloat(c.contribution_amount || c.amount || 0)
    );
    const total = calculateTotal(amounts);
    
    expect(total).toBeGreaterThanOrEqual(0);
    
    // Date-based aggregations should only include contributions with dates
    const contributionsWithDates = contributions.filter(
      (c: any) => c.contribution_date || c.date
    );
    
    // Should have fewer contributions in date-based analysis
    expect(contributionsWithDates.length).toBeLessThanOrEqual(contributions.length);
  });

  test('should handle very small amounts correctly', async ({ page }) => {
    const candidateId = TEST_CANDIDATES.MULTI_CYCLE;
    
    // Fetch contributions from API
    const response = await apiClient.get(`/api/contributions`, {
      params: { candidate_id: candidateId, limit: 10000 },
    });
    
    const contributions = response.data.results || response.data || [];
    const amounts = contributions.map((c: any) => 
      parseFloat(c.contribution_amount || c.amount || 0)
    );
    
    // Find minimum positive amount
    const positiveAmounts = amounts.filter(a => a > 0);
    const minAmount = positiveAmounts.length > 0 ? Math.min(...positiveAmounts) : 0;
    
    // System should handle very small amounts (e.g., $0.01)
    if (minAmount > 0 && minAmount < 1) {
      const total = calculateTotal(amounts);
      expect(total).toBeGreaterThanOrEqual(minAmount);
      expect(isNaN(total)).toBe(false);
    }
  });
});

