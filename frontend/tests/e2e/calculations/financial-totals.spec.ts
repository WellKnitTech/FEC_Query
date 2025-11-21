/**
 * Tests for financial totals calculations
 */

import { test, expect } from '../setup/test-setup';
import { CandidateDetailPage } from '../utils/page-objects';
import { TEST_CANDIDATES, getCycleDateRange, CALCULATION_TOLERANCE } from '../setup/test-data';
import {
  calculateTotal,
  calculateAverage,
  expectApproximatelyEqual,
  extractNumber,
} from '../setup/helpers';
import { AxiosInstance } from 'axios';

test.describe('Financial Totals Calculations', () => {
  let apiClient: AxiosInstance;
  let candidatePage: CandidateDetailPage;

  test.beforeEach(async ({ page, apiClient: client }) => {
    apiClient = client;
    candidatePage = new CandidateDetailPage(page);
  });

  test('should calculate total contributions correctly', async ({ page }) => {
    const candidateId = TEST_CANDIDATES.MULTI_CYCLE;
    
    // Navigate to candidate page
    await candidatePage.goto(candidateId);
    
    // Get total contributions from UI
    const uiTotal = await candidatePage.getTotalContributions();
    
    // Fetch contributions from API
    const response = await apiClient.get(`/api/contributions`, {
      params: { candidate_id: candidateId, limit: 10000 },
    });
    
    const contributions = response.data.results || response.data || [];
    const amounts = contributions.map((c: any) => 
      parseFloat(c.contribution_amount || c.amount || 0)
    );
    const expectedTotal = calculateTotal(amounts);
    
    // Validate
    expectApproximatelyEqual(uiTotal, expectedTotal, CALCULATION_TOLERANCE);
  });

  test('should calculate average contribution correctly', async ({ page }) => {
    const candidateId = TEST_CANDIDATES.MULTI_CYCLE;
    
    await candidatePage.goto(candidateId);
    
    // Get average from UI
    const uiAverage = await candidatePage.getAverageContribution();
    
    // Fetch contributions from API
    const response = await apiClient.get(`/api/contributions`, {
      params: { candidate_id: candidateId, limit: 10000 },
    });
    
    const contributions = response.data.results || response.data || [];
    const amounts = contributions.map((c: any) => 
      parseFloat(c.contribution_amount || c.amount || 0)
    );
    const total = calculateTotal(amounts);
    const expectedAverage = calculateAverage(total, contributions.length);
    
    // Validate
    expectApproximatelyEqual(uiAverage, expectedAverage, CALCULATION_TOLERANCE);
  });

  test('should display financial summary totals correctly', async ({ page }) => {
    const candidateId = TEST_CANDIDATES.MULTI_CYCLE;
    
    await candidatePage.goto(candidateId);
    
    // Get financial summary from UI
    const summary = await candidatePage.getFinancialSummary();
    
    // Fetch financial data from API
    const response = await apiClient.get(`/api/candidates/${candidateId}/financials`);
    const financials = Array.isArray(response.data) ? response.data : [response.data];
    const latestFinancial = financials[0];
    
    if (latestFinancial) {
      // Validate totals match
      expectApproximatelyEqual(
        summary.totalReceipts,
        latestFinancial.total_receipts || 0,
        CALCULATION_TOLERANCE
      );
      
      expectApproximatelyEqual(
        summary.totalDisbursements,
        latestFinancial.total_disbursements || 0,
        CALCULATION_TOLERANCE
      );
      
      expectApproximatelyEqual(
        summary.cashOnHand,
        latestFinancial.cash_on_hand || 0,
        CALCULATION_TOLERANCE
      );
      
      expectApproximatelyEqual(
        summary.totalContributions,
        latestFinancial.total_contributions || 0,
        CALCULATION_TOLERANCE
      );
    }
  });

  test('should filter financial totals by cycle correctly', async ({ page }) => {
    const candidateId = TEST_CANDIDATES.MULTI_CYCLE;
    const cycle = 2024;
    
    await candidatePage.goto(candidateId);
    
    // Select cycle
    await candidatePage.selectCycle(cycle);
    
    // Get financial summary after cycle selection
    const summary = await candidatePage.getFinancialSummary();
    
    // Fetch financial data for specific cycle
    const response = await apiClient.get(`/api/candidates/${candidateId}/financials`, {
      params: { cycle },
    });
    const financials = Array.isArray(response.data) ? response.data : [response.data];
    const cycleFinancial = financials.find((f: any) => f.cycle === cycle) || financials[0];
    
    if (cycleFinancial) {
      expectApproximatelyEqual(
        summary.totalContributions,
        cycleFinancial.total_contributions || 0,
        CALCULATION_TOLERANCE
      );
    }
  });

  test('should calculate contribution type breakdowns correctly', async ({ page }) => {
    const candidateId = TEST_CANDIDATES.MULTI_CYCLE;
    
    await candidatePage.goto(candidateId);
    
    // Get financial summary
    const summary = await candidatePage.getFinancialSummary();
    
    // Fetch financial data
    const response = await apiClient.get(`/api/candidates/${candidateId}/financials`);
    const financials = Array.isArray(response.data) ? response.data : [response.data];
    const latestFinancial = financials[0];
    
    if (latestFinancial) {
      // Validate contribution types sum to total
      const individual = latestFinancial.individual_contributions || 0;
      const pac = latestFinancial.pac_contributions || 0;
      const party = latestFinancial.party_contributions || 0;
      const loan = latestFinancial.loan_contributions || 0;
      
      const sumOfTypes = individual + pac + party + loan;
      
      // Total contributions should be approximately equal to sum of types
      // (allowing for other contribution types)
      expect(sumOfTypes).toBeLessThanOrEqual(
        (latestFinancial.total_contributions || 0) + CALCULATION_TOLERANCE
      );
    }
  });

  test('should handle zero financial totals correctly', async ({ page }) => {
    const candidateId = TEST_CANDIDATES.NO_DATA;
    
    await candidatePage.goto(candidateId);
    
    // Check that page loads without errors
    const candidateName = await candidatePage.getCandidateName();
    expect(candidateName).toBeTruthy();
    
    // Financial summary should show zero or "No data" message
    const summary = await candidatePage.getFinancialSummary();
    
    // Values should be zero or very small
    expect(summary.totalContributions).toBeGreaterThanOrEqual(0);
    expect(summary.totalReceipts).toBeGreaterThanOrEqual(0);
  });
});

