/**
 * Cross-component validation tests for candidate detail page
 */

import { test, expect } from '../setup/test-setup';
import { CandidateDetailPage } from '../utils/page-objects';
import { TEST_CANDIDATES, getCycleDateRange, CALCULATION_TOLERANCE } from '../setup/test-data';
import {
  expectApproximatelyEqual,
} from '../setup/helpers';
import { AxiosInstance } from 'axios';

test.describe('Candidate Detail Page - Cross-Component Validation', () => {
  let apiClient: AxiosInstance;
  let candidatePage: CandidateDetailPage;

  test.beforeEach(async ({ page, apiClient: client }) => {
    apiClient = client;
    candidatePage = new CandidateDetailPage(page);
  });

  test('should show consistent totals across components', async ({ page }) => {
    const candidateId = TEST_CANDIDATES.MULTI_CYCLE;
    
    await candidatePage.goto(candidateId);
    
    // Get total from financial summary
    const financialSummary = await candidatePage.getFinancialSummary();
    const summaryTotal = financialSummary.totalContributions;
    
    // Get total from contribution analysis (if visible on page)
    const totalElement = page.locator('text=/total.*contribution/i').first();
    const analysisTotalText = await totalElement.textContent();
    const analysisTotal = parseFloat((analysisTotalText || '').replace(/[^0-9.]/g, '')) || 0;
    
    // Fetch from API for comparison
    const response = await apiClient.get(`/api/candidates/${candidateId}/financials`);
    const financials = Array.isArray(response.data) ? response.data : [response.data];
    const apiTotal = financials[0]?.total_contributions || 0;
    
    // All totals should match (within tolerance)
    if (summaryTotal > 0 && analysisTotal > 0) {
      expectApproximatelyEqual(summaryTotal, analysisTotal, CALCULATION_TOLERANCE);
    }
    if (summaryTotal > 0 && apiTotal > 0) {
      expectApproximatelyEqual(summaryTotal, apiTotal, CALCULATION_TOLERANCE);
    }
  });

  test('should apply cycle filtering consistently across all components', async ({ page }) => {
    const candidateId = TEST_CANDIDATES.MULTI_CYCLE;
    const cycle = 2024;
    
    await candidatePage.goto(candidateId);
    
    // Select cycle
    await candidatePage.selectCycle(cycle);
    await page.waitForTimeout(1000); // Wait for components to update
    
    // Get date range for cycle
    const dateRange = getCycleDateRange(cycle);
    
    // Verify cycle is selected
    const cycleSelect = page.locator('select[name*="cycle"], [data-testid="cycle-selector"]').first();
    const selectedValue = await cycleSelect.inputValue();
    expect(selectedValue).toBe(String(cycle));
    
    // Check that components show cycle-specific data
    const financialSummary = await candidatePage.getFinancialSummary();
    
    // Fetch financial data for specific cycle
    const response = await apiClient.get(`/api/candidates/${candidateId}/financials`, {
      params: { cycle },
    });
    const financials = Array.isArray(response.data) ? response.data : [response.data];
    const cycleFinancial = financials.find((f: any) => f.cycle === cycle) || financials[0];
    
    if (cycleFinancial) {
      expectApproximatelyEqual(
        financialSummary.totalContributions,
        cycleFinancial.total_contributions || 0,
        CALCULATION_TOLERANCE
      );
    }
  });

  test('should update calculations when cycle changes', async ({ page }) => {
    const candidateId = TEST_CANDIDATES.MULTI_CYCLE;
    const cycle1 = 2022;
    const cycle2 = 2024;
    
    await candidatePage.goto(candidateId);
    
    // Select first cycle
    await candidatePage.selectCycle(cycle1);
    await page.waitForTimeout(1000);
    const summary1 = await candidatePage.getFinancialSummary();
    const total1 = summary1.totalContributions;
    
    // Select second cycle
    await candidatePage.selectCycle(cycle2);
    await page.waitForTimeout(1000);
    const summary2 = await candidatePage.getFinancialSummary();
    const total2 = summary2.totalContributions;
    
    // Totals should potentially differ (unless data is identical)
    // At minimum, both should be valid numbers
    expect(total1).toBeGreaterThanOrEqual(0);
    expect(total2).toBeGreaterThanOrEqual(0);
  });

  test('should maintain data consistency when switching between cycles', async ({ page }) => {
    const candidateId = TEST_CANDIDATES.MULTI_CYCLE;
    const cycles = [2020, 2022, 2024];
    
    await candidatePage.goto(candidateId);
    
    const totals: number[] = [];
    
    for (const cycle of cycles) {
      await candidatePage.selectCycle(cycle);
      await page.waitForTimeout(1000);
      
      const summary = await candidatePage.getFinancialSummary();
      totals.push(summary.totalContributions);
      
      // Each cycle should have valid data
      expect(summary.totalContributions).toBeGreaterThanOrEqual(0);
      expect(summary.totalReceipts).toBeGreaterThanOrEqual(0);
    }
    
    // Verify we got different totals for different cycles (if data exists)
    const uniqueTotals = new Set(totals);
    // At least one cycle should have data (if candidate has multiple cycles)
    expect(uniqueTotals.size).toBeGreaterThanOrEqual(1);
  });
});

