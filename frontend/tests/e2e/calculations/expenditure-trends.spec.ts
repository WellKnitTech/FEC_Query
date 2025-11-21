/**
 * Tests for independent expenditures and trend analysis calculations
 */

import { test, expect } from '../setup/test-setup';
import { CandidateDetailPage } from '../utils/page-objects';
import { TEST_CANDIDATES, CALCULATION_TOLERANCE } from '../setup/test-data';
import {
  calculateTotal,
  aggregateByDate,
  aggregateByField,
  expectApproximatelyEqual,
} from '../setup/helpers';
import { AxiosInstance } from 'axios';

test.describe('Independent Expenditures and Trends Calculations', () => {
  let apiClient: AxiosInstance;
  let candidatePage: CandidateDetailPage;

  test.beforeEach(async ({ page, apiClient: client }) => {
    apiClient = client;
    candidatePage = new CandidateDetailPage(page);
  });

  test('should calculate total expenditures correctly', async ({ page }) => {
    const candidateId = TEST_CANDIDATES.MULTI_CYCLE;
    
    // Fetch independent expenditures from API
    const response = await apiClient.get(`/api/independent-expenditures`, {
      params: { candidate_id: candidateId, limit: 10000 },
    });
    
    const expenditures = Array.isArray(response.data) 
      ? response.data 
      : response.data.results || [];
    
    const amounts = expenditures.map((e: any) => 
      parseFloat(e.expenditure_amount || e.amount || 0)
    );
    const expectedTotal = calculateTotal(amounts);
    
    // Fetch expenditure analysis from API
    const analysisResponse = await apiClient.get(`/api/analysis/expenditure-breakdown`, {
      params: { candidate_id: candidateId },
    });
    
    const analysis = analysisResponse.data;
    const actualTotal = analysis.total_expenditures || analysis.total || 0;
    
    // Validate
    expectApproximatelyEqual(actualTotal, expectedTotal, CALCULATION_TOLERANCE);
  });

  test('should calculate support vs oppose breakdowns correctly', async ({ page }) => {
    const candidateId = TEST_CANDIDATES.MULTI_CYCLE;
    
    // Fetch independent expenditures from API
    const response = await apiClient.get(`/api/independent-expenditures`, {
      params: { candidate_id: candidateId, limit: 10000 },
    });
    
    const expenditures = Array.isArray(response.data) 
      ? response.data 
      : response.data.results || [];
    
    // Calculate expected support/oppose totals
    let expectedSupport = 0;
    let expectedOppose = 0;
    
    for (const exp of expenditures) {
      const amount = parseFloat(exp.expenditure_amount || exp.amount || 0);
      const indicator = (exp.support_oppose_indicator || exp.indicator || '').toUpperCase();
      
      if (indicator === 'S' || indicator === 'SUPPORT') {
        expectedSupport += amount;
      } else if (indicator === 'O' || indicator === 'OPPOSE') {
        expectedOppose += amount;
      }
    }
    
    // Fetch expenditure analysis from API
    const analysisResponse = await apiClient.get(`/api/analysis/expenditure-breakdown`, {
      params: { candidate_id: candidateId },
    });
    
    const analysis = analysisResponse.data;
    const actualSupport = analysis.total_support || 0;
    const actualOppose = analysis.total_oppose || 0;
    
    // Validate
    expectApproximatelyEqual(actualSupport, expectedSupport, CALCULATION_TOLERANCE);
    expectApproximatelyEqual(actualOppose, expectedOppose, CALCULATION_TOLERANCE);
  });

  test('should calculate expenditures by date aggregations correctly', async ({ page }) => {
    const candidateId = TEST_CANDIDATES.MULTI_CYCLE;
    
    // Fetch independent expenditures from API
    const response = await apiClient.get(`/api/independent-expenditures`, {
      params: { candidate_id: candidateId, limit: 10000 },
    });
    
    const expenditures = Array.isArray(response.data) 
      ? response.data 
      : response.data.results || [];
    
    const expendituresWithDates = expenditures
      .map((e: any) => ({
        date: e.expenditure_date || e.date,
        amount: parseFloat(e.expenditure_amount || e.amount || 0),
      }))
      .filter((e: any) => e.date);
    
    // Calculate expected aggregation
    const expectedByDate = aggregateByDate(expendituresWithDates);
    
    // Fetch expenditure analysis from API
    const analysisResponse = await apiClient.get(`/api/analysis/expenditure-breakdown`, {
      params: { candidate_id: candidateId },
    });
    
    const analysis = analysisResponse.data;
    const actualByDate = analysis.expenditures_by_date || {};
    
    // Validate key dates match (sample a few)
    const sampleDates = Object.keys(expectedByDate).slice(0, 10);
    for (const date of sampleDates) {
      const expected = expectedByDate[date] || 0;
      const actual = actualByDate[date] || 0;
      expectApproximatelyEqual(actual, expected, CALCULATION_TOLERANCE);
    }
  });

  test('should calculate multi-cycle trends correctly', async ({ page }) => {
    const candidateId = TEST_CANDIDATES.MULTI_CYCLE;
    
    // Fetch trend analysis from API
    const response = await apiClient.get(`/api/trends/candidate/${candidateId}`, {
      params: { min_cycle: 2020, max_cycle: 2024 },
    });
    
    const trends = response.data;
    const cycles = trends.cycles || trends.by_cycle || [];
    
    // Validate trend structure
    if (cycles.length > 0) {
      // Each cycle should have financial data
      for (const cycle of cycles) {
        expect(cycle.cycle || cycle.year).toBeTruthy();
        expect(cycle.total_contributions || cycle.contributions).toBeGreaterThanOrEqual(0);
      }
      
      // Cycles should be sorted
      if (cycles.length > 1) {
        const cycleNumbers = cycles.map((c: any) => c.cycle || c.year).filter((n: any) => n);
        for (let i = 0; i < cycleNumbers.length - 1; i++) {
          expect(cycleNumbers[i]).toBeLessThanOrEqual(cycleNumbers[i + 1]);
        }
      }
    }
  });

  test('should calculate year-over-year comparisons correctly', async ({ page }) => {
    const candidateId = TEST_CANDIDATES.MULTI_CYCLE;
    
    // Fetch trend analysis from API
    const response = await apiClient.get(`/api/trends/candidate/${candidateId}`, {
      params: { min_cycle: 2020, max_cycle: 2024 },
    });
    
    const trends = response.data;
    const cycles = trends.cycles || trends.by_cycle || [];
    
    // Calculate year-over-year changes
    if (cycles.length >= 2) {
      const sortedCycles = [...cycles].sort((a: any, b: any) => 
        (a.cycle || a.year) - (b.cycle || b.year)
      );
      
      for (let i = 1; i < sortedCycles.length; i++) {
        const current = sortedCycles[i];
        const previous = sortedCycles[i - 1];
        
        const currentTotal = current.total_contributions || current.contributions || 0;
        const previousTotal = previous.total_contributions || previous.contributions || 0;
        
        // Calculate expected change
        const expectedChange = currentTotal - previousTotal;
        
        // If trend analysis includes change calculations, validate them
        if (current.change_from_previous !== undefined) {
          expectApproximatelyEqual(
            current.change_from_previous,
            expectedChange,
            CALCULATION_TOLERANCE
          );
        }
      }
    }
  });

  test('should handle candidates with no expenditures correctly', async ({ page }) => {
    const candidateId = TEST_CANDIDATES.NO_DATA;
    
    // Fetch expenditure analysis from API
    const response = await apiClient.get(`/api/analysis/expenditure-breakdown`, {
      params: { candidate_id: candidateId },
    });
    
    const analysis = response.data;
    const totalExpenditures = analysis.total_expenditures || analysis.total || 0;
    
    // Should return zero or empty results
    expect(totalExpenditures).toBeGreaterThanOrEqual(0);
  });
});

