/**
 * Tests for contribution analysis calculations
 */

import { test, expect } from '../setup/test-setup';
import { CandidateDetailPage } from '../utils/page-objects';
import { TEST_CANDIDATES, getCycleDateRange, CALCULATION_TOLERANCE } from '../setup/test-data';
import {
  calculateTotal,
  calculateAverage,
  calculateDistribution,
  aggregateByDate,
  aggregateByField,
  calculateUniqueCount,
  sortByAmount,
  expectApproximatelyEqual,
} from '../setup/helpers';
import { AxiosInstance } from 'axios';

test.describe('Contribution Analysis Calculations', () => {
  let apiClient: AxiosInstance;
  let candidatePage: CandidateDetailPage;

  test.beforeEach(async ({ page, apiClient: client }) => {
    apiClient = client;
    candidatePage = new CandidateDetailPage(page);
  });

  test('should calculate contribution totals match sum of individual contributions', async ({ page }) => {
    const candidateId = TEST_CANDIDATES.MULTI_CYCLE;
    
    await candidatePage.goto(candidateId);
    
    // Get total from contribution analysis component
    const totalElement = page.locator('text=/total.*contribution/i').first();
    const uiTotalText = await totalElement.textContent();
    const uiTotal = parseFloat((uiTotalText || '').replace(/[^0-9.]/g, '')) || 0;
    
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

  test('should calculate contribution distribution bins correctly', async ({ page }) => {
    const candidateId = TEST_CANDIDATES.MULTI_CYCLE;
    
    await candidatePage.goto(candidateId);
    
    // Fetch contributions from API
    const response = await apiClient.get(`/api/contributions`, {
      params: { candidate_id: candidateId, limit: 10000 },
    });
    
    const contributions = response.data.results || response.data || [];
    const amounts = contributions.map((c: any) => 
      parseFloat(c.contribution_amount || c.amount || 0)
    );
    
    // Calculate expected distribution
    const expectedDistribution = calculateDistribution(amounts);
    
    // Fetch contribution analysis from API
    const analysisResponse = await apiClient.get(`/api/analysis/contributions`, {
      params: { candidate_id: candidateId },
    });
    
    const analysis = analysisResponse.data;
    const actualDistribution = analysis.contribution_distribution || {};
    
    // Validate each bin
    for (const [bin, expectedCount] of Object.entries(expectedDistribution)) {
      const actualCount = actualDistribution[bin] || 0;
      expect(actualCount).toBe(expectedCount);
    }
  });

  test('should sort top donors correctly by total amount', async ({ page }) => {
    const candidateId = TEST_CANDIDATES.MULTI_CYCLE;
    
    await candidatePage.goto(candidateId);
    
    // Fetch contribution analysis from API
    const response = await apiClient.get(`/api/analysis/contributions`, {
      params: { candidate_id: candidateId },
    });
    
    const analysis = response.data;
    const topDonors = analysis.top_donors || [];
    
    // Validate sorting (should be descending by total)
    for (let i = 0; i < topDonors.length - 1; i++) {
      expect(topDonors[i].total).toBeGreaterThanOrEqual(topDonors[i + 1].total);
    }
    
    // Validate that top donors match aggregated contributions
    const contributionsResponse = await apiClient.get(`/api/contributions`, {
      params: { candidate_id: candidateId, limit: 10000 },
    });
    
    const contributions = contributionsResponse.data.results || contributionsResponse.data || [];
    
    // Aggregate by donor name
    const donorTotals: Record<string, number> = {};
    for (const contrib of contributions) {
      const name = contrib.contributor_name || contrib.name || 'Unknown';
      const amount = parseFloat(contrib.contribution_amount || contrib.amount || 0);
      donorTotals[name] = (donorTotals[name] || 0) + amount;
    }
    
    // Check that top donor matches
    if (topDonors.length > 0) {
      const topDonorName = topDonors[0].name;
      const expectedTotal = donorTotals[topDonorName] || 0;
      expectApproximatelyEqual(topDonors[0].total, expectedTotal, CALCULATION_TOLERANCE);
    }
  });

  test('should calculate contributions by date aggregations correctly', async ({ page }) => {
    const candidateId = TEST_CANDIDATES.MULTI_CYCLE;
    
    await candidatePage.goto(candidateId);
    
    // Fetch contributions from API
    const response = await apiClient.get(`/api/contributions`, {
      params: { candidate_id: candidateId, limit: 10000 },
    });
    
    const contributions = response.data.results || response.data || [];
    const contributionsWithDates = contributions
      .map((c: any) => ({
        date: c.contribution_date || c.date,
        amount: parseFloat(c.contribution_amount || c.amount || 0),
      }))
      .filter((c: any) => c.date);
    
    // Calculate expected aggregation
    const expectedByDate = aggregateByDate(contributionsWithDates);
    
    // Fetch contribution analysis from API
    const analysisResponse = await apiClient.get(`/api/analysis/contributions`, {
      params: { candidate_id: candidateId },
    });
    
    const analysis = analysisResponse.data;
    const actualByDate = analysis.contributions_by_date || {};
    
    // Validate key dates match (sample a few)
    const sampleDates = Object.keys(expectedByDate).slice(0, 10);
    for (const date of sampleDates) {
      const expected = expectedByDate[date] || 0;
      const actual = actualByDate[date] || 0;
      expectApproximatelyEqual(actual, expected, CALCULATION_TOLERANCE);
    }
  });

  test('should calculate contributions by state aggregations correctly', async ({ page }) => {
    const candidateId = TEST_CANDIDATES.MULTI_CYCLE;
    
    await candidatePage.goto(candidateId);
    
    // Fetch contributions from API
    const response = await apiClient.get(`/api/contributions`, {
      params: { candidate_id: candidateId, limit: 10000 },
    });
    
    const contributions = response.data.results || response.data || [];
    const contributionsWithStates = contributions
      .map((c: any) => ({
        state: c.contributor_state || c.state,
        amount: parseFloat(c.contribution_amount || c.amount || 0),
      }))
      .filter((c: any) => c.state);
    
    // Calculate expected aggregation
    const expectedByState = aggregateByField(contributionsWithStates, 'state', 'amount');
    
    // Fetch contribution analysis from API
    const analysisResponse = await apiClient.get(`/api/analysis/contributions`, {
      params: { candidate_id: candidateId },
    });
    
    const analysis = analysisResponse.data;
    const actualByState = analysis.contributions_by_state || {};
    
    // Validate key states match (sample a few)
    const sampleStates = Object.keys(expectedByState).slice(0, 10);
    for (const state of sampleStates) {
      const expected = expectedByState[state] || 0;
      const actual = actualByState[state] || 0;
      expectApproximatelyEqual(actual, expected, CALCULATION_TOLERANCE);
    }
  });

  test('should calculate data completeness percentage correctly', async ({ page }) => {
    const candidateId = TEST_CANDIDATES.MULTI_CYCLE;
    
    await candidatePage.goto(candidateId);
    
    // Fetch contribution analysis from API
    const analysisResponse = await apiClient.get(`/api/analysis/contributions`, {
      params: { candidate_id: candidateId },
    });
    
    const analysis = analysisResponse.data;
    
    if (analysis.total_from_api && analysis.total_from_api > 0) {
      const expectedCompleteness = (analysis.total_contributions / analysis.total_from_api) * 100;
      const actualCompleteness = analysis.data_completeness || 0;
      
      // Completeness should be between 0 and 100
      expect(actualCompleteness).toBeGreaterThanOrEqual(0);
      expect(actualCompleteness).toBeLessThanOrEqual(100);
      
      // Should match calculated value
      expectApproximatelyEqual(actualCompleteness, expectedCompleteness, 0.1);
    }
  });

  test('should calculate total contributors (unique count) correctly', async ({ page }) => {
    const candidateId = TEST_CANDIDATES.MULTI_CYCLE;
    
    await candidatePage.goto(candidateId);
    
    // Fetch contributions from API
    const response = await apiClient.get(`/api/contributions`, {
      params: { candidate_id: candidateId, limit: 10000 },
    });
    
    const contributions = response.data.results || response.data || [];
    const expectedUniqueCount = calculateUniqueCount(contributions, 'contributor_name');
    
    // Fetch contribution analysis from API
    const analysisResponse = await apiClient.get(`/api/analysis/contributions`, {
      params: { candidate_id: candidateId },
    });
    
    const analysis = analysisResponse.data;
    const actualUniqueCount = analysis.total_contributors || 0;
    
    // Validate
    expect(actualUniqueCount).toBe(expectedUniqueCount);
  });
});

