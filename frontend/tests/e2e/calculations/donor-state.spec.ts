/**
 * Tests for donor state analysis calculations
 */

import { test, expect } from '../setup/test-setup';
import { CandidateDetailPage } from '../utils/page-objects';
import { TEST_CANDIDATES, CALCULATION_TOLERANCE, PERCENTAGE_TOLERANCE } from '../setup/test-data';
import {
  aggregateByField,
  calculatePercentage,
  calculateTotal,
  expectApproximatelyEqual,
  expectPercentageEqual,
} from '../setup/helpers';
import { AxiosInstance } from 'axios';

test.describe('Donor State Analysis Calculations', () => {
  let apiClient: AxiosInstance;
  let candidatePage: CandidateDetailPage;

  test.beforeEach(async ({ page, apiClient: client }) => {
    apiClient = client;
    candidatePage = new CandidateDetailPage(page);
  });

  test('should calculate state percentages correctly', async ({ page }) => {
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
    const totalAmount = calculateTotal(contributionsWithStates.map(c => c.amount));
    
    // Fetch donor state analysis from API
    const analysisResponse = await apiClient.get(`/api/analysis/donor-states`, {
      params: { candidate_id: candidateId },
    });
    
    const analysis = analysisResponse.data;
    const statePercentages = analysis.state_percentages || analysis.by_state || {};
    
    // Validate percentages for key states
    const sampleStates = Object.keys(expectedByState).slice(0, 5);
    for (const state of sampleStates) {
      const expectedAmount = expectedByState[state] || 0;
      const expectedPercentage = calculatePercentage(expectedAmount, totalAmount);
      
      const actualPercentage = statePercentages[state] || 0;
      
      // Percentages should match
      expectPercentageEqual(actualPercentage, expectedPercentage, PERCENTAGE_TOLERANCE);
    }
    
    // Total percentages should sum to approximately 100%
    const totalPercentage = Object.values(statePercentages).reduce(
      (sum: number, p: any) => sum + (typeof p === 'number' ? p : p.percentage || 0),
      0
    );
    expectPercentageEqual(totalPercentage, 100, PERCENTAGE_TOLERANCE);
  });

  test('should identify out-of-state contributions correctly', async ({ page }) => {
    const candidateId = TEST_CANDIDATES.MULTI_CYCLE;
    
    // First, get candidate info to determine candidate state
    const candidateResponse = await apiClient.get(`/api/candidates/${candidateId}`);
    const candidate = candidateResponse.data;
    const candidateState = candidate.state;
    
    if (!candidateState) {
      test.skip();
      return;
    }
    
    // Fetch out-of-state contributions from API
    const response = await apiClient.get(`/api/analysis/donor-states/out-of-state-contributions`, {
      params: { candidate_id: candidateId, limit: 10000 },
    });
    
    const outOfStateContributions = Array.isArray(response.data) ? response.data : [];
    
    // Validate that all contributions are from different states
    for (const contrib of outOfStateContributions.slice(0, 10)) {
      const contributorState = contrib.contributor_state || contrib.state;
      expect(contributorState).toBeTruthy();
      expect(contributorState).not.toBe(candidateState);
    }
  });

  test('should calculate geographic aggregations correctly', async ({ page }) => {
    const candidateId = TEST_CANDIDATES.MULTI_CYCLE;
    
    // Fetch contributions from API
    const contributionsResponse = await apiClient.get(`/api/contributions`, {
      params: { candidate_id: candidateId, limit: 10000 },
    });
    
    const contributions = contributionsResponse.data.results || contributionsResponse.data || [];
    const contributionsWithStates = contributions
      .map((c: any) => ({
        state: c.contributor_state || c.state,
        amount: parseFloat(c.contribution_amount || c.amount || 0),
      }))
      .filter((c: any) => c.state);
    
    // Calculate expected aggregation
    const expectedByState = aggregateByField(contributionsWithStates, 'state', 'amount');
    
    // Fetch donor state analysis from API
    const analysisResponse = await apiClient.get(`/api/analysis/donor-states`, {
      params: { candidate_id: candidateId },
    });
    
    const analysis = analysisResponse.data;
    const byState = analysis.by_state || analysis.state_totals || {};
    
    // Validate key states match
    const sampleStates = Object.keys(expectedByState).slice(0, 10);
    for (const state of sampleStates) {
      const expected = expectedByState[state] || 0;
      const actual = typeof byState[state] === 'number' 
        ? byState[state] 
        : byState[state]?.total || byState[state]?.amount || 0;
      
      expectApproximatelyEqual(actual, expected, CALCULATION_TOLERANCE);
    }
  });

  test('should handle candidates with no state data correctly', async ({ page }) => {
    const candidateId = TEST_CANDIDATES.NO_DATA;
    
    // Fetch donor state analysis from API
    const response = await apiClient.get(`/api/analysis/donor-states`, {
      params: { candidate_id: candidateId },
    });
    
    const analysis = response.data;
    const statePercentages = analysis.state_percentages || analysis.by_state || {};
    
    // Should return empty or zero values
    const totalPercentage = Object.values(statePercentages).reduce(
      (sum: number, p: any) => sum + (typeof p === 'number' ? p : p.percentage || 0),
      0
    );
    
    expect(totalPercentage).toBeGreaterThanOrEqual(0);
    expect(totalPercentage).toBeLessThanOrEqual(100);
  });
});

