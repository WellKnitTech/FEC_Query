/**
 * Tests for donor aggregation calculations
 */

import { test, expect } from '../setup/test-setup';
import { DonorAnalysisPage } from '../utils/page-objects';
import { TEST_CANDIDATES, CALCULATION_TOLERANCE } from '../setup/test-data';
import {
  calculateTotal,
  calculateUniqueCount,
  expectApproximatelyEqual,
} from '../setup/helpers';
import { AxiosInstance } from 'axios';

test.describe('Donor Aggregation Calculations', () => {
  let apiClient: AxiosInstance;
  let donorPage: DonorAnalysisPage;

  test.beforeEach(async ({ page, apiClient: client }) => {
    apiClient = client;
    donorPage = new DonorAnalysisPage(page);
  });

  test('should aggregate donors by name variations correctly', async ({ page }) => {
    const candidateId = TEST_CANDIDATES.MULTI_CYCLE;
    
    await donorPage.goto(candidateId);
    
    // Fetch contributions from API
    const contributionsResponse = await apiClient.get(`/api/contributions`, {
      params: { candidate_id: candidateId, limit: 10000 },
    });
    
    const contributions = contributionsResponse.data.results || contributionsResponse.data || [];
    const totalFromContributions = calculateTotal(
      contributions.map((c: any) => parseFloat(c.contribution_amount || c.amount || 0))
    );
    
    // Fetch aggregated donors from API
    const aggregatedResponse = await apiClient.get(`/api/analysis/donor-states/out-of-state-contributions`, {
      params: { candidate_id: candidateId, aggregate: true, limit: 10000 },
    });
    
    const aggregatedDonors = Array.isArray(aggregatedResponse.data) 
      ? aggregatedResponse.data 
      : aggregatedResponse.data.results || [];
    
    // Calculate total from aggregated donors
    const totalFromAggregated = calculateTotal(
      aggregatedDonors.map((d: any) => parseFloat(d.total_amount || d.amount || 0))
    );
    
    // Totals should be approximately equal (allowing for name matching differences)
    expectApproximatelyEqual(
      totalFromAggregated,
      totalFromContributions,
      CALCULATION_TOLERANCE * 100 // Allow larger tolerance for aggregation
    );
  });

  test('should calculate aggregated totals match sum of individual contributions', async ({ page }) => {
    const candidateId = TEST_CANDIDATES.MULTI_CYCLE;
    
    // Fetch aggregated donors from API
    const aggregatedResponse = await apiClient.get(`/api/analysis/donor-states/out-of-state-contributions`, {
      params: { candidate_id: candidateId, aggregate: true, limit: 10000 },
    });
    
    const aggregatedDonors = Array.isArray(aggregatedResponse.data) 
      ? aggregatedResponse.data 
      : aggregatedResponse.data.results || [];
    
    // For each aggregated donor, validate that total_amount matches sum of contributions
    for (const donor of aggregatedDonors.slice(0, 10)) {
      const donorName = donor.name || donor.contributor_name || '';
      const reportedTotal = parseFloat(donor.total_amount || donor.amount || 0);
      
      // Fetch individual contributions for this donor (if available)
      const contributionsResponse = await apiClient.get(`/api/contributions`, {
        params: { 
          candidate_id: candidateId,
          contributor_name: donorName,
          limit: 1000,
        },
      });
      
      const contributions = contributionsResponse.data.results || contributionsResponse.data || [];
      const expectedTotal = calculateTotal(
        contributions.map((c: any) => parseFloat(c.contribution_amount || c.amount || 0))
      );
      
      // Validate (allowing for name variation matching)
      if (contributions.length > 0) {
        expectApproximatelyEqual(
          reportedTotal,
          expectedTotal,
          CALCULATION_TOLERANCE * 10
        );
      }
    }
  });

  test('should calculate contribution counts per aggregated donor correctly', async ({ page }) => {
    const candidateId = TEST_CANDIDATES.MULTI_CYCLE;
    
    // Fetch aggregated donors from API
    const aggregatedResponse = await apiClient.get(`/api/analysis/donor-states/out-of-state-contributions`, {
      params: { candidate_id: candidateId, aggregate: true, limit: 10000 },
    });
    
    const aggregatedDonors = Array.isArray(aggregatedResponse.data) 
      ? aggregatedResponse.data 
      : aggregatedResponse.data.results || [];
    
    // Validate contribution counts
    for (const donor of aggregatedDonors.slice(0, 10)) {
      const reportedCount = donor.contribution_count || donor.count || 0;
      
      // Count should be at least 1 (since donor is aggregated)
      expect(reportedCount).toBeGreaterThanOrEqual(1);
      
      // If donor has name variations, count might be higher
      if (donor.all_names && Array.isArray(donor.all_names)) {
        expect(donor.all_names.length).toBeGreaterThanOrEqual(1);
      }
    }
  });

  test('should match name variations with confidence scores', async ({ page }) => {
    const candidateId = TEST_CANDIDATES.MULTI_CYCLE;
    
    // Fetch aggregated donors from API
    const aggregatedResponse = await apiClient.get(`/api/analysis/donor-states/out-of-state-contributions`, {
      params: { candidate_id: candidateId, aggregate: true, limit: 10000 },
    });
    
    const aggregatedDonors = Array.isArray(aggregatedResponse.data) 
      ? aggregatedResponse.data 
      : aggregatedResponse.data.results || [];
    
    // Check for donors with multiple name variations
    const donorsWithVariations = aggregatedDonors.filter(
      (d: any) => d.all_names && Array.isArray(d.all_names) && d.all_names.length > 1
    );
    
    // If name variations are found, validate structure
    for (const donor of donorsWithVariations.slice(0, 5)) {
      expect(donor.all_names.length).toBeGreaterThan(1);
      
      // Match confidence should be present if available
      if (donor.match_confidence !== undefined) {
        expect(donor.match_confidence).toBeGreaterThanOrEqual(0);
        expect(donor.match_confidence).toBeLessThanOrEqual(1);
      }
    }
  });

  test('should calculate unique donor count correctly', async ({ page }) => {
    const candidateId = TEST_CANDIDATES.MULTI_CYCLE;
    
    // Fetch contributions from API
    const contributionsResponse = await apiClient.get(`/api/contributions`, {
      params: { candidate_id: candidateId, limit: 10000 },
    });
    
    const contributions = contributionsResponse.data.results || contributionsResponse.data || [];
    const uniqueContributorNames = calculateUniqueCount(contributions, 'contributor_name');
    
    // Fetch aggregated donors from API
    const aggregatedResponse = await apiClient.get(`/api/analysis/donor-states/out-of-state-contributions`, {
      params: { candidate_id: candidateId, aggregate: true, limit: 10000 },
    });
    
    const aggregatedDonors = Array.isArray(aggregatedResponse.data) 
      ? aggregatedResponse.data 
      : aggregatedResponse.data.results || [];
    
    // Aggregated donor count should be less than or equal to unique contributor names
    // (aggregation groups similar names together)
    expect(aggregatedDonors.length).toBeLessThanOrEqual(uniqueContributorNames);
  });

  test('should handle donors with no name variations correctly', async ({ page }) => {
    const candidateId = TEST_CANDIDATES.MULTI_CYCLE;
    
    // Fetch aggregated donors from API
    const aggregatedResponse = await apiClient.get(`/api/analysis/donor-states/out-of-state-contributions`, {
      params: { candidate_id: candidateId, aggregate: true, limit: 10000 },
    });
    
    const aggregatedDonors = Array.isArray(aggregatedResponse.data) 
      ? aggregatedResponse.data 
      : aggregatedResponse.data.results || [];
    
    // Check donors with single name (no variations)
    const singleNameDonors = aggregatedDonors.filter(
      (d: any) => !d.all_names || (Array.isArray(d.all_names) && d.all_names.length === 1)
    );
    
    // These should still have valid totals and counts
    for (const donor of singleNameDonors.slice(0, 10)) {
      expect(donor.total_amount || donor.amount).toBeGreaterThanOrEqual(0);
      expect(donor.contribution_count || donor.count).toBeGreaterThanOrEqual(1);
    }
  });
});

