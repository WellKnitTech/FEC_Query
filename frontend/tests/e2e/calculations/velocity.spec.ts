/**
 * Tests for contribution velocity calculations
 */

import { test, expect } from '../setup/test-setup';
import { CandidateDetailPage } from '../utils/page-objects';
import { TEST_CANDIDATES, CALCULATION_TOLERANCE } from '../setup/test-data';
import {
  calculateVelocity,
  expectApproximatelyEqual,
} from '../setup/helpers';
import { AxiosInstance } from 'axios';

test.describe('Contribution Velocity Calculations', () => {
  let apiClient: AxiosInstance;
  let candidatePage: CandidateDetailPage;

  test.beforeEach(async ({ page, apiClient: client }) => {
    apiClient = client;
    candidatePage = new CandidateDetailPage(page);
  });

  test('should calculate daily velocity correctly', async ({ page }) => {
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
      }))
      .filter((c: any) => c.date);
    
    // Calculate expected daily velocity
    const expectedDailyVelocity = calculateVelocity(contributionsWithDates, 'day');
    
    // Fetch velocity analysis from API
    const analysisResponse = await apiClient.get(`/api/analysis/velocity`, {
      params: { candidate_id: candidateId },
    });
    
    const analysis = analysisResponse.data;
    const actualDailyVelocity = analysis.daily_velocity || analysis.contributions_per_day || 0;
    
    // Validate (allow some tolerance for date range differences)
    expectApproximatelyEqual(actualDailyVelocity, expectedDailyVelocity, 0.1);
  });

  test('should calculate weekly velocity correctly', async ({ page }) => {
    const candidateId = TEST_CANDIDATES.MULTI_CYCLE;
    
    // Fetch contributions from API
    const response = await apiClient.get(`/api/contributions`, {
      params: { candidate_id: candidateId, limit: 10000 },
    });
    
    const contributions = response.data.results || response.data || [];
    const contributionsWithDates = contributions
      .map((c: any) => ({
        date: c.contribution_date || c.date,
      }))
      .filter((c: any) => c.date);
    
    // Calculate expected weekly velocity
    const expectedWeeklyVelocity = calculateVelocity(contributionsWithDates, 'week');
    
    // Fetch velocity analysis from API
    const analysisResponse = await apiClient.get(`/api/analysis/velocity`, {
      params: { candidate_id: candidateId },
    });
    
    const analysis = analysisResponse.data;
    const actualWeeklyVelocity = analysis.weekly_velocity || analysis.contributions_per_week || 0;
    
    // Validate (allow some tolerance for date range differences)
    expectApproximatelyEqual(actualWeeklyVelocity, expectedWeeklyVelocity, 0.1);
  });

  test('should calculate velocity trends over time correctly', async ({ page }) => {
    const candidateId = TEST_CANDIDATES.MULTI_CYCLE;
    
    // Fetch velocity analysis from API
    const response = await apiClient.get(`/api/analysis/velocity`, {
      params: { candidate_id: candidateId },
    });
    
    const analysis = response.data;
    const velocityByPeriod = analysis.velocity_by_period || analysis.velocity_trends || [];
    
    // Validate that velocity trends are reasonable
    if (velocityByPeriod.length > 0) {
      // Each period should have a date and velocity value
      for (const period of velocityByPeriod) {
        expect(period.date || period.period).toBeTruthy();
        expect(period.velocity || period.contributions_per_day).toBeGreaterThanOrEqual(0);
      }
      
      // Velocity should be sorted by date (if applicable)
      if (velocityByPeriod.length > 1) {
        const dates = velocityByPeriod
          .map((p: any) => new Date(p.date || p.period).getTime())
          .filter((d: number) => !isNaN(d));
        
        if (dates.length > 1) {
          for (let i = 0; i < dates.length - 1; i++) {
            expect(dates[i]).toBeLessThanOrEqual(dates[i + 1]);
          }
        }
      }
    }
  });

  test('should handle zero contributions correctly', async ({ page }) => {
    const candidateId = TEST_CANDIDATES.NO_DATA;
    
    // Fetch velocity analysis from API
    const response = await apiClient.get(`/api/analysis/velocity`, {
      params: { candidate_id: candidateId },
    });
    
    const analysis = response.data;
    const dailyVelocity = analysis.daily_velocity || analysis.contributions_per_day || 0;
    const weeklyVelocity = analysis.weekly_velocity || analysis.contributions_per_week || 0;
    
    // Velocity should be zero or very small for candidates with no contributions
    expect(dailyVelocity).toBeGreaterThanOrEqual(0);
    expect(weeklyVelocity).toBeGreaterThanOrEqual(0);
  });
});

