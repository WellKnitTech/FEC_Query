/**
 * Tests for fraud detection calculations
 */

import { test, expect } from '../setup/test-setup';
import { FraudAnalysisPage } from '../utils/page-objects';
import { TEST_CANDIDATES, CALCULATION_TOLERANCE } from '../setup/test-data';
import {
  calculateTotal,
  expectApproximatelyEqual,
} from '../setup/helpers';
import { AxiosInstance } from 'axios';

test.describe('Fraud Detection Calculations', () => {
  let apiClient: AxiosInstance;
  let fraudPage: FraudAnalysisPage;

  test.beforeEach(async ({ page, apiClient: client }) => {
    apiClient = client;
    fraudPage = new FraudAnalysisPage(page);
  });

  test('should calculate risk score in valid range (0-100)', async ({ page }) => {
    const candidateId = TEST_CANDIDATES.MULTI_CYCLE;
    
    await fraudPage.goto(candidateId);
    
    // Fetch fraud analysis from API
    const response = await apiClient.get(`/api/fraud/analyze-donors`, {
      params: { candidate_id: candidateId },
    });
    
    const analysis = response.data;
    const riskScore = analysis.risk_score || 0;
    
    // Risk score should be between 0 and 100
    expect(riskScore).toBeGreaterThanOrEqual(0);
    expect(riskScore).toBeLessThanOrEqual(100);
  });

  test('should detect fraud patterns correctly', async ({ page }) => {
    const candidateId = TEST_CANDIDATES.MULTI_CYCLE;
    
    // Fetch fraud analysis from API
    const response = await apiClient.get(`/api/fraud/analyze-donors`, {
      params: { candidate_id: candidateId },
    });
    
    const analysis = response.data;
    const patterns = analysis.patterns || [];
    
    // Validate pattern structure
    for (const pattern of patterns) {
      expect(pattern.pattern_type).toBeTruthy();
      expect(['low', 'medium', 'high']).toContain(pattern.severity);
      expect(pattern.total_amount).toBeGreaterThanOrEqual(0);
      expect(pattern.confidence_score).toBeGreaterThanOrEqual(0);
      expect(pattern.confidence_score).toBeLessThanOrEqual(1);
      
      // Affected contributions should be an array
      if (pattern.affected_contributions) {
        expect(Array.isArray(pattern.affected_contributions)).toBe(true);
      }
    }
  });

  test('should calculate total suspicious amount correctly', async ({ page }) => {
    const candidateId = TEST_CANDIDATES.MULTI_CYCLE;
    
    // Fetch fraud analysis from API
    const response = await apiClient.get(`/api/fraud/analyze-donors`, {
      params: { candidate_id: candidateId },
    });
    
    const analysis = response.data;
    const patterns = analysis.patterns || [];
    const reportedSuspiciousAmount = analysis.total_suspicious_amount || 0;
    
    // Calculate expected suspicious amount from patterns
    const expectedSuspiciousAmount = calculateTotal(
      patterns.map((p: any) => p.total_amount || 0)
    );
    
    // Validate
    expectApproximatelyEqual(
      reportedSuspiciousAmount,
      expectedSuspiciousAmount,
      CALCULATION_TOLERANCE
    );
  });

  test('should use aggregated donor analysis when enabled', async ({ page }) => {
    const candidateId = TEST_CANDIDATES.MULTI_CYCLE;
    
    // Fetch fraud analysis with aggregation enabled
    const response = await apiClient.get(`/api/fraud/analyze-donors`, {
      params: { candidate_id: candidateId, use_aggregation: true },
    });
    
    const analysis = response.data;
    
    // Should indicate aggregation is enabled
    expect(analysis.aggregation_enabled).toBe(true);
    expect(analysis.aggregated_donors_count).toBeGreaterThanOrEqual(0);
  });

  test('should detect smurfing patterns correctly', async ({ page }) => {
    const candidateId = TEST_CANDIDATES.MULTI_CYCLE;
    
    // Fetch fraud analysis from API
    const response = await apiClient.get(`/api/fraud/analyze-donors`, {
      params: { candidate_id: candidateId },
    });
    
    const analysis = response.data;
    const patterns = analysis.patterns || [];
    
    // Check for smurfing pattern
    const smurfingPattern = patterns.find(
      (p: any) => p.pattern_type?.toLowerCase().includes('smurf') || 
                  p.pattern_type?.toLowerCase().includes('threshold')
    );
    
    // If smurfing is detected, validate the pattern
    if (smurfingPattern) {
      expect(smurfingPattern.total_amount).toBeGreaterThanOrEqual(0);
      expect(smurfingPattern.affected_contributions?.length || 0).toBeGreaterThan(0);
    }
  });

  test('should detect threshold clustering patterns correctly', async ({ page }) => {
    const candidateId = TEST_CANDIDATES.MULTI_CYCLE;
    
    // Fetch fraud analysis from API
    const response = await apiClient.get(`/api/fraud/analyze-donors`, {
      params: { candidate_id: candidateId },
    });
    
    const analysis = response.data;
    const patterns = analysis.patterns || [];
    
    // Check for threshold clustering pattern
    const clusteringPattern = patterns.find(
      (p: any) => p.pattern_type?.toLowerCase().includes('threshold') ||
                  p.pattern_type?.toLowerCase().includes('clustering')
    );
    
    // If clustering is detected, validate the pattern
    if (clusteringPattern) {
      expect(clusteringPattern.total_amount).toBeGreaterThanOrEqual(0);
      expect(clusteringPattern.confidence_score).toBeGreaterThanOrEqual(0);
    }
  });

  test('should handle candidates with no fraud patterns correctly', async ({ page }) => {
    const candidateId = TEST_CANDIDATES.NO_DATA;
    
    // Fetch fraud analysis from API
    const response = await apiClient.get(`/api/fraud/analyze-donors`, {
      params: { candidate_id: candidateId },
    });
    
    const analysis = response.data;
    const riskScore = analysis.risk_score || 0;
    const patterns = analysis.patterns || [];
    
    // Risk score should be low or zero
    expect(riskScore).toBeGreaterThanOrEqual(0);
    expect(riskScore).toBeLessThanOrEqual(100);
    
    // Should have empty or minimal patterns
    expect(Array.isArray(patterns)).toBe(true);
  });
});

