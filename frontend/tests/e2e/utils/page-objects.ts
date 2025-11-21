/**
 * Page Object Models for reusable test components
 */

import { Page, Locator } from '@playwright/test';

/**
 * Base page object with common functionality
 */
export class BasePage {
  constructor(protected page: Page) {}

  /**
   * Wait for page to be fully loaded
   */
  async waitForLoad(): Promise<void> {
    await this.page.waitForLoadState('networkidle');
  }

  /**
   * Extract number from text element
   */
  async extractNumber(locator: Locator): Promise<number> {
    const text = await locator.textContent();
    if (!text) return 0;
    
    // Remove currency symbols, commas, and whitespace
    const cleaned = text.replace(/[$,\s]/g, '');
    const parsed = parseFloat(cleaned);
    return isNaN(parsed) ? 0 : parsed;
  }

  /**
   * Extract text content
   */
  async extractText(locator: Locator): Promise<string> {
    const text = await locator.textContent();
    return text?.trim() || '';
  }
}

/**
 * Candidate Detail Page Object
 */
export class CandidateDetailPage extends BasePage {
  // Selectors
  private readonly candidateNameSelector = '[data-testid="candidate-name"], h1, h2';
  private readonly financialSummarySelector = '[data-testid="financial-summary"]';
  private readonly totalContributionsSelector = '[data-testid="total-contributions"], text=/total.*contribution/i';
  private readonly averageContributionSelector = '[data-testid="average-contribution"], text=/average.*contribution/i';
  private readonly cycleSelector = '[data-testid="cycle-selector"], select[name*="cycle"]';

  /**
   * Get candidate name
   */
  async getCandidateName(): Promise<string> {
    const nameElement = this.page.locator(this.candidateNameSelector).first();
    return await this.extractText(nameElement);
  }

  /**
   * Get total contributions value
   */
  async getTotalContributions(): Promise<number> {
    const totalElement = this.page.locator(this.totalContributionsSelector).first();
    // Try to find the number near the label
    const text = await totalElement.textContent();
    if (!text) return 0;
    
    // Extract number from text like "Total Contributions: $123,456.78"
    const match = text.match(/[\d,]+\.?\d*/);
    if (match) {
      return parseFloat(match[0].replace(/,/g, ''));
    }
    return 0;
  }

  /**
   * Get average contribution value
   */
  async getAverageContribution(): Promise<number> {
    const avgElement = this.page.locator(this.averageContributionSelector).first();
    const text = await avgElement.textContent();
    if (!text) return 0;
    
    const match = text.match(/[\d,]+\.?\d*/);
    if (match) {
      return parseFloat(match[0].replace(/,/g, ''));
    }
    return 0;
  }

  /**
   * Select cycle
   */
  async selectCycle(cycle: number): Promise<void> {
    const cycleSelect = this.page.locator(this.cycleSelector).first();
    await cycleSelect.selectOption(String(cycle));
    await this.page.waitForTimeout(500); // Wait for data to load
  }

  /**
   * Get financial summary values
   */
  async getFinancialSummary(): Promise<{
    totalReceipts: number;
    totalDisbursements: number;
    cashOnHand: number;
    totalContributions: number;
  }> {
    // Try to find financial summary section
    const summarySection = this.page.locator(this.financialSummarySelector);
    
    // Extract values from text content
    const text = await summarySection.textContent() || '';
    
    const extractValue = (label: string): number => {
      const regex = new RegExp(`${label}[\\s:]*\\$?([\\d,]+(?:\\.\\d+)?)`, 'i');
      const match = text.match(regex);
      return match ? parseFloat(match[1].replace(/,/g, '')) : 0;
    };

    return {
      totalReceipts: extractValue('total receipts|receipts'),
      totalDisbursements: extractValue('total disbursements|disbursements'),
      cashOnHand: extractValue('cash on hand'),
      totalContributions: extractValue('total contributions|contributions'),
    };
  }

  /**
   * Navigate to candidate page
   */
  async goto(candidateId: string): Promise<void> {
    await this.page.goto(`/candidates/${candidateId}`);
    await this.waitForLoad();
  }
}

/**
 * Donor Analysis Page Object
 */
export class DonorAnalysisPage extends BasePage {
  /**
   * Navigate to donor analysis page
   */
  async goto(candidateId: string, cycle?: number): Promise<void> {
    let url = `/donor-analysis?candidate_id=${candidateId}`;
    if (cycle) {
      url += `&cycle=${cycle}`;
    }
    await this.page.goto(url);
    await this.waitForLoad();
  }

  /**
   * Get summary statistics
   */
  async getSummaryStats(): Promise<{
    totalAmount: number;
    totalContributions: number;
    uniqueDonors: number;
    averageContribution: number;
  }> {
    // Find summary cards or statistics section
    const summarySection = this.page.locator('[data-testid="summary-stats"], .summary-stats, .statistics');
    const text = await summarySection.textContent() || '';
    
    const extractValue = (label: string): number => {
      const regex = new RegExp(`${label}[\\s:]*\\$?([\\d,]+(?:\\.\\d+)?)`, 'i');
      const match = text.match(regex);
      return match ? parseFloat(match[1].replace(/,/g, '')) : 0;
    };

    return {
      totalAmount: extractValue('total amount|total'),
      totalContributions: extractValue('total contributions|contributions'),
      uniqueDonors: extractValue('unique donors|donors'),
      averageContribution: extractValue('average|avg'),
    };
  }
}

/**
 * Fraud Analysis Page Object
 */
export class FraudAnalysisPage extends BasePage {
  /**
   * Navigate to fraud analysis (usually part of candidate detail)
   */
  async goto(candidateId: string, cycle?: number): Promise<void> {
    await this.page.goto(`/candidates/${candidateId}`);
    await this.waitForLoad();
    
    // Scroll to fraud section or click fraud tab if exists
    const fraudSection = this.page.locator('[data-testid="fraud-section"], text=/fraud/i');
    if (await fraudSection.count() > 0) {
      await fraudSection.first().scrollIntoViewIfNeeded();
    }
  }

  /**
   * Get risk score
   */
  async getRiskScore(): Promise<number> {
    const riskElement = this.page.locator('[data-testid="risk-score"], text=/risk.*score/i').first();
    const text = await riskElement.textContent() || '';
    const match = text.match(/(\d+(?:\.\d+)?)/);
    return match ? parseFloat(match[1]) : 0;
  }

  /**
   * Get total suspicious amount
   */
  async getTotalSuspiciousAmount(): Promise<number> {
    const amountElement = this.page.locator('[data-testid="suspicious-amount"], text=/suspicious.*amount/i').first();
    const text = await amountElement.textContent() || '';
    const match = text.match(/\$?([\d,]+(?:\.\d+)?)/);
    return match ? parseFloat(match[1].replace(/,/g, '')) : 0;
  }
}

