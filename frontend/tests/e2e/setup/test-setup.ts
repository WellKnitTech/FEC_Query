/**
 * Test setup and configuration for Playwright E2E tests
 */

import { test as base } from '@playwright/test';
import axios, { AxiosInstance } from 'axios';

// API base URL for test data fetching
const API_BASE_URL = process.env.PLAYWRIGHT_API_URL || 'http://localhost:8000';

/**
 * Extended test context with API client for fetching test data
 */
export interface TestContext {
  apiClient: AxiosInstance;
}

/**
 * Extend base test with custom fixtures
 */
export const test = base.extend<TestContext>({
  apiClient: async ({}, use) => {
    const client = axios.create({
      baseURL: API_BASE_URL,
      headers: {
        'Content-Type': 'application/json',
      },
      timeout: 30000,
    });

    await use(client);
  },
});

export { expect } from '@playwright/test';

