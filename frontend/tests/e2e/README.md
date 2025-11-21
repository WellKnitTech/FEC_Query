# E2E Testing with Playwright

This directory contains end-to-end browser tests for validating all calculation logic and functionality in the FEC Query application.

## Overview

The E2E tests use Playwright to automatically:
- Navigate through the application UI
- Extract displayed values from components
- Compare UI values with backend API responses
- Validate calculation formulas and logic
- Test edge cases and error handling

## Test Structure

```
tests/e2e/
├── setup/
│   ├── test-setup.ts      # Playwright test configuration and fixtures
│   ├── test-data.ts       # Test data fixtures and constants
│   └── helpers.ts         # Calculation validation helper functions
├── pages/
│   └── candidate-detail.spec.ts  # Cross-component validation tests
├── calculations/
│   ├── financial-totals.spec.ts
│   ├── contribution-analysis.spec.ts
│   ├── employer-analysis.spec.ts
│   ├── velocity.spec.ts
│   ├── donor-state.spec.ts
│   ├── fraud-detection.spec.ts
│   ├── expenditure-trends.spec.ts
│   ├── donor-aggregation.spec.ts
│   └── edge-cases.spec.ts
└── utils/
    └── page-objects.ts    # Page Object Models for reusable components
```

## Running Tests

### Prerequisites

1. Install dependencies:
   ```bash
   cd frontend
   npm install
   ```

2. Install Playwright browsers:
   ```bash
   npx playwright install
   ```

3. Start the backend server:
   ```bash
   cd backend
   python -m uvicorn app.main:app --reload
   ```

4. Start the frontend server (in another terminal):
   ```bash
   cd frontend
   npm run dev
   ```

### Run All Tests

```bash
npm run test:e2e
```

### Run Tests in UI Mode

```bash
npm run test:e2e:ui
```

### Run Tests in Headed Mode (see browser)

```bash
npm run test:e2e:headed
```

### Run Tests in Debug Mode

```bash
npm run test:e2e:debug
```

### Run Specific Test File

```bash
npx playwright test tests/e2e/calculations/financial-totals.spec.ts
```

### Run Tests for Specific Browser

```bash
npx playwright test --project=chromium
npx playwright test --project=firefox
npx playwright test --project=webkit
```

## Test Coverage

### Financial Totals
- Total contributions calculation
- Average contribution calculation
- Financial summary totals (receipts, disbursements, cash on hand)
- Cycle filtering and date range calculations
- Contribution type breakdowns

### Contribution Analysis
- Contribution totals match sum of individual contributions
- Contribution distribution bins
- Top donors sorting
- Contributions by date aggregations
- Contributions by state aggregations
- Data completeness percentage

### Employer Analysis
- Employer name normalization
- Employer aggregation totals
- Top employers sorting

### Contribution Velocity
- Daily/weekly velocity calculations
- Velocity trends over time

### Donor State Analysis
- State percentage calculations
- Out-of-state contribution identification
- Geographic aggregations

### Fraud Detection
- Risk score calculations (0-100 range)
- Pattern detection (smurfing, threshold clustering)
- Aggregated donor fraud analysis
- Suspicious amount totals

### Independent Expenditures
- Expenditure totals
- Support/oppose breakdowns
- Expenditure aggregations

### Trend Analysis
- Multi-cycle calculations
- Year-over-year comparisons
- Historical trend calculations

### Donor Aggregation
- Name variation matching
- Aggregated totals match sum of individual contributions
- Contribution counts per aggregated donor

### Cross-Component Validation
- Calculations are consistent across components
- Cycle filtering affects all calculations correctly
- Date range filtering is applied consistently

### Edge Cases
- Empty data handling
- Missing fields handling
- Invalid data handling
- Very large numbers
- Zero values
- Negative amounts
- Missing dates

## Configuration

Test configuration is in `playwright.config.ts`:
- Base URL: `http://localhost:5173` (frontend)
- API URL: `http://localhost:8000` (backend)
- Browsers: Chromium, Firefox, WebKit
- Retries: 2 on CI, 0 locally
- Screenshots: On failure
- Videos: On failure

## Test Data

Test candidates are defined in `setup/test-data.ts`:
- `TEST_CANDIDATES.MULTI_CYCLE`: Candidate with multiple cycles
- `TEST_CANDIDATES.SINGLE_CYCLE`: Candidate with single cycle
- `TEST_CANDIDATES.NO_DATA`: Candidate with no financial data

**Note**: Update these candidate IDs with real candidate IDs from your database.

## Writing New Tests

1. Use Page Object Models from `utils/page-objects.ts` for reusable components
2. Use calculation helpers from `setup/helpers.ts` for validation
3. Follow the pattern:
   ```typescript
   test('should calculate X correctly', async ({ page, apiClient }) => {
     // Navigate to page
     await page.goto('/path');
     
     // Extract value from UI
     const uiValue = await extractValueFromUI(page);
     
     // Fetch from API
     const apiData = await apiClient.get('/api/endpoint');
     const expectedValue = calculateExpected(apiData);
     
     // Validate
     expectApproximatelyEqual(uiValue, expectedValue, TOLERANCE);
   });
   ```

## CI/CD Integration

Tests run automatically in GitHub Actions on:
- Push to main/develop branches
- Pull requests

The CI workflow:
1. Sets up Node.js and Python
2. Installs dependencies
3. Starts backend and frontend servers
4. Runs Playwright tests
5. Uploads test reports as artifacts

## Troubleshooting

### Tests fail with "page.goto: net::ERR_CONNECTION_REFUSED"
- Ensure backend server is running on port 8000
- Ensure frontend server is running on port 5173

### Tests fail with timeout
- Increase timeout in `playwright.config.ts`
- Check that servers are responding: `curl http://localhost:8000/health`

### Tests fail with "element not found"
- Check that test data (candidate IDs) exists in database
- Verify selectors in page objects match actual UI elements
- Use Playwright's codegen to find correct selectors: `npx playwright codegen`

### Tests are flaky
- Add explicit waits: `await page.waitForSelector(...)`
- Use `waitForLoadState('networkidle')` after navigation
- Increase retries in CI configuration

## Viewing Test Reports

After running tests, view the HTML report:
```bash
npx playwright show-report
```

## Best Practices

1. **Use Page Objects**: Encapsulate page interactions in reusable Page Object Models
2. **Validate Calculations**: Always compare UI values with API data or calculated expected values
3. **Handle Edge Cases**: Test empty data, missing fields, invalid inputs
4. **Use Tolerances**: Account for floating-point precision with appropriate tolerances
5. **Keep Tests Independent**: Each test should be able to run in isolation
6. **Use Descriptive Names**: Test names should clearly describe what is being tested

