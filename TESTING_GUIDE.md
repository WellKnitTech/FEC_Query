# Candidate Page Component Testing Guide

This guide provides step-by-step instructions for testing the candidate page components to ensure they display appropriate and correct data.

## Quick Start

1. **Start the application**
   ```bash
   # Start backend
   cd backend
   python -m uvicorn app.main:app --reload

   # Start frontend (in another terminal)
   cd frontend
   npm run dev
   ```

2. **Open the application**
   - Navigate to `http://localhost:5173` (or your frontend URL)
   - Open browser developer tools (F12)
   - Go to Network tab to monitor API calls
   - Go to Console tab to check for errors

3. **Prepare test candidates**
   - Identify a candidate with multiple cycles (e.g., cycles 2020, 2022, 2024)
   - Identify a candidate with single cycle
   - Note candidate IDs for testing

## Testing Workflow

### Phase 1: Basic Component Display

1. **Navigate to a candidate page**
   - Search for a candidate or navigate directly using candidate ID
   - URL format: `/candidates/{candidate_id}`

2. **Verify page loads correctly**
   - Check that candidate name displays
   - Check that all components render
   - Check browser console for errors
   - Check network tab for failed requests

3. **Test each component individually**
   - Scroll through page
   - Verify each component displays data
   - Check for loading states
   - Check for error states

### Phase 2: Cycle Filtering

1. **Verify cycle indicator banner**
   - Check that banner displays when cycle is selected
   - Verify cycle number is correct
   - Verify date range is correct
   - Formula: Cycle YYYY = (YYYY-1)-01-01 to YYYY-12-31

2. **Test cycle switching**
   - Use page-level cycle selector (if available)
   - Use FinancialSummary cycle selector
   - Verify both selectors stay in sync
   - Verify all components update

3. **Verify date ranges**
   - Check each component's date range display
   - Verify dates match selected cycle
   - Test with multiple cycles

### Phase 3: Data Accuracy

1. **Cross-reference data**
   - Compare contribution totals across components
   - Verify calculations (sums, averages)
   - Check date ranges are consistent
   - Verify cycle numbers match everywhere

2. **Test with different cycles**
   - Switch between cycles
   - Verify data changes appropriately
   - Check that old data doesn't persist

### Phase 4: Edge Cases

1. **Test with minimal data**
   - Candidate with no contributions
   - Candidate with no expenditures
   - Candidate with single cycle
   - Candidate with no financial data

2. **Test error handling**
   - Invalid candidate ID
   - Network errors (disable network)
   - API timeouts
   - Missing data scenarios

### Phase 5: Performance

1. **Measure load times**
   - Initial page load
   - Cycle switching time
   - Component rendering time

2. **Monitor API calls**
   - Check for unnecessary requests
   - Verify caching works
   - Check for duplicate requests

## Component-Specific Testing

### FinancialSummary
- **Key Checks**:
  - Currency formatting
  - Cycle selector functionality
  - Data updates on cycle change
  - Loading/error states

- **Test Data**:
  - Total Receipts should match cycle
  - Cash on Hand should be accurate
  - All metrics should update when cycle changes

### DonorStateAnalysis
- **Key Checks**:
  - State breakdown accuracy
  - Cycle filtering
  - Geographic visualization
  - Out-of-state indicators

- **Test Data**:
  - Top states should match selected cycle
  - Percentages should be accurate
  - Map/visualization should update

### ContributionAnalysis
- **Key Checks**:
  - Statistics accuracy
  - Top donors list
  - Chart displays
  - Cycle filtering

- **Test Data**:
  - Total contributions should match cycle
  - Average contribution should be correct
  - Charts should show correct time period

### NetworkGraph
- **Key Checks**:
  - Date range in header
  - Graph displays correctly
  - Cycle filtering (via minDate/maxDate)
  - Interaction (hover, drag, zoom)

- **Test Data**:
  - Date range should display in header
  - Nodes and edges should match cycle
  - Graph should update on cycle change

### Fraud Components (RadarChart, Alerts, SmurfingScatter)
- **Key Checks**:
  - Date range in headers
  - Cycle filtering (via minDate/maxDate)
  - Risk scores
  - Pattern detection

- **Test Data**:
  - All should show date range in header
  - Data should match selected cycle
  - Risk scores should be reasonable (0-100)

## Common Issues to Watch For

### Cycle Filtering Issues
- **Symptom**: Components show all-time data instead of cycle data
- **Check**: Verify date ranges are passed to components
- **Check**: Verify components use date ranges in API calls

### Data Inconsistency
- **Symptom**: Different totals across components
- **Check**: Verify all components use same cycle
- **Check**: Verify calculations are correct

### Date Range Errors
- **Symptom**: Wrong dates displayed
- **Check**: Verify cycle-to-date conversion
- **Formula**: Cycle YYYY = (YYYY-1)-01-01 to YYYY-12-31

### Performance Issues
- **Symptom**: Slow page loads or cycle switching
- **Check**: Monitor API calls
- **Check**: Check for unnecessary re-renders
- **Check**: Verify loading states work

## Testing Tools

### Browser DevTools
- **Network Tab**: Monitor API calls
- **Console Tab**: Check for errors
- **Performance Tab**: Measure load times
- **Application Tab**: Check storage/cache

### Manual Verification
- Compare with FEC API (if available)
- Check database directly
- Verify calculations manually

## Reporting Issues

When reporting issues, include:
1. **Component name**
2. **Test candidate ID**
3. **Selected cycle**
4. **Expected behavior**
5. **Actual behavior**
6. **Steps to reproduce**
7. **Screenshots** (if applicable)
8. **Browser/OS information**
9. **Console errors** (if any)
10. **Network requests** (if relevant)

## Test Completion Checklist

Before marking testing complete, verify:
- [ ] All 14 components tested
- [ ] Cycle filtering works for all components
- [ ] Date ranges are correct
- [ ] Data is consistent across components
- [ ] Error states handled gracefully
- [ ] Performance is acceptable
- [ ] No critical issues found
- [ ] All issues documented
- [ ] Test results documented

## Next Steps After Testing

1. **Document findings** in TEST_RESULTS_TEMPLATE.md
2. **Report critical issues** immediately
3. **Create tickets** for bugs found
4. **Update test checklist** with any new test cases
5. **Share results** with development team

