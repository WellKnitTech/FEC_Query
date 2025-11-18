# Candidate Page Component Testing - Execution Checklist

## Test Session Information
- **Tester Name**: _______________________
- **Test Date**: _______________________
- **Browser**: _______________________
- **OS**: _______________________
- **Backend URL**: _______________________
- **Frontend URL**: _______________________

## Test Candidate IDs
- **Multi-cycle candidate**: _______________________
- **Single-cycle candidate**: _______________________
- **No data candidate**: _______________________

---

## 1. Page Header & Candidate Information

**Test Candidate ID**: _______________________

- [ ] Candidate name displays correctly
- [ ] Office information displays (if available)
- [ ] Party affiliation displays (if available)
- [ ] Election years display correctly
- [ ] Export button is present and clickable
- [ ] Test with candidate missing some fields

**Notes**: 
_________________________________________________

---

## 2. Contact Information Section

**Test Candidate ID**: _______________________

- [ ] Contact information displays when available
- [ ] "No contact information" message when missing
- [ ] "Refresh Contact Info" button works
- [ ] Button disabled state during refresh
- [ ] "Last updated" timestamp displays correctly
- [ ] Test with partial contact info

**Notes**: 
_________________________________________________

---

## 3. Cycle Indicator Banner

**Test Candidate ID**: _______________________

- [ ] Banner displays when cycle is selected
- [ ] Cycle number displays correctly (e.g., "Cycle 2024")
- [ ] Date range displays correctly (e.g., "Jan 1, 2023 - Dec 31, 2024")
- [ ] Formatted range displays (e.g., "Range: 2023-2024")
- [ ] Cycle selector dropdown appears when multiple cycles available
- [ ] Cycle switching via dropdown works
- [ ] Banner hides when no cycle selected
- [ ] Test with single cycle (no dropdown)
- [ ] Verify date range calculation: Cycle YYYY = (YYYY-1)-01-01 to YYYY-12-31

**Date Range Verification**:
- Cycle 2024 should show: Jan 1, 2023 - Dec 31, 2024
- Cycle 2022 should show: Jan 1, 2021 - Dec 31, 2022

**Notes**: 
_________________________________________________

---

## 4. FinancialSummary Component

**Test Candidate ID**: _______________________

- [ ] Financial metrics display (Total Receipts, Cash on Hand, etc.)
- [ ] Currency formatting is correct
- [ ] Cycle selector appears when multiple cycles available
- [ ] Cycle switching within component works
- [ ] Data updates when cycle changes
- [ ] Test with candidate having no financial data
- [ ] "Latest" option in cycle selector works
- [ ] Loading state displays correctly
- [ ] Error state displays correctly

**Data Verification**:
- Total Receipts: _______________________
- Cash on Hand: _______________________
- Total Disbursements: _______________________

**Notes**: 
_________________________________________________

---

## 5. DonorStateAnalysis Component

**Test Candidate ID**: _______________________

- [ ] State breakdown displays correctly
- [ ] Data is filtered by selected cycle
- [ ] Cycle switching updates data
- [ ] "Out of state" indicators work correctly
- [ ] Test with contributions from multiple states
- [ ] Loading state works
- [ ] Empty state (no contributions) works

**Top States**:
1. _______________________
2. _______________________
3. _______________________

**Notes**: 
_________________________________________________

---

## 6. ContributionAnalysis Component

**Test Candidate ID**: _______________________

- [ ] Total contributions displays correctly
- [ ] Contribution statistics (average, count, etc.) display
- [ ] Top donors list displays
- [ ] Data is filtered by selected cycle
- [ ] Cycle switching updates data
- [ ] Contribution distribution chart displays
- [ ] Contributions by date chart displays
- [ ] Test with candidate having no contributions

**Statistics**:
- Total Contributions: _______________________
- Average Contribution: _______________________
- Total Contributors: _______________________

**Notes**: 
_________________________________________________

---

## 7. CumulativeChart Component

**Test Candidate ID**: _______________________

- [ ] Chart displays correctly
- [ ] Data is filtered by selected cycle
- [ ] Cycle switching updates chart
- [ ] Chart shows cumulative trend over time
- [ ] Date range matches cycle dates
- [ ] Test with sparse contribution data

**Chart Verification**:
- Start date matches cycle: _______________________
- End date matches cycle: _______________________
- Trend is cumulative: [ ] Yes [ ] No

**Notes**: 
_________________________________________________

---

## 8. ContributionVelocity Component

**Test Candidate ID**: _______________________

- [ ] Velocity chart displays correctly
- [ ] Data is filtered by selected cycle
- [ ] Cycle switching updates chart
- [ ] Velocity metrics (contributions per day/week) display
- [ ] Date range matches cycle dates
- [ ] Test with irregular contribution patterns

**Velocity Metrics**:
- Contributions per day: _______________________
- Contributions per week: _______________________

**Notes**: 
_________________________________________________

---

## 9. EmployerTreemap Component

**Test Candidate ID**: _______________________

- [ ] Treemap displays correctly
- [ ] Data is filtered by selected cycle
- [ ] Cycle switching updates treemap
- [ ] Employer breakdown is accurate
- [ ] Date range matches cycle dates
- [ ] Test with many employers

**Top Employers**:
1. _______________________
2. _______________________
3. _______________________

**Notes**: 
_________________________________________________

---

## 10. ExpenditureBreakdown Component

**Test Candidate ID**: _______________________

- [ ] Expenditure breakdown displays correctly
- [ ] Data is filtered by selected cycle
- [ ] Cycle switching updates breakdown
- [ ] Category aggregation works correctly
- [ ] Date range matches cycle dates
- [ ] Test with candidate having no expenditures

**Top Categories**:
1. _______________________
2. _______________________
3. _______________________

**Notes**: 
_________________________________________________

---

## 11. NetworkGraph Component

**Test Candidate ID**: _______________________

- [ ] Network graph displays correctly
- [ ] Date range displays in component header
- [ ] Data is filtered by selected cycle (via minDate/maxDate)
- [ ] Cycle switching updates graph
- [ ] "Group by Employer" toggle works
- [ ] Node and edge counts display correctly
- [ ] Interaction works (hover, drag, zoom)
- [ ] Loading state works
- [ ] Test with no network data

**Graph Metrics**:
- Nodes: _______________________
- Edges: _______________________
- Date range in header: _______________________

**Notes**: 
_________________________________________________

---

## 12. FraudRadarChart Component

**Test Candidate ID**: _______________________

- [ ] Radar chart displays correctly
- [ ] Date range displays in component header
- [ ] Data is filtered by selected cycle (via minDate/maxDate)
- [ ] Cycle switching updates chart
- [ ] Risk score displays correctly
- [ ] Pattern counts display correctly
- [ ] Suspicious amount displays correctly
- [ ] Test with no fraud patterns

**Fraud Metrics**:
- Risk Score: _______________________
- Patterns Detected: _______________________
- Suspicious Amount: _______________________
- Date range in header: _______________________

**Notes**: 
_________________________________________________

---

## 13. SmurfingScatter Component

**Test Candidate ID**: _______________________

- [ ] Scatter plot displays correctly
- [ ] Date range displays in component header
- [ ] Data is filtered by selected cycle (via minDate/maxDate)
- [ ] Cycle switching updates plot
- [ ] Suspicious groups display correctly
- [ ] Contribution counts display correctly
- [ ] Date range matches cycle dates
- [ ] Test with no smurfing patterns

**Smurfing Metrics**:
- Contributions $190-$199: _______________________
- Suspicious Groups: _______________________
- Total Suspicious Amount: _______________________
- Date range in header: _______________________

**Notes**: 
_________________________________________________

---

## 14. FraudAlerts Component

**Test Candidate ID**: _______________________

- [ ] Fraud alerts list displays correctly
- [ ] Date range displays in component header
- [ ] Data is filtered by selected cycle (via minDate/maxDate)
- [ ] Cycle switching updates alerts
- [ ] "Use Aggregated Donors" toggle works
- [ ] Risk score displays correctly
- [ ] Pattern details expand/collapse
- [ ] Severity indicators display correctly
- [ ] Test with no fraud alerts

**Alert Metrics**:
- Risk Score: _______________________
- Patterns Detected: _______________________
- Total Suspicious Amount: _______________________
- Date range in header: _______________________

**Notes**: 
_________________________________________________

---

## Cross-Component Testing

### Cycle Synchronization

**Test Candidate ID**: _______________________

- [ ] Change cycle in FinancialSummary - all components update
- [ ] Change cycle in page-level banner - all components update
- [ ] Both selectors stay in sync
- [ ] Rapid cycle switching works
- [ ] Loading states don't cause desync

**Notes**: 
_________________________________________________

---

### Data Consistency

**Test Candidate ID**: _______________________

- [ ] Contribution totals match across components
- [ ] Date ranges are consistent
- [ ] Cycle numbers match everywhere
- [ ] Calculations are correct (sums, averages, etc.)

**Cross-Reference**:
- FinancialSummary total contributions: _______________________
- ContributionAnalysis total: _______________________
- Match: [ ] Yes [ ] No

**Notes**: 
_________________________________________________

---

## Edge Cases & Error Handling

### Edge Cases

- [ ] Candidate with no financial data
- [ ] Candidate with single cycle
- [ ] Candidate with many cycles (10+)
- [ ] Candidate with no contributions
- [ ] Candidate with no expenditures
- [ ] Candidate with missing contact info
- [ ] Very large contribution amounts
- [ ] Very small contribution amounts
- [ ] Invalid candidate ID
- [ ] Network timeout scenarios

**Notes**: 
_________________________________________________

---

### Error Handling

- [ ] Error states display correctly
- [ ] Retry functionality works
- [ ] Loading states don't hang
- [ ] Graceful degradation when API fails
- [ ] Console errors are handled

**Notes**: 
_________________________________________________

---

## Performance Testing

- [ ] Page loads within acceptable time (< 3 seconds)
- [ ] Cycle switching is responsive (< 1 second)
- [ ] No unnecessary API calls
- [ ] Components don't re-render excessively
- [ ] Test with slow network connection
- [ ] No memory leaks (long session testing)

**Performance Metrics**:
- Initial page load: _______________________ seconds
- Cycle switch time: _______________________ seconds
- API calls on page load: _______________________

**Notes**: 
_________________________________________________

---

## Browser Compatibility

- [ ] Chrome - All tests pass
- [ ] Firefox - All tests pass
- [ ] Safari - All tests pass
- [ ] Edge - All tests pass
- [ ] Mobile browsers - All tests pass
- [ ] Responsive design works

**Notes**: 
_________________________________________________

---

## Issues Found

### Critical Issues
1. _________________________________________________
2. _________________________________________________
3. _________________________________________________

### Minor Issues
1. _________________________________________________
2. _________________________________________________
3. _________________________________________________

### Suggestions
1. _________________________________________________
2. _________________________________________________
3. _________________________________________________

---

## Final Verification

- [ ] All components display data for selected cycle
- [ ] Date ranges are correct for each cycle
- [ ] Cycle switching works across all components
- [ ] No data inconsistencies between components
- [ ] All error states handled gracefully
- [ ] Performance is acceptable
- [ ] UI/UX is consistent and intuitive

## Test Completion

- **Overall Status**: [ ] Pass [ ] Fail [ ] Partial
- **Date Completed**: _______________________
- **Tester Signature**: _______________________

