# Investigation Summary: Missing Contributions for H6TX21301

**Date:** November 19, 2025  
**Candidate:** H6TX21301 (TEIXEIRA, MARK CHARLES)  
**Committee:** C00917658 (MARK TEIXEIRA FOR CONGRESS)

## Key Findings

### ✅ What's Working

1. **Database Integrity:**
   - Committee C00917658 exists in database ✓
   - Committee correctly linked to candidate H6TX21301 ✓
   - Candidate exists in database ✓
   - Linkage data imported correctly ✓

2. **Query Logic:**
   - Query builder correctly finds committees linked to candidate ✓
   - OR condition (candidate_id OR committee_id) works correctly ✓
   - API fallback mechanism is in place ✓

3. **Bulk Data Import:**
   - 9.2M contributions successfully imported for 2026 cycle ✓
   - All data types imported correctly ✓
   - Import process working as expected ✓

### ❌ The Problem

**Contributions for committee C00917658 are NOT in the bulk data file:**

1. **Bulk Data File Check:**
   - File: `backend/data/bulk/individual_contributions_2026.txt` (1.6GB)
   - Searched for committee_id "C00917658": **0 matches**
   - The file does NOT contain any contributions for this committee

2. **FEC API Check:**
   - Committee exists in API ✓
   - Direct contributions query for committee: **0 contributions**
   - Direct contributions query for candidate: **2 contributions** (both $0.00)
   - Financial totals API shows: **$150.4K in individual contributions**

3. **Database Check:**
   - 0 contributions found for committee C00917658
   - 2 contributions found with candidate_id=H6TX21301 (both $0.00, wrong committee)

## Root Cause Analysis

The $150.4K shown in the frontend comes from the **FEC API's financial totals endpoint**, not from individual contribution records. This is a discrepancy between:

1. **Financial Totals API** (`/totals/candidate/`) - Shows aggregated totals including $150.4K
2. **Individual Contributions API** (`/schedules/schedule_a/`) - Returns 0 contributions
3. **Bulk Data File** - Contains 0 contributions for this committee

### Possible Explanations

1. **Timing Issue:**
   - Contributions may have been filed after the bulk data snapshot
   - Bulk data files are periodic snapshots, not real-time
   - Financial totals are updated more frequently than bulk data

2. **Data Source Mismatch:**
   - Financial totals may include contributions from other sources
   - May include contributions from other committees linked to the candidate
   - May include contributions that haven't been processed into Schedule A yet

3. **FEC Data Processing Delay:**
   - Individual contribution records may be processed with a delay
   - Financial totals are calculated from filings, which may be available before detailed records

4. **Committee Type:**
   - This is a candidate committee (authorized committee)
   - Some contribution types may not appear in Schedule A bulk data
   - May need to check other data types (other_transactions, etc.)

## Verification Steps Completed

1. ✅ Verified bulk data file does not contain C00917658
2. ✅ Verified database has 0 contributions for this committee
3. ✅ Verified FEC API returns 0 individual contributions
4. ✅ Verified committee exists and is linked correctly
5. ✅ Verified query logic works correctly
6. ✅ Verified financial totals API shows $150.4K

## Recommendations

### Immediate Actions

1. **Check Other Data Types:**
   - Check `other_transactions` bulk data for this committee
   - Contributions may be in a different data type
   - Command: `grep -c "C00917658" backend/data/bulk/other_transactions_2026.txt`

2. **Check All Cycles:**
   - Contributions may be in 2024 cycle instead of 2026
   - Check if contributions exist in other cycles
   - Command: `grep -c "C00917658" backend/data/bulk/individual_contributions_*.txt`

3. **Verify Financial Totals Source:**
   - The $150.4K may come from a different source
   - May include PAC contributions, loans, or other transaction types
   - Check what the financial totals API actually includes

4. **Wait for Updated Bulk Data:**
   - If contributions were filed recently, they may appear in next bulk data update
   - Financial totals are updated more frequently than bulk data files

### Long-term Improvements

1. **Add Data Completeness Warnings:**
   - When financial totals show contributions but database has none, show a warning
   - Alert users when there's a discrepancy between totals and individual records

2. **Enhanced Logging:**
   - Log when financial totals don't match individual contribution counts
   - Track committees with totals but no individual records

3. **Fallback Strategy:**
   - When individual contributions are missing, use financial totals as estimate
   - Show "Estimated from financial totals" when individual records unavailable

## Conclusion

The issue is **NOT** with the import system or query logic. The contributions for committee C00917658 are simply not present in:
- The bulk data file
- The FEC API's individual contributions endpoint
- The database (because they weren't imported)

However, the FEC API's financial totals endpoint shows $150.4K, suggesting the contributions exist somewhere but are not accessible via the standard individual contributions query.

This is a **data availability issue**, not a code issue. The system is working correctly - it's just that the data isn't available in the expected format/location.

