# Import Issues Analysis Report

**Date:** November 19, 2025  
**Cycle:** 2026

## Executive Summary

Investigation of import discrepancies revealed:

1. **Candidate Summary**: 2,740 records in file → 0 in database
   - **Root Cause**: Column name mismatch - file uses 'Cand_Id' but parser expects 'candidate_id' or 'CAND_ID'
   - **Status**: FIXED - Parser updated to handle 'Cand_Id' column name

2. **Operating Expenditures**: 418,105 records in file → 11,883 in database
   - **Root Cause**: NOT AN ISSUE - File contains 97.16% duplicate SUB_IDs
   - **Status**: CORRECT BEHAVIOR - Unique constraint correctly prevents duplicate inserts
   - **Evidence**: 418,105 total records, only 11,883 unique SUB_ID values

## Detailed Analysis

### Candidate Summary Issue

**Problem:**
- File has 2,740 records with column name 'Cand_Id'
- Parser was looking for 'candidate_id' or 'CAND_ID'
- All rows were filtered out because column not found
- Result: 0 records imported

**Solution:**
- Updated parser to also check for 'Cand_Id' and 'Cand_ID' column names
- Updated all field mappings to handle FEC column name variations:
  - 'Cand_Id' → candidate_id
  - 'Cand_Name' → candidate_name
  - 'Cand_Office' → office
  - 'Cand_Office_St' → state
  - 'Cand_Office_Dist' → district
  - 'Cand_Party_Affiliation' → party
  - 'Total_Receipt' → total_receipts
  - 'Total_Disbursement' → total_disbursements
  - 'Cash_On_Hand_COP' → cash_on_hand

**Fix Applied:**
- Modified `parse_candidate_summary()` in `app/services/bulk_data_parsers.py`
- Added 'Cand_Id' and 'Cand_ID' to column name fallback list
- Added FEC column name variations for all fields

### Operating Expenditures Analysis

**Investigation Results:**
- Total records in file: 418,105
- Unique SUB_ID values: 11,883
- Duplicate SUB_IDs: 406,222 (97.16%)
- Records in database: 11,883

**Conclusion:**
This is **NOT an import issue**. The database has a unique constraint on `expenditure_id` (derived from SUB_ID). When the parser encounters duplicate SUB_IDs, the `on_conflict_do_update` correctly skips them, keeping only the first occurrence.

**Why duplicates exist:**
- FEC bulk data files can contain multiple rows with the same SUB_ID
- This can happen when:
  - Amendments are filed (same transaction, updated information)
  - Multiple schedule entries reference the same transaction
  - Data consolidation issues

**Verification:**
- Database count (11,883) matches unique SUB_ID count (11,883) ✓
- All unique records are present in database ✓
- Import behavior is correct ✓

## Multi-Cycle Data Verification

**Confirmed:** Discrepancies in other data types are due to multiple cycles:

- **Candidate Master**: 6,522 (2026 file) vs 16,044 (all cycles in DB)
  - Database contains candidates from multiple cycles (2020, 2024, 2026)
  
- **Committee Master**: 17,308 (2026 file) vs 31,999 (all cycles in DB)
  - Database contains committees from multiple cycles
  
- **Candidate-Committee Linkage**: 6,141 (2026 file) vs 13,767 (all cycles in DB)
  - Database contains linkages from multiple cycles

**Note:** The verification script counts ALL records in database, not just cycle 2026. This is expected behavior since these tables don't have cycle-specific filtering in the count queries.

## Recommendations

1. **Re-import Candidate Summary** (after fix):
   ```bash
   # Re-import candidate_summary for cycle 2026
   # The fix will now correctly parse 'Cand_Id' column
   ```

2. **Update Verification Logic**:
   - For data types with unique constraints (like operating_expenditures), compare unique values in file vs database, not total records
   - Add cycle-specific filtering to database count queries for master files

3. **Document Expected Behavior**:
   - Document that operating_expenditures and similar tables will have fewer records than file due to duplicate handling
   - This is correct behavior, not an error

## Files Modified

1. `backend/app/services/bulk_data_parsers.py`
   - Updated `parse_candidate_summary()` to handle FEC column name variations

## Next Steps

1. Re-run candidate_summary import for cycle 2026
2. Verify candidate_summary records are now imported correctly
3. Update verification reports to account for duplicate handling in operating_expenditures

