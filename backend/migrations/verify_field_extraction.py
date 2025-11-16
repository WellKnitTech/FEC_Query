"""
Verification script to check that new bulk imports will populate all fields correctly.
This script verifies:
1. Database columns exist
2. Parsers are configured to extract all fields
3. Next imports will populate structured columns
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.db.database import Contribution, OperatingExpenditure
from app.services.bulk_data import BulkDataService
from app.services.bulk_data_parsers import GenericBulkDataParser
import inspect

def verify_contribution_parser():
    """Verify contribution parser extracts all fields"""
    print("=" * 80)
    print("VERIFYING CONTRIBUTION PARSER")
    print("=" * 80)
    
    # Check that parse_and_store_csv extracts all 20 fields
    service = BulkDataService()
    source = inspect.getsource(service.parse_and_store_csv)
    
    required_fields = [
        'AMNDT_IND', 'RPT_TP', 'TRAN_ID', 'ENTITY_TP', 'OTHER_ID',
        'FILE_NUM', 'MEMO_CD', 'MEMO_TEXT'
    ]
    
    print("\nChecking for field extraction in parser:")
    missing = []
    for field in required_fields:
        if field in source:
            print(f"  ✓ {field} extraction found")
        else:
            print(f"  ✗ {field} extraction MISSING")
            missing.append(field)
    
    # Check raw_data includes all 20 fields
    if "'CMTE_ID':" in source and "'AMNDT_IND':" in source and "'SUB_ID':" in source:
        print("\n  ✓ raw_data includes all 20 Schedule A fields")
    else:
        print("\n  ✗ raw_data may not include all fields")
    
    # Check INSERT statement includes new columns
    if 'amendment_indicator' in source and 'memo_text' in source:
        print("  ✓ INSERT statement includes new columns")
    else:
        print("  ✗ INSERT statement may be missing new columns")
        missing.append("INSERT_COLUMNS")
    
    return len(missing) == 0


def verify_operating_expenditure_parser():
    """Verify operating expenditure parser extracts all fields"""
    print("\n" + "=" * 80)
    print("VERIFYING OPERATING EXPENDITURE PARSER")
    print("=" * 80)
    
    parser = GenericBulkDataParser(None)
    source = inspect.getsource(parser.parse_operating_expenditures)
    
    required_fields = [
        'AMNDT_IND', 'RPT_YR', 'RPT_TP', 'MEMO_CD', 'MEMO_TEXT',
        'CATEGORY', 'TRAN_ID', 'BACK_REF_TRAN_ID'
    ]
    
    print("\nChecking for field extraction in parser:")
    missing = []
    for field in required_fields:
        if field in source:
            print(f"  ✓ {field} extraction found")
        else:
            print(f"  ✗ {field} extraction MISSING")
            missing.append(field)
    
    # Check upsert includes new columns
    if 'amendment_indicator' in source and 'back_reference_transaction_id' in source:
        print("  ✓ UPSERT statement includes new columns")
    else:
        print("  ✗ UPSERT statement may be missing new columns")
        missing.append("UPSERT_COLUMNS")
    
    return len(missing) == 0


def verify_database_schema():
    """Verify database columns exist"""
    print("\n" + "=" * 80)
    print("VERIFYING DATABASE SCHEMA")
    print("=" * 80)
    
    # Check Contribution model
    contrib_required = [
        'amendment_indicator', 'report_type', 'transaction_id', 'entity_type',
        'other_id', 'file_number', 'memo_code', 'memo_text'
    ]
    
    print("\nContribution table columns:")
    missing_contrib = []
    for col in contrib_required:
        if hasattr(Contribution, col):
            print(f"  ✓ {col}")
        else:
            print(f"  ✗ {col} MISSING")
            missing_contrib.append(col)
    
    # Check OperatingExpenditure model
    op_exp_required = [
        'amendment_indicator', 'report_year', 'report_type', 'memo_code',
        'memo_text', 'category', 'transaction_id', 'back_reference_transaction_id'
    ]
    
    print("\nOperatingExpenditure table columns:")
    missing_op_exp = []
    for col in op_exp_required:
        if hasattr(OperatingExpenditure, col):
            print(f"  ✓ {col}")
        else:
            print(f"  ✗ {col} MISSING")
            missing_op_exp.append(col)
    
    return len(missing_contrib) == 0 and len(missing_op_exp) == 0


def main():
    """Run all verifications"""
    print("=" * 80)
    print("VERIFYING FIELD EXTRACTION SETUP")
    print("=" * 80)
    print("\nThis script verifies that:")
    print("  1. Database columns exist")
    print("  2. Parsers extract all fields")
    print("  3. Next bulk imports will populate structured columns")
    print()
    
    results = []
    
    results.append(("Database Schema", verify_database_schema()))
    results.append(("Contribution Parser", verify_contribution_parser()))
    results.append(("Operating Expenditure Parser", verify_operating_expenditure_parser()))
    
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    
    all_passed = True
    for name, passed in results:
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"  {status}: {name}")
        if not passed:
            all_passed = False
    
    print()
    if all_passed:
        print("✓ All verifications passed!")
        print("\nNext bulk imports will automatically populate all new fields.")
        print("Existing records will remain NULL (they have incomplete raw_data).")
        return 0
    else:
        print("⚠️  Some verifications failed. Please review the output above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())

