#!/usr/bin/env python3
"""
Standalone test script for bulk import and API data integration functionality.

Run with: python test_integration_standalone.py
"""
import sys
import asyncio
from datetime import datetime
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

# Add current directory to path
sys.path.insert(0, '.')

from app.db.database import Base, Contribution
from app.utils.field_mapping import (
    normalize_from_bulk, normalize_from_api, extract_unified_field,
    get_date_field, get_amount_field, merge_raw_data
)
from app.services.fec_client import FECClient
from app.utils.date_utils import extract_date_from_raw_data

# Test database URL - use in-memory SQLite
TEST_DATABASE_URL = "sqlite+aiosqlite:///:memory:"

async def run_tests():
    """Run all integration tests"""
    print("=" * 60)
    print("Testing Bulk Import and API Data Integration")
    print("=" * 60)
    
    # Create test database
    test_engine = create_async_engine(TEST_DATABASE_URL, echo=False)
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    TestSessionLocal = async_sessionmaker(test_engine, class_=AsyncSession, expire_on_commit=False)
    
    tests_passed = 0
    tests_failed = 0
    
    # Test 1: Field Mapping - Normalize from Bulk
    print("\n[Test 1] Normalize from Bulk Import Data")
    try:
        bulk_data = {
            'SUB_ID': '12345',
            'TRANSACTION_DT': '01012024',
            'TRANSACTION_AMT': '1000.00',
            'CMTE_ID': 'C001',
            'NAME': 'John Doe'
        }
        normalized = normalize_from_bulk(bulk_data)
        assert normalized['contribution_id'] == '12345'
        assert normalized['contribution_date'] == '01012024'
        assert normalized['contribution_amount'] == '1000.00'
        assert normalized['committee_id'] == 'C001'
        assert normalized['contributor_name'] == 'John Doe'
        print("  ✓ PASSED")
        tests_passed += 1
    except Exception as e:
        print(f"  ✗ FAILED: {e}")
        tests_failed += 1
    
    # Test 2: Field Mapping - Normalize from API
    print("\n[Test 2] Normalize from API Data")
    try:
        api_data = {
            'sub_id': '12345',
            'contribution_receipt_date': '2024-01-01',
            'contribution_receipt_amount': 1000.0,
            'committee_id': 'C001',
            'contributor_name': 'John Doe'
        }
        normalized = normalize_from_api(api_data)
        assert normalized['contribution_id'] == '12345'
        assert normalized['contribution_date'] == '2024-01-01'
        assert normalized['contribution_amount'] == 1000.0
        assert normalized['committee_id'] == 'C001'
        print("  ✓ PASSED")
        tests_passed += 1
    except Exception as e:
        print(f"  ✗ FAILED: {e}")
        tests_failed += 1
    
    # Test 3: Merge Raw Data - Preserve Bulk Fields
    print("\n[Test 3] Merge Raw Data - Preserve Bulk Import Fields")
    try:
        existing_raw_data = {
            'TRANSACTION_DT': '01012024',
            'TRANSACTION_AMT': '1000.00',
            'CMTE_ID': 'C001'
        }
        new_raw_data = {
            'contribution_receipt_date': '2024-01-01',
            'contribution_receipt_amount': 1000.0
        }
        merged = merge_raw_data(existing_raw_data, new_raw_data, 'api')
        assert 'TRANSACTION_DT' in merged
        assert merged['TRANSACTION_DT'] == '01012024'
        assert 'TRANSACTION_AMT' in merged
        assert 'contribution_receipt_date' in merged
        print("  ✓ PASSED")
        tests_passed += 1
    except Exception as e:
        print(f"  ✗ FAILED: {e}")
        tests_failed += 1
    
    # Test 4: Smart Merge - Preserve Existing Data
    print("\n[Test 4] Smart Merge - Preserve Existing Non-NULL Values")
    try:
        async with TestSessionLocal() as session:
            existing = Contribution(
                contribution_id='TEST_001',
                contributor_name='John Doe',
                contributor_city='Washington',
                contribution_amount=1000.0,
                contribution_date=datetime(2024, 1, 1),
                data_source='bulk',
                last_updated_from='bulk',
                raw_data={'TRANSACTION_DT': '01012024', 'TRANSACTION_AMT': '1000.00'}
            )
            session.add(existing)
            await session.commit()
            
            new_data = {
                'contributor_name': None,  # Should not overwrite
                'contributor_state': 'DC',  # Should add
                'contribution_amount': None,  # Should not overwrite
                'raw_data': {'contribution_receipt_date': '2024-01-01'}
            }
            
            fec_client = FECClient()
            merged = fec_client._smart_merge_contribution(existing, new_data, 'api')
            
            assert merged.contributor_name == 'John Doe'
            assert merged.contributor_city == 'Washington'
            assert merged.contribution_amount == 1000.0
            assert merged.contributor_state == 'DC'
            assert merged.data_source == 'both'
            assert merged.last_updated_from == 'api'
            assert 'TRANSACTION_DT' in merged.raw_data
            assert 'contribution_receipt_date' in merged.raw_data
            
            print("  ✓ PASSED")
            tests_passed += 1
    except Exception as e:
        print(f"  ✗ FAILED: {e}")
        import traceback
        traceback.print_exc()
        tests_failed += 1
    
    # Test 5: Smart Merge - Prefer New Non-NULL Values
    print("\n[Test 5] Smart Merge - Prefer New Non-NULL Values")
    try:
        async with TestSessionLocal() as session:
            existing = Contribution(
                contribution_id='TEST_002',
                contributor_name='John Doe',
                contribution_amount=1000.0,
                data_source='bulk',
                raw_data={'TRANSACTION_DT': '01012024'}
            )
            session.add(existing)
            await session.commit()
            
            new_data = {
                'contributor_name': 'Jane Smith',
                'contribution_amount': 2000.0,
                'raw_data': {'contribution_receipt_date': '2024-01-01'}
            }
            
            fec_client = FECClient()
            merged = fec_client._smart_merge_contribution(existing, new_data, 'api')
            
            assert merged.contributor_name == 'Jane Smith'
            assert merged.contribution_amount == 2000.0
            
            print("  ✓ PASSED")
            tests_passed += 1
    except Exception as e:
        print(f"  ✗ FAILED: {e}")
        import traceback
        traceback.print_exc()
        tests_failed += 1
    
    # Test 6: Date Extraction from Bulk Data
    print("\n[Test 6] Date Extraction from Bulk Import Data")
    try:
        raw_data = {'TRANSACTION_DT': '01012024'}
        date = extract_date_from_raw_data(raw_data)
        assert date is not None
        assert isinstance(date, datetime)
        assert date.year == 2024
        assert date.month == 1
        assert date.day == 1
        print("  ✓ PASSED")
        tests_passed += 1
    except Exception as e:
        print(f"  ✗ FAILED: {e}")
        tests_failed += 1
    
    # Test 7: Date Extraction from API Data
    print("\n[Test 7] Date Extraction from API Data")
    try:
        raw_data = {'contribution_receipt_date': '2024-01-01'}
        date = extract_date_from_raw_data(raw_data)
        assert date is not None
        assert isinstance(date, datetime)
        assert date.year == 2024
        assert date.month == 1
        assert date.day == 1
        print("  ✓ PASSED")
        tests_passed += 1
    except Exception as e:
        print(f"  ✗ FAILED: {e}")
        tests_failed += 1
    
    # Test 8: Raw Data Storage as Dict
    print("\n[Test 8] Raw Data Stored as Dict (Not JSON String)")
    try:
        async with TestSessionLocal() as session:
            # Use ORM to insert - SQLAlchemy handles JSON serialization automatically
            contrib = Contribution(
                contribution_id='DICT_TEST_001',
                contributor_name='Test',
                raw_data={'TRANSACTION_DT': '01012024', 'TRANSACTION_AMT': '1000.00'},
                data_source='bulk',
                last_updated_from='bulk'
            )
            session.add(contrib)
            await session.commit()
            
            # Retrieve and check
            result = await session.execute(
                select(Contribution).where(Contribution.contribution_id == 'DICT_TEST_001')
            )
            contrib_retrieved = result.scalar_one_or_none()
            
            assert contrib_retrieved is not None
            assert isinstance(contrib_retrieved.raw_data, dict)
            assert contrib_retrieved.raw_data['TRANSACTION_DT'] == '01012024'
            assert contrib_retrieved.data_source == 'bulk'
            
            print("  ✓ PASSED")
            tests_passed += 1
    except Exception as e:
        print(f"  ✗ FAILED: {e}")
        import traceback
        traceback.print_exc()
        tests_failed += 1
    
    # Test 9: Data Source Tracking
    print("\n[Test 9] Data Source Tracking")
    try:
        async with TestSessionLocal() as session:
            # Start with bulk
            contrib = Contribution(
                contribution_id='SOURCE_TEST_001',
                contributor_name='Test',
                data_source='bulk',
                last_updated_from='bulk',
                raw_data={'TRANSACTION_DT': '01012024'}
            )
            session.add(contrib)
            await session.commit()
            
            # Merge with API data
            fec_client = FECClient()
            fec_client._smart_merge_contribution(
                contrib,
                {'raw_data': {'contribution_receipt_date': '2024-01-01'}},
                'api'
            )
            await session.commit()
            
            assert contrib.data_source == 'both'
            assert contrib.last_updated_from == 'api'
            
            print("  ✓ PASSED")
            tests_passed += 1
    except Exception as e:
        print(f"  ✗ FAILED: {e}")
        import traceback
        traceback.print_exc()
        tests_failed += 1
    
    # Cleanup
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await test_engine.dispose()
    
    # Summary
    print("\n" + "=" * 60)
    print(f"Test Results: {tests_passed} passed, {tests_failed} failed")
    print("=" * 60)
    
    if tests_failed == 0:
        print("\n✓ All tests passed!")
        return 0
    else:
        print(f"\n✗ {tests_failed} test(s) failed")
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(run_tests())
    sys.exit(exit_code)

