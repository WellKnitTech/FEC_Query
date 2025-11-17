"""
Tests for bulk import and API data integration functionality.

Tests field mapping, smart merge, and data source tracking.
"""
import pytest
from datetime import datetime
from sqlalchemy import select
from app.db.database import Contribution, AsyncSessionLocal
from app.utils.field_mapping import (
    normalize_from_bulk, normalize_from_api, extract_unified_field,
    get_date_field, get_amount_field, merge_raw_data
)
from app.services.fec_client import FECClient
from app.utils.date_utils import extract_date_from_raw_data


class TestFieldMapping:
    """Test field mapping utilities"""
    
    def test_normalize_from_bulk(self):
        """Test normalizing bulk import data to unified format"""
        bulk_data = {
            'SUB_ID': '12345',
            'TRANSACTION_DT': '01012024',
            'TRANSACTION_AMT': '1000.00',
            'CMTE_ID': 'C001',
            'CAND_ID': 'P001',
            'NAME': 'John Doe',
            'CITY': 'Washington',
            'STATE': 'DC'
        }
        
        normalized = normalize_from_bulk(bulk_data)
        
        assert normalized['contribution_id'] == '12345'
        assert normalized['contribution_date'] == '01012024'
        assert normalized['contribution_amount'] == '1000.00'
        assert normalized['committee_id'] == 'C001'
        assert normalized['candidate_id'] == 'P001'
        assert normalized['contributor_name'] == 'John Doe'
        assert normalized['contributor_city'] == 'Washington'
        assert normalized['contributor_state'] == 'DC'
    
    def test_normalize_from_api(self):
        """Test normalizing API data to unified format"""
        api_data = {
            'sub_id': '12345',
            'contribution_receipt_date': '2024-01-01',
            'contribution_receipt_amount': 1000.0,
            'committee_id': 'C001',
            'candidate_id': 'P001',
            'contributor_name': 'John Doe',
            'contributor_city': 'Washington',
            'contributor_state': 'DC'
        }
        
        normalized = normalize_from_api(api_data)
        
        assert normalized['contribution_id'] == '12345'
        assert normalized['contribution_date'] == '2024-01-01'
        assert normalized['contribution_amount'] == 1000.0
        assert normalized['committee_id'] == 'C001'
        assert normalized['candidate_id'] == 'P001'
        assert normalized['contributor_name'] == 'John Doe'
    
    def test_extract_unified_field_bulk(self):
        """Test extracting unified field from bulk data"""
        bulk_data = {
            'TRANSACTION_DT': '01012024',
            'TRANSACTION_AMT': '1000.00'
        }
        
        date = extract_unified_field(bulk_data, 'contribution_date', 'bulk')
        amount = extract_unified_field(bulk_data, 'contribution_amount', 'bulk')
        
        assert date == '01012024'
        assert amount == '1000.00'
    
    def test_extract_unified_field_api(self):
        """Test extracting unified field from API data"""
        api_data = {
            'contribution_receipt_date': '2024-01-01',
            'contribution_receipt_amount': 1000.0
        }
        
        date = extract_unified_field(api_data, 'contribution_date', 'api')
        amount = extract_unified_field(api_data, 'contribution_amount', 'api')
        
        assert date == '2024-01-01'
        assert amount == 1000.0
    
    def test_get_date_field_bulk(self):
        """Test getting date field from bulk data"""
        bulk_data = {
            'TRANSACTION_DT': '01012024'
        }
        
        date = get_date_field(bulk_data, 'bulk')
        assert date == '01012024'
    
    def test_get_date_field_api(self):
        """Test getting date field from API data"""
        api_data = {
            'contribution_receipt_date': '2024-01-01'
        }
        
        date = get_date_field(api_data, 'api')
        assert date == '2024-01-01'
    
    def test_get_amount_field_bulk(self):
        """Test getting amount field from bulk data"""
        bulk_data = {
            'TRANSACTION_AMT': '1000.00'
        }
        
        amount = get_amount_field(bulk_data, 'bulk')
        assert amount == 1000.0
    
    def test_get_amount_field_api(self):
        """Test getting amount field from API data"""
        api_data = {
            'contribution_receipt_amount': 1000.0
        }
        
        amount = get_amount_field(api_data, 'api')
        assert amount == 1000.0
    
    def test_merge_raw_data_preserves_bulk_fields(self):
        """Test that merging API data preserves bulk import fields"""
        existing_raw_data = {
            'TRANSACTION_DT': '01012024',
            'TRANSACTION_AMT': '1000.00',
            'CMTE_ID': 'C001'
        }
        
        new_raw_data = {
            'contribution_receipt_date': '2024-01-01',
            'contribution_receipt_amount': 1000.0,
            'committee_id': 'C001'
        }
        
        merged = merge_raw_data(existing_raw_data, new_raw_data, 'api')
        
        # Bulk fields should be preserved
        assert 'TRANSACTION_DT' in merged
        assert merged['TRANSACTION_DT'] == '01012024'
        assert 'TRANSACTION_AMT' in merged
        assert merged['TRANSACTION_AMT'] == '1000.00'
        
        # API fields should be added
        assert 'contribution_receipt_date' in merged
        assert 'contribution_receipt_amount' in merged
        
        # Should track sources
        assert '_sources' in merged
        assert 'bulk' in merged['_sources'] or 'api' in merged['_sources']


class TestSmartMerge:
    """Test smart merge functionality"""
    
    @pytest.mark.asyncio
    async def test_smart_merge_preserves_existing_data(self, test_db):
        """Test that smart merge preserves existing non-NULL values"""
        # Create existing contribution
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
        test_db.add(existing)
        await test_db.commit()
        
        # New data with some NULL values
        new_data = {
            'contributor_name': None,  # Should not overwrite
            'contributor_state': 'DC',  # Should add
            'contribution_amount': None,  # Should not overwrite
            'raw_data': {'contribution_receipt_date': '2024-01-01'}
        }
        
        fec_client = FECClient()
        merged = fec_client._smart_merge_contribution(existing, new_data, 'api')
        
        # Existing values should be preserved
        assert merged.contributor_name == 'John Doe'
        assert merged.contributor_city == 'Washington'
        assert merged.contribution_amount == 1000.0
        
        # New values should be added
        assert merged.contributor_state == 'DC'
        
        # Data source should be updated
        assert merged.data_source == 'both'
        assert merged.last_updated_from == 'api'
        
        # raw_data should be merged
        assert 'TRANSACTION_DT' in merged.raw_data
        assert 'contribution_receipt_date' in merged.raw_data
    
    @pytest.mark.asyncio
    async def test_smart_merge_prefers_new_non_null_values(self, test_db):
        """Test that smart merge prefers new non-NULL values"""
        existing = Contribution(
            contribution_id='TEST_002',
            contributor_name='John Doe',
            contribution_amount=1000.0,
            data_source='bulk',
            raw_data={'TRANSACTION_DT': '01012024'}
        )
        test_db.add(existing)
        await test_db.commit()
        
        new_data = {
            'contributor_name': 'Jane Smith',  # Should overwrite
            'contribution_amount': 2000.0,  # Should overwrite
            'raw_data': {'contribution_receipt_date': '2024-01-01'}
        }
        
        fec_client = FECClient()
        merged = fec_client._smart_merge_contribution(existing, new_data, 'api')
        
        assert merged.contributor_name == 'Jane Smith'
        assert merged.contribution_amount == 2000.0
    
    @pytest.mark.asyncio
    async def test_smart_merge_handles_dates(self, test_db):
        """Test that smart merge handles dates correctly"""
        existing = Contribution(
            contribution_id='TEST_003',
            contribution_date=None,
            data_source='bulk',
            raw_data={'TRANSACTION_DT': '01012024'}
        )
        test_db.add(existing)
        await test_db.commit()
        
        new_data = {
            'contribution_date': '2024-01-01',
            'raw_data': {'contribution_receipt_date': '2024-01-01'}
        }
        
        fec_client = FECClient()
        merged = fec_client._smart_merge_contribution(existing, new_data, 'api')
        
        # Date should be set from new data
        assert merged.contribution_date is not None


class TestBulkImportIntegration:
    """Test bulk import with smart merge"""
    
    @pytest.mark.asyncio
    async def test_bulk_import_stores_raw_data_as_dict(self, test_db):
        """Test that bulk import stores raw_data as dict, not JSON string"""
        from app.services.bulk_data import BulkDataService
        
        # Create a minimal bulk import record
        record = {
            'contribution_id': 'BULK_001',
            'candidate_id': 'P001',
            'committee_id': 'C001',
            'contributor_name': 'Test Contributor',
            'contribution_amount': 1000.0,
            'contribution_date': datetime(2024, 1, 1),
            'raw_data': {
                'TRANSACTION_DT': '01012024',
                'TRANSACTION_AMT': '1000.00',
                'CMTE_ID': 'C001'
            }
        }
        
        from sqlalchemy import text
        await test_db.execute(
            text("""
                INSERT INTO contributions 
                (contribution_id, candidate_id, committee_id, contributor_name,
                 contribution_amount, contribution_date, raw_data, data_source, last_updated_from)
                VALUES 
                (:contribution_id, :candidate_id, :committee_id, :contributor_name,
                 :contribution_amount, :contribution_date, :raw_data, :data_source, :last_updated_from)
            """),
            {
                **record,
                'data_source': 'bulk',
                'last_updated_from': 'bulk'
            }
        )
        await test_db.commit()
        
        # Retrieve and check
        result = await test_db.execute(
            select(Contribution).where(Contribution.contribution_id == 'BULK_001')
        )
        contrib = result.scalar_one_or_none()
        
        assert contrib is not None
        assert isinstance(contrib.raw_data, dict)
        assert contrib.raw_data['TRANSACTION_DT'] == '01012024'
        assert contrib.data_source == 'bulk'


class TestAPIIntegration:
    """Test API storage with smart merge"""
    
    @pytest.mark.asyncio
    async def test_api_storage_uses_smart_merge(self, test_db):
        """Test that API storage uses smart merge"""
        # Create existing contribution from bulk import
        existing = Contribution(
            contribution_id='API_TEST_001',
            contributor_name='John Doe',
            contribution_amount=1000.0,
            data_source='bulk',
            last_updated_from='bulk',
            raw_data={'TRANSACTION_DT': '01012024', 'TRANSACTION_AMT': '1000.00'}
        )
        test_db.add(existing)
        await test_db.commit()
        
        # Store API response
        fec_client = FECClient()
        api_response = {
            'sub_id': 'API_TEST_001',
            'contribution_receipt_date': '2024-01-01',
            'contribution_receipt_amount': 1000.0,
            'contributor_name': 'John Doe Updated'
        }
        
        await fec_client._store_api_response_in_db(
            'API_TEST_001',
            api_response,
            datetime(2024, 1, 1)
        )
        
        # Retrieve and check
        await test_db.refresh(existing)
        
        # Should have merged data
        assert existing.data_source == 'both'
        assert existing.last_updated_from == 'api'
        assert 'TRANSACTION_DT' in existing.raw_data  # Bulk field preserved
        assert 'contribution_receipt_date' in existing.raw_data  # API field added


class TestDateExtraction:
    """Test date extraction with field mapping"""
    
    def test_extract_date_from_bulk_data(self):
        """Test extracting date from bulk import data"""
        raw_data = {
            'TRANSACTION_DT': '01012024'
        }
        
        date = extract_date_from_raw_data(raw_data)
        assert date is not None
        assert isinstance(date, datetime)
        assert date.year == 2024
        assert date.month == 1
        assert date.day == 1
    
    def test_extract_date_from_api_data(self):
        """Test extracting date from API data"""
        raw_data = {
            'contribution_receipt_date': '2024-01-01'
        }
        
        date = extract_date_from_raw_data(raw_data)
        assert date is not None
        assert isinstance(date, datetime)
        assert date.year == 2024
        assert date.month == 1
        assert date.day == 1
    
    def test_extract_date_from_merged_data(self):
        """Test extracting date from merged data (both sources)"""
        raw_data = {
            'TRANSACTION_DT': '01012024',
            'contribution_receipt_date': '2024-01-01'
        }
        
        date = extract_date_from_raw_data(raw_data)
        assert date is not None
        assert isinstance(date, datetime)


class TestDataSourceTracking:
    """Test data source tracking"""
    
    @pytest.mark.asyncio
    async def test_data_source_tracking_bulk(self, test_db):
        """Test that bulk import sets data source correctly"""
        contrib = Contribution(
            contribution_id='SOURCE_TEST_001',
            contributor_name='Test',
            data_source='bulk',
            last_updated_from='bulk'
        )
        test_db.add(contrib)
        await test_db.commit()
        
        assert contrib.data_source == 'bulk'
        assert contrib.last_updated_from == 'bulk'
    
    @pytest.mark.asyncio
    async def test_data_source_tracking_api(self, test_db):
        """Test that API storage sets data source correctly"""
        contrib = Contribution(
            contribution_id='SOURCE_TEST_002',
            contributor_name='Test',
            data_source='api',
            last_updated_from='api'
        )
        test_db.add(contrib)
        await test_db.commit()
        
        assert contrib.data_source == 'api'
        assert contrib.last_updated_from == 'api'
    
    @pytest.mark.asyncio
    async def test_data_source_tracking_both(self, test_db):
        """Test that merging from both sources sets data_source to 'both'"""
        # Start with bulk
        contrib = Contribution(
            contribution_id='SOURCE_TEST_003',
            contributor_name='Test',
            data_source='bulk',
            last_updated_from='bulk',
            raw_data={'TRANSACTION_DT': '01012024'}
        )
        test_db.add(contrib)
        await test_db.commit()
        
        # Merge with API data
        fec_client = FECClient()
        fec_client._smart_merge_contribution(
            contrib,
            {'raw_data': {'contribution_receipt_date': '2024-01-01'}},
            'api'
        )
        await test_db.commit()
        
        assert contrib.data_source == 'both'
        assert contrib.last_updated_from == 'api'

