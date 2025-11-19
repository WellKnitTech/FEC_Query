"""
Utility functions for mapping FEC API response fields to internal schema

This module provides functions to normalize data from different sources (bulk CSV files,
FEC API responses) into a unified format for consistent processing and storage.
"""
from typing import Dict, Any, Optional, Union
from datetime import datetime, date
import logging

logger = logging.getLogger(__name__)


def map_contribution_fields(api_response: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map FEC API contribution response to internal Contribution schema
    
    Handles multiple field name variations used by the FEC API.
    
    Args:
        api_response: Raw contribution data from FEC API
    
    Returns:
        Dictionary with standardized field names matching Contribution schema
    """
    # Extract amount from multiple possible field names (FEC API uses contb_receipt_amt)
    amount = 0.0
    for amt_key in ['contb_receipt_amt', 'contribution_amount', 'contribution_receipt_amount', 
                    'amount', 'contribution_receipt_amt']:
        amt_val = api_response.get(amt_key)
        if amt_val is not None:
            try:
                amount = float(amt_val)
                if amount > 0:
                    break
            except (ValueError, TypeError):
                continue
    
    # Extract contributor name from multiple possible fields
    contributor_name = (
        api_response.get("contributor_name") or 
        api_response.get("contributor") or 
        api_response.get("name") or 
        api_response.get("contributor_name_1")
    )
    
    # Map common field name variations
    contrib_data = {
        "contribution_id": api_response.get("sub_id") or api_response.get("contribution_id"),
        "candidate_id": api_response.get("candidate_id"),
        "committee_id": api_response.get("committee_id"),
        "contributor_name": contributor_name,
        "contributor_city": api_response.get("contributor_city") or api_response.get("city"),
        "contributor_state": api_response.get("contributor_state") or api_response.get("state"),
        "contributor_zip": (
            api_response.get("contributor_zip") or 
            api_response.get("zip_code") or 
            api_response.get("zip")
        ),
        "contributor_employer": api_response.get("contributor_employer") or api_response.get("employer"),
        "contributor_occupation": api_response.get("contributor_occupation") or api_response.get("occupation"),
        "contribution_amount": amount,
        "contribution_date": (
            api_response.get("contribution_receipt_date") or 
            api_response.get("contribution_date") or 
            api_response.get("receipt_date")
        ),
        "contribution_type": (
            api_response.get("contribution_type") or 
            api_response.get("transaction_type")
        ),
        "receipt_type": api_response.get("receipt_type")
    }
    
    return contrib_data


def map_contribution_for_aggregation(api_response: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Map FEC API contribution response for donor aggregation
    
    Similar to map_contribution_fields but returns None if contributor_name is missing
    (required for aggregation).
    
    Args:
        api_response: Raw contribution data from FEC API
    
    Returns:
        Dictionary with standardized field names, or None if contributor_name is missing
    """
    # Extract contributor name from multiple possible fields
    contributor_name = (
        api_response.get('contributor_name') or 
        api_response.get('contributor') or 
        api_response.get('name') or
        api_response.get('contributor_name_1')
    )
    
    # Only include contributions with a valid name
    if not contributor_name:
        return None
    
    # Extract amount from multiple possible field names
    amount = 0.0
    for amt_key in ['contb_receipt_amt', 'contribution_amount', 'contribution_receipt_amount', 
                    'amount', 'contribution_receipt_amt']:
        amt_val = api_response.get(amt_key)
        if amt_val is not None:
            try:
                amount = float(amt_val)
                # Use the first valid numeric value found, even if it's 0
                break
            except (ValueError, TypeError):
                continue
    
    contrib_dict = {
        'contribution_id': api_response.get('contribution_id') or api_response.get('sub_id'),
        'contributor_name': contributor_name,
        'contributor_city': api_response.get('contributor_city') or api_response.get('city'),
        'contributor_state': api_response.get('contributor_state') or api_response.get('state'),
        'contributor_zip': api_response.get('contributor_zip') or api_response.get('zip'),
        'contributor_employer': api_response.get('contributor_employer') or api_response.get('employer'),
        'contributor_occupation': api_response.get('contributor_occupation') or api_response.get('occupation'),
        'contribution_amount': amount,
        'contribution_date': (
            api_response.get('contribution_date') or 
            api_response.get('contribution_receipt_date')
        )
    }
    
    return contrib_dict


def normalize_from_bulk(bulk_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize bulk import data (CSV format) to unified schema
    
    Maps FEC bulk CSV field names (e.g., SUB_ID, TRANSACTION_DT, TRANSACTION_AMT)
    to internal schema field names (contribution_id, contribution_date, contribution_amount).
    
    Args:
        bulk_data: Raw bulk data dictionary with FEC CSV field names
        
    Returns:
        Dictionary with normalized field names matching internal schema
    """
    normalized = {}
    
    # Map contribution ID
    normalized['contribution_id'] = bulk_data.get('SUB_ID') or bulk_data.get('contribution_id')
    
    # Map dates - bulk data uses TRANSACTION_DT in MMDDYYYY format
    normalized['contribution_date'] = bulk_data.get('TRANSACTION_DT') or bulk_data.get('contribution_date')
    
    # Map amounts - bulk data uses TRANSACTION_AMT
    normalized['contribution_amount'] = bulk_data.get('TRANSACTION_AMT') or bulk_data.get('contribution_amount')
    
    # Map committee and candidate IDs
    normalized['committee_id'] = bulk_data.get('CMTE_ID') or bulk_data.get('committee_id')
    normalized['candidate_id'] = bulk_data.get('CAND_ID') or bulk_data.get('candidate_id')
    
    # Map contributor information
    normalized['contributor_name'] = bulk_data.get('NAME') or bulk_data.get('contributor_name')
    normalized['contributor_city'] = bulk_data.get('CITY') or bulk_data.get('contributor_city')
    normalized['contributor_state'] = bulk_data.get('STATE') or bulk_data.get('contributor_state')
    normalized['contributor_zip'] = bulk_data.get('ZIP_CODE') or bulk_data.get('ZIP') or bulk_data.get('contributor_zip')
    normalized['contributor_employer'] = bulk_data.get('EMPLOYER') or bulk_data.get('contributor_employer')
    normalized['contributor_occupation'] = bulk_data.get('OCCUPATION') or bulk_data.get('contributor_occupation')
    
    # Map transaction type
    normalized['contribution_type'] = bulk_data.get('TRAN_TP') or bulk_data.get('contribution_type')
    
    # Map other fields
    normalized['amendment_indicator'] = bulk_data.get('AMNDT_IND') or bulk_data.get('amendment_indicator')
    normalized['report_type'] = bulk_data.get('RPT_TP') or bulk_data.get('report_type')
    normalized['transaction_id'] = bulk_data.get('TRAN_ID') or bulk_data.get('transaction_id')
    normalized['other_id'] = bulk_data.get('OTHER_ID') or bulk_data.get('other_id')
    normalized['file_number'] = bulk_data.get('FILE_NUM') or bulk_data.get('file_number')
    normalized['memo_code'] = bulk_data.get('MEMO_CD') or bulk_data.get('memo_code')
    
    return normalized


def normalize_from_api(api_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize FEC API response data to unified schema
    
    Maps FEC API field names (e.g., sub_id, contribution_receipt_date, contribution_receipt_amount)
    to internal schema field names (contribution_id, contribution_date, contribution_amount).
    
    Args:
        api_data: Raw API response data dictionary
        
    Returns:
        Dictionary with normalized field names matching internal schema
    """
    normalized = {}
    
    # Map contribution ID
    normalized['contribution_id'] = api_data.get('sub_id') or api_data.get('contribution_id')
    
    # Map dates - API uses contribution_receipt_date
    normalized['contribution_date'] = (
        api_data.get('contribution_receipt_date') or 
        api_data.get('contribution_date') or 
        api_data.get('receipt_date')
    )
    
    # Map amounts - API uses contribution_receipt_amount
    normalized['contribution_amount'] = (
        api_data.get('contribution_receipt_amount') or 
        api_data.get('contribution_amount') or 
        api_data.get('contb_receipt_amt')
    )
    
    # Map committee and candidate IDs
    normalized['committee_id'] = api_data.get('committee_id')
    normalized['candidate_id'] = api_data.get('candidate_id')
    
    # Map contributor information
    normalized['contributor_name'] = (
        api_data.get('contributor_name') or 
        api_data.get('contributor') or 
        api_data.get('name')
    )
    normalized['contributor_city'] = api_data.get('contributor_city') or api_data.get('city')
    normalized['contributor_state'] = api_data.get('contributor_state') or api_data.get('state')
    normalized['contributor_zip'] = (
        api_data.get('contributor_zip') or 
        api_data.get('zip_code') or 
        api_data.get('zip')
    )
    normalized['contributor_employer'] = api_data.get('contributor_employer') or api_data.get('employer')
    normalized['contributor_occupation'] = api_data.get('contributor_occupation') or api_data.get('occupation')
    
    # Map transaction type
    normalized['contribution_type'] = (
        api_data.get('contribution_type') or 
        api_data.get('transaction_type')
    )
    
    # Map other fields
    normalized['amendment_indicator'] = api_data.get('amendment_indicator')
    normalized['report_type'] = api_data.get('report_type')
    normalized['transaction_id'] = api_data.get('transaction_id')
    normalized['other_id'] = api_data.get('other_id')
    normalized['file_number'] = api_data.get('file_number')
    normalized['memo_code'] = api_data.get('memo_code')
    
    return normalized


def extract_unified_field(data: Dict[str, Any], field_name: str, source: str) -> Optional[Any]:
    """
    Extract a unified field from data based on source type
    
    Args:
        data: Data dictionary (bulk or API format)
        field_name: Unified field name (e.g., 'contribution_date', 'contribution_amount')
        source: Source type ('bulk' or 'api')
        
    Returns:
        Field value or None if not found
    """
    if source == 'bulk':
        # Map unified field names to bulk CSV field names
        field_mapping = {
            'contribution_id': 'SUB_ID',
            'contribution_date': 'TRANSACTION_DT',
            'contribution_amount': 'TRANSACTION_AMT',
            'committee_id': 'CMTE_ID',
            'candidate_id': 'CAND_ID',
            'contributor_name': 'NAME',
            'contributor_city': 'CITY',
            'contributor_state': 'STATE',
            'contributor_zip': 'ZIP_CODE',
            'contributor_employer': 'EMPLOYER',
            'contributor_occupation': 'OCCUPATION',
        }
        bulk_field = field_mapping.get(field_name, field_name)
        return data.get(bulk_field) or data.get(field_name)
    else:
        # For API, use the unified field name directly or common variations
        return data.get(field_name)


def get_date_field(data: Dict[str, Any], source: str) -> Optional[str]:
    """
    Get contribution date field from data based on source
    
    Args:
        data: Data dictionary
        source: Source type ('bulk' or 'api')
        
    Returns:
        Date value as string or None
    """
    return extract_unified_field(data, 'contribution_date', source)


def get_amount_field(data: Dict[str, Any], source: str) -> Optional[Union[str, float]]:
    """
    Get contribution amount field from data based on source
    
    Args:
        data: Data dictionary
        source: Source type ('bulk' or 'api')
        
    Returns:
        Amount value (string for bulk, float for API) or None
    """
    return extract_unified_field(data, 'contribution_amount', source)


def merge_raw_data(existing_raw: Optional[Dict[str, Any]], new_raw: Dict[str, Any], source: str) -> Dict[str, Any]:
    """
    Intelligently merge raw_data dictionaries, preserving bulk fields and adding API fields
    
    Args:
        existing_raw: Existing raw_data dictionary (may be None)
        new_raw: New raw_data dictionary
        source: Source of new data ('bulk' or 'api')
        
    Returns:
        Merged raw_data dictionary
    """
    if existing_raw is None:
        return new_raw.copy()
    
    merged = existing_raw.copy()
    
    if source == 'bulk':
        # For bulk data, preserve existing API fields, update/add bulk fields
        # Bulk fields take precedence for bulk updates
        merged.update(new_raw)
    else:
        # For API data, preserve existing bulk fields, add/update API fields
        # Only add API-specific fields, don't overwrite bulk fields
        for key, value in new_raw.items():
            if key not in merged or merged[key] is None:
                merged[key] = value
    
    return merged
