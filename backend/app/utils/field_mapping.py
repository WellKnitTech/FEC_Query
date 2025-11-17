"""
Unified field mapping for FEC data sources.

This module provides mappings between bulk import field names and API field names,
and functions to normalize data from either source to a unified format.
"""

from typing import Dict, Any, Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

# Mapping from bulk import field names to unified/canonical field names
BULK_TO_UNIFIED = {
    # Core identifiers
    'SUB_ID': 'contribution_id',
    'CMTE_ID': 'committee_id',
    'CAND_ID': 'candidate_id',
    'TRAN_ID': 'transaction_id',
    
    # Dates
    'TRANSACTION_DT': 'contribution_date',
    
    # Amounts
    'TRANSACTION_AMT': 'contribution_amount',
    
    # Contributor information
    'NAME': 'contributor_name',
    'CITY': 'contributor_city',
    'STATE': 'contributor_state',
    'ZIP_CODE': 'contributor_zip',
    'EMPLOYER': 'contributor_employer',
    'OCCUPATION': 'contributor_occupation',
    
    # FEC metadata
    'AMNDT_IND': 'amendment_indicator',
    'RPT_TP': 'report_type',
    'ENTITY_TP': 'entity_type',
    'OTHER_ID': 'other_id',
    'FILE_NUM': 'file_number',
    'MEMO_CD': 'memo_code',
    'MEMO_TEXT': 'memo_text',
    'TRAN_TP': 'contribution_type',
}

# Mapping from API field names to unified/canonical field names
API_TO_UNIFIED = {
    # Core identifiers
    'sub_id': 'contribution_id',
    'committee_id': 'committee_id',
    'candidate_id': 'candidate_id',
    'transaction_id': 'transaction_id',
    
    # Dates
    'contribution_receipt_date': 'contribution_date',
    'contribution_date': 'contribution_date',
    'receipt_date': 'contribution_date',
    
    # Amounts
    'contribution_receipt_amount': 'contribution_amount',
    'contribution_amount': 'contribution_amount',
    'contb_receipt_amt': 'contribution_amount',
    'amount': 'contribution_amount',
    
    # Contributor information
    'contributor_name': 'contributor_name',
    'contributor': 'contributor_name',
    'name': 'contributor_name',
    'contributor_name_1': 'contributor_name',
    'contributor_city': 'contributor_city',
    'contributor_state': 'contributor_state',
    'contributor_zip': 'contributor_zip',
    'contributor_employer': 'contributor_employer',
    'contributor_occupation': 'contributor_occupation',
    
    # FEC metadata
    'amendment_indicator': 'amendment_indicator',
    'report_type': 'report_type',
    'entity_type': 'entity_type',
    'other_id': 'other_id',
    'file_number': 'file_number',
    'memo_code': 'memo_code',
    'memo_text': 'memo_text',
    'contribution_type': 'contribution_type',
    'receipt_type': 'contribution_type',
}

# Reverse mapping: unified field names to bulk import field names
UNIFIED_TO_BULK = {v: k for k, v in BULK_TO_UNIFIED.items()}

# Reverse mapping: unified field names to API field names (prefer most common)
UNIFIED_TO_API = {
    'contribution_id': 'sub_id',
    'committee_id': 'committee_id',
    'candidate_id': 'candidate_id',
    'transaction_id': 'transaction_id',
    'contribution_date': 'contribution_receipt_date',
    'contribution_amount': 'contribution_receipt_amount',
    'contributor_name': 'contributor_name',
    'contributor_city': 'contributor_city',
    'contributor_state': 'contributor_state',
    'contributor_zip': 'contributor_zip',
    'contributor_employer': 'contributor_employer',
    'contributor_occupation': 'contributor_occupation',
    'amendment_indicator': 'amendment_indicator',
    'report_type': 'report_type',
    'entity_type': 'entity_type',
    'other_id': 'other_id',
    'file_number': 'file_number',
    'memo_code': 'memo_code',
    'memo_text': 'memo_text',
    'contribution_type': 'contribution_type',
}

# Fields that should be preserved from bulk import when merging with API data
BULK_PRESERVE_FIELDS = [
    'TRANSACTION_DT', 'TRANSACTION_AMT', 'CMTE_ID', 'CAND_ID',
    'AMNDT_IND', 'RPT_TP', 'TRAN_ID', 'ENTITY_TP', 'FILE_NUM',
    'MEMO_CD', 'MEMO_TEXT', 'SUB_ID', 'NAME', 'CITY', 'STATE',
    'ZIP_CODE', 'EMPLOYER', 'OCCUPATION', 'TRAN_TP', 'OTHER_ID'
]


def normalize_from_bulk(bulk_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize bulk import data to unified field names.
    
    Args:
        bulk_data: Dictionary with bulk import field names
        
    Returns:
        Dictionary with unified field names
    """
    normalized = {}
    
    for bulk_key, value in bulk_data.items():
        unified_key = BULK_TO_UNIFIED.get(bulk_key, bulk_key)
        normalized[unified_key] = value
    
    return normalized


def normalize_from_api(api_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize API response data to unified field names.
    
    Args:
        api_data: Dictionary with API field names
        
    Returns:
        Dictionary with unified field names
    """
    normalized = {}
    
    for api_key, value in api_data.items():
        unified_key = API_TO_UNIFIED.get(api_key, api_key)
        # If multiple API fields map to same unified key, prefer non-None values
        if unified_key not in normalized or normalized[unified_key] is None:
            normalized[unified_key] = value
    
    return normalized


def extract_unified_field(data: Dict[str, Any], unified_field: str, source: Optional[str] = None) -> Any:
    """
    Extract a unified field from data, checking both bulk and API field names.
    
    Args:
        data: Dictionary with either bulk or API field names
        unified_field: The unified field name to extract
        source: Optional hint about source ('bulk' or 'api')
        
    Returns:
        The field value, or None if not found
    """
    # If source is known, use appropriate mapping
    if source == 'bulk':
        bulk_field = UNIFIED_TO_BULK.get(unified_field)
        if bulk_field and bulk_field in data:
            return data[bulk_field]
    elif source == 'api':
        api_field = UNIFIED_TO_API.get(unified_field)
        if api_field and api_field in data:
            return data[api_field]
        # Also check other API field variants
        for api_key, mapped_unified in API_TO_UNIFIED.items():
            if mapped_unified == unified_field and api_key in data:
                return data[api_key]
    
    # Unknown source or field not found - try both mappings
    # Try bulk import field
    bulk_field = UNIFIED_TO_BULK.get(unified_field)
    if bulk_field and bulk_field in data:
        return data[bulk_field]
    
    # Try API field
    api_field = UNIFIED_TO_API.get(unified_field)
    if api_field and api_field in data:
        return data[api_field]
    
    # Try all API field variants
    for api_key, mapped_unified in API_TO_UNIFIED.items():
        if mapped_unified == unified_field and api_key in data:
            return data[api_key]
    
    # Try direct match
    if unified_field in data:
        return data[unified_field]
    
    return None


def get_date_field(data: Dict[str, Any], source: Optional[str] = None) -> Optional[str]:
    """
    Extract contribution date from data, checking all possible field names.
    
    Args:
        data: Dictionary with either bulk or API field names
        source: Optional hint about source ('bulk' or 'api')
        
    Returns:
        String date value if found, None otherwise (will be parsed by date_utils)
    """
    # Try unified field first
    date_value = extract_unified_field(data, 'contribution_date', source)
    
    if date_value:
        if isinstance(date_value, datetime):
            return date_value.strftime('%Y-%m-%d')
        if isinstance(date_value, str) and date_value.strip() and date_value.lower() != 'none':
            # Will be parsed by date_utils
            return date_value
    
    # Try bulk import field directly
    if 'TRANSACTION_DT' in data:
        trans_dt = data['TRANSACTION_DT']
        # Debug logging
        import logging
        logger = logging.getLogger(__name__)
        logger.debug(f"get_date_field: Found TRANSACTION_DT in data: value={trans_dt}, type={type(trans_dt).__name__ if trans_dt else 'None'}")
        
        # Skip None, empty strings, string "None", and string "nan" (pandas NaN conversion issue)
        if trans_dt is not None:
            trans_dt_str = str(trans_dt).strip().lower()
            # Skip empty, "none", "nan", "null", etc.
            if trans_dt_str and trans_dt_str not in ['none', 'nan', 'null', '']:
                result = str(trans_dt).strip()
                logger.debug(f"get_date_field: Returning TRANSACTION_DT value: '{result}'")
                return result
            else:
                logger.debug(f"get_date_field: TRANSACTION_DT value is None, empty, or invalid string: '{trans_dt}'")
        else:
            logger.debug(f"get_date_field: TRANSACTION_DT value is None")
    
    # Try API fields directly
    for api_field in ['contribution_receipt_date', 'contribution_date', 'receipt_date']:
        if api_field in data and data[api_field]:
            api_date = data[api_field]
            if api_date and str(api_date).strip() and str(api_date).lower() != 'none':
                return str(api_date).strip()
    
    return None


def get_amount_field(data: Dict[str, Any], source: Optional[str] = None) -> Optional[float]:
    """
    Extract contribution amount from data, checking all possible field names.
    
    Args:
        data: Dictionary with either bulk or API field names
        source: Optional hint about source ('bulk' or 'api')
        
    Returns:
        float amount if found, None otherwise
    """
    # Try unified field first
    amount_value = extract_unified_field(data, 'contribution_amount', source)
    
    if amount_value is not None:
        try:
            return float(amount_value)
        except (ValueError, TypeError):
            pass
    
    # Try bulk import field directly
    if 'TRANSACTION_AMT' in data and data['TRANSACTION_AMT']:
        try:
            return float(str(data['TRANSACTION_AMT']).replace('$', '').replace(',', ''))
        except (ValueError, TypeError):
            pass
    
    # Try API fields directly
    for api_field in ['contribution_receipt_amount', 'contribution_amount', 'contb_receipt_amt', 'amount']:
        if api_field in data and data[api_field] is not None:
            try:
                return float(data[api_field])
            except (ValueError, TypeError):
                pass
    
    return None


def merge_raw_data(existing_raw_data: Dict[str, Any], new_raw_data: Dict[str, Any], source: str) -> Dict[str, Any]:
    """
    Intelligently merge raw_data from two sources.
    
    Strategy:
    - Preserve bulk import fields (TRANSACTION_DT, etc.) when merging API data
    - Add new fields from new source
    - Prefer non-None values from new source for overlapping fields
    - Track data source in metadata
    
    Args:
        existing_raw_data: Existing raw_data dictionary
        new_raw_data: New raw_data dictionary to merge
        source: Source of new data ('bulk' or 'api')
        
    Returns:
        Merged raw_data dictionary
    """
    if not existing_raw_data:
        return new_raw_data.copy() if new_raw_data else {}
    
    if not isinstance(existing_raw_data, dict):
        logger.warning(f"merge_raw_data: existing_raw_data is not a dict, type: {type(existing_raw_data)}")
        return new_raw_data.copy() if new_raw_data else {}
    
    if not new_raw_data:
        return existing_raw_data.copy()
    
    # Start with existing data
    merged = existing_raw_data.copy()
    
    # Determine which fields to preserve from existing data
    if source == 'api':
        # When merging API data, preserve bulk import fields
        bulk_fields_to_preserve = {
            k: v for k, v in existing_raw_data.items()
            if k in BULK_PRESERVE_FIELDS and v is not None
        }
    
    # Merge new data (new data takes precedence for non-None values)
    for key, value in new_raw_data.items():
        if value is not None:
            merged[key] = value
        elif key not in merged:
            # Add key even if None, to track that it was checked
            merged[key] = value
    
    # Restore preserved bulk fields if they were overwritten with None
    if source == 'api':
        for key, value in bulk_fields_to_preserve.items():
            if key not in merged or merged[key] is None:
                merged[key] = value
    
    # Track data sources in metadata
    if '_sources' not in merged:
        merged['_sources'] = []
    
    if source not in merged['_sources']:
        merged['_sources'].append(source)
    
    return merged

