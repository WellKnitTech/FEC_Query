"""
Centralized date utility functions for consistent date serialization across the backend.
"""
from datetime import datetime, date
from typing import Optional, Union, Dict, Any
import logging
import re

logger = logging.getLogger(__name__)


def serialize_date(date_value: Optional[Union[datetime, date, str]]) -> Optional[str]:
    """
    Serialize a date or datetime object to YYYY-MM-DD format string.
    
    Args:
        date_value: A datetime, date, or string value to serialize
        
    Returns:
        A string in YYYY-MM-DD format, or None if the input is None/invalid
    """
    if date_value is None:
        logger.debug("serialize_date: date_value is None")
        return None
    
    logger.debug(f"serialize_date: Processing date_value: {date_value} (type: {type(date_value).__name__})")
    
    try:
        # If it's already a string, validate and return first 10 chars (YYYY-MM-DD)
        if isinstance(date_value, str):
            # If it's an ISO format string with time, extract just the date part
            if 'T' in date_value:
                dt = datetime.fromisoformat(date_value.replace('Z', '+00:00'))
                return dt.strftime('%Y-%m-%d')
            # If it's already YYYY-MM-DD format, return as-is
            elif len(date_value) >= 10 and date_value.count('-') >= 2:
                return date_value[:10]
            # Try to parse and reformat
            else:
                dt = datetime.strptime(date_value[:10], '%Y-%m-%d')
                return dt.strftime('%Y-%m-%d')
        
        # If it's a datetime object, format as date-only
        elif isinstance(date_value, datetime):
            return date_value.strftime('%Y-%m-%d')
        
        # If it's a date object, format it
        elif isinstance(date_value, date):
            return date_value.strftime('%Y-%m-%d')
        
        # If it has strftime method, use it
        elif hasattr(date_value, 'strftime'):
            return date_value.strftime('%Y-%m-%d')
        
    except (ValueError, TypeError, AttributeError):
        pass
    
    return None


def serialize_datetime(datetime_value: Optional[Union[datetime, str]]) -> Optional[str]:
    """
    Serialize a datetime object to ISO 8601 format string with timezone.
    
    Args:
        datetime_value: A datetime object or ISO string to serialize
        
    Returns:
        An ISO 8601 format string, or None if the input is None/invalid
    """
    if datetime_value is None:
        return None
    
    try:
        # If it's already a string, validate and return
        if isinstance(datetime_value, str):
            # Try to parse and re-serialize to ensure consistency
            if 'T' in datetime_value:
                dt = datetime.fromisoformat(datetime_value.replace('Z', '+00:00'))
                return dt.isoformat()
            # If it's date-only, convert to datetime at midnight
            elif len(datetime_value) >= 10:
                dt = datetime.strptime(datetime_value[:10], '%Y-%m-%d')
                return dt.isoformat()
            return datetime_value
        
        # If it's a datetime object, use isoformat
        elif isinstance(datetime_value, datetime):
            return datetime_value.isoformat()
        
        # If it's a date object, convert to datetime at midnight
        elif isinstance(datetime_value, date):
            dt = datetime.combine(datetime_value, datetime.min.time())
            return dt.isoformat()
        
    except (ValueError, TypeError, AttributeError):
        pass
    
    return None


def extract_date_from_raw_data(raw_data: Optional[Dict[str, Any]]) -> Optional[datetime]:
    """
    Extract and parse a date from raw_data dictionary, handling various formats and field names.
    
    This function uses the unified field mapping to check both bulk import and API field names:
    - TRANSACTION_DT (bulk, often in MMDDYYYY format - 8 digits)
    - contribution_receipt_date (API)
    - contribution_date (unified)
    - receipt_date (API)
    
    Args:
        raw_data: Dictionary containing raw contribution data
        
    Returns:
        A datetime object if a valid date is found, None otherwise
    """
    if not raw_data or not isinstance(raw_data, dict):
        logger.warning(f"extract_date_from_raw_data: raw_data is None or not a dict. Type: {type(raw_data)}")
        return None
    
    # Use field mapping to extract date (preferred method)
    from app.utils.field_mapping import get_date_field
    
    # Try to determine source from raw_data structure
    source = None
    if 'TRANSACTION_DT' in raw_data:
        source = 'bulk'
    elif 'contribution_receipt_date' in raw_data or 'sub_id' in raw_data:
        source = 'api'
    
    # Use unified field mapping to get date
    date_value = get_date_field(raw_data, source)
    
    if date_value:
        # get_date_field now returns a string (or None)
        if isinstance(date_value, datetime):
            logger.debug(f"extract_date_from_raw_data: get_date_field returned datetime: {date_value}")
            return date_value
        elif isinstance(date_value, str):
            date_str = date_value.strip()
            # Skip if it's "None" or empty
            if not date_str or date_str.lower() == 'none':
                logger.debug(f"extract_date_from_raw_data: get_date_field returned empty or 'None' string: '{date_str}'")
                date_str = None
            else:
                logger.debug(f"extract_date_from_raw_data: get_date_field returned string: '{date_str}'")
        else:
            # Not a string or datetime, try fallback
            date_str = str(date_value).strip() if date_value else None
            if date_str and date_str.lower() == 'none':
                logger.debug(f"extract_date_from_raw_data: get_date_field returned non-string that converted to 'None': {date_value}")
                date_str = None
            else:
                logger.debug(f"extract_date_from_raw_data: get_date_field returned non-string: {date_value} (type: {type(date_value).__name__})")
    else:
        # Field mapping didn't find a date, try fallback to original logic
        logger.debug(f"extract_date_from_raw_data: get_date_field returned None. Source={source}, TRANSACTION_DT in raw_data={'TRANSACTION_DT' in raw_data if raw_data else False}")
        date_str = None
    
    # Fallback: original field checking logic (for edge cases)
    # Log available keys for debugging
    available_keys = list(raw_data.keys())
    logger.debug(f"extract_date_from_raw_data: Available keys in raw_data (first 30): {available_keys[:30]}")
    
    # List of date field names to check (for logging and fallback)
    priority_date_fields = [
        'TRANSACTION_DT',
        'contribution_receipt_date',
        'contribution_date',
        'receipt_date',
        'TRANSACTION_DATE',
        'DATE',
        'transaction_dt',
        'transaction_date',
        'date'
    ]
    
    # More precise pattern matching for date fields - only match fields that end with or contain specific date-related terms
    # This avoids false positives like "candidate_id" or "transaction_id"
    date_field_patterns = [
        r'_date$',      # ends with _date
        r'_dt$',        # ends with _dt
        r'^date',        # starts with date
        r'^DATE',        # starts with DATE
        r'_DATE$',      # ends with _DATE
        r'load_date',    # specific field
        r'transaction_dt',  # specific field (case insensitive)
        r'transaction_date', # specific field (case insensitive)
    ]
    
    additional_date_fields = []
    for key in available_keys:
        key_lower = key.lower()
        # Check if it matches any date pattern
        if any(re.search(pattern, key_lower) for pattern in date_field_patterns):
            # But exclude fields that are clearly not dates
            if not any(exclude in key_lower for exclude in ['_id', '_name', '_type', '_desc', '_full', '_code', '_text', '_label', '_period']):
                additional_date_fields.append(key)
    
    # Log date fields found
    logger.debug(f"extract_date_from_raw_data: Priority date fields found: {[f for f in priority_date_fields if f in available_keys]}")
    logger.debug(f"extract_date_from_raw_data: Additional date fields found: {additional_date_fields[:10]}")
    
    # Log sample values for key date fields
    keys_to_log = []
    for field in priority_date_fields[:3]:  # Log first 3 priority fields
        if field in available_keys:
            keys_to_log.append(field)
    for field in additional_date_fields[:2]:  # Log first 2 additional fields
        if field not in keys_to_log:
            keys_to_log.append(field)
    
    for key in keys_to_log:
        value = raw_data.get(key)
        logger.debug(f"extract_date_from_raw_data: Key '{key}' = {repr(value)} (type: {type(value).__name__})")
    
    # Combine: priority fields first, then additional date fields (excluding load_date as it's usually not the contribution date)
    # Always check all priority fields (even if not in available_keys) to handle cases where
    # bulk import data (TRANSACTION_DT) might exist but wasn't detected in initial scan
    fields_to_check = []
    for field in priority_date_fields:
        fields_to_check.append(field)  # Check all priority fields
    for field in additional_date_fields:
        if field not in fields_to_check and field != 'load_date':  # Skip load_date unless it's in priority list
            fields_to_check.append(field)
    
    logger.debug(f"extract_date_from_raw_data: Will check these date fields (in order): {fields_to_check[:10]}")
    
    # If we got a date_str from field mapping, parse it directly
    if date_str:
        try:
            # MMDDYYYY format (8 digits)
            if len(date_str) == 8 and date_str.isdigit():
                try:
                    parsed = datetime.strptime(date_str, '%m%d%Y')
                    logger.debug(f"extract_date_from_raw_data: Successfully parsed MMDDYYYY format: {parsed}")
                    return parsed
                except ValueError as e:
                    logger.debug(f"extract_date_from_raw_data: Failed to parse MMDDYYYY format '{date_str}': {e}")
            
            # YYYY-MM-DD format (10 characters)
            elif len(date_str) >= 10 and date_str.count('-') >= 2:
                try:
                    parsed = datetime.strptime(date_str[:10], '%Y-%m-%d')
                    logger.debug(f"extract_date_from_raw_data: Successfully parsed YYYY-MM-DD format: {parsed}")
                    return parsed
                except ValueError as e:
                    logger.debug(f"extract_date_from_raw_data: Failed to parse YYYY-MM-DD format '{date_str}': {e}")
            
            # ISO format with time
            elif 'T' in date_str:
                try:
                    parsed = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                    logger.debug(f"extract_date_from_raw_data: Successfully parsed ISO format: {parsed}")
                    return parsed
                except ValueError as e:
                    logger.debug(f"extract_date_from_raw_data: Failed to parse ISO format '{date_str}': {e}")
            
            # Try parsing as ISO format
            else:
                try:
                    parsed = datetime.fromisoformat(date_str)
                    logger.debug(f"extract_date_from_raw_data: Successfully parsed as ISO: {parsed}")
                    return parsed
                except ValueError as e:
                    logger.debug(f"extract_date_from_raw_data: Failed to parse as ISO '{date_str}': {e}")
        except Exception as e:
            logger.warning(f"extract_date_from_raw_data: Error parsing date_str '{date_str}': {e}")
    
    # Fallback: check fields directly (original logic)
    for field_name in fields_to_check:
        # Only check fields that actually exist in raw_data
        if field_name not in raw_data:
            continue
            
        date_value = raw_data[field_name]  # Use direct access to distinguish None value from missing key
        
        # Skip falsy values (log at debug level)
        if date_value is None:
            logger.debug(f"extract_date_from_raw_data: Field '{field_name}' exists but value is None")
            continue
        elif date_value == "":
            logger.debug(f"extract_date_from_raw_data: Field '{field_name}' exists but value is empty string")
            continue
        elif not date_value:
            logger.debug(f"extract_date_from_raw_data: Field '{field_name}' exists but value is falsy: {date_value} (type: {type(date_value).__name__})")
            continue
        
        logger.debug(f"extract_date_from_raw_data: Found date field '{field_name}' with value: {date_value} (type: {type(date_value).__name__})")
        
        try:
            # Handle MMDDYYYY format (8 digits) - common in FEC bulk data
            if isinstance(date_value, str):
                date_str = str(date_value).strip()
                
                # Skip obviously non-date values (common data quality issues)
                non_date_patterns = ['NOT EMPLOYED', 'N/A', 'NA', 'NULL', 'NONE', 'UNKNOWN', 'RETIRED', 'SELF', 
                                    'EMPLOYED', 'UNEMPLOYED', 'HOMEMAKER', 'STUDENT']
                if date_str.upper() in [p.upper() for p in non_date_patterns]:
                    logger.debug(f"extract_date_from_raw_data: Skipping non-date value '{date_str}' in field '{field_name}'")
                    continue
                
                logger.debug(f"extract_date_from_raw_data: Processing string date '{date_str}' (length: {len(date_str)})")
                
                # MMDDYYYY format (8 digits)
                if len(date_str) == 8 and date_str.isdigit():
                    try:
                        parsed = datetime.strptime(date_str, '%m%d%Y')
                        logger.debug(f"extract_date_from_raw_data: Successfully parsed MMDDYYYY format: {parsed}")
                        return parsed
                    except ValueError as e:
                        logger.debug(f"extract_date_from_raw_data: Failed to parse MMDDYYYY format '{date_str}': {e}")
                        continue
                
                # YYYY-MM-DD format (10 characters)
                elif len(date_str) >= 10 and date_str.count('-') >= 2:
                    try:
                        parsed = datetime.strptime(date_str[:10], '%Y-%m-%d')
                        logger.debug(f"extract_date_from_raw_data: Successfully parsed YYYY-MM-DD format: {parsed}")
                        return parsed
                    except ValueError as e:
                        logger.debug(f"extract_date_from_raw_data: Failed to parse YYYY-MM-DD format '{date_str}': {e}")
                        continue
                
                # ISO format with time
                elif 'T' in date_str:
                    try:
                        parsed = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                        logger.debug(f"extract_date_from_raw_data: Successfully parsed ISO format: {parsed}")
                        return parsed
                    except ValueError as e:
                        logger.debug(f"extract_date_from_raw_data: Failed to parse ISO format '{date_str}': {e}")
                        continue
                
                # Try parsing as ISO format
                else:
                    try:
                        parsed = datetime.fromisoformat(date_str)
                        logger.debug(f"extract_date_from_raw_data: Successfully parsed as ISO: {parsed}")
                        return parsed
                    except ValueError as e:
                        logger.debug(f"extract_date_from_raw_data: Failed to parse as ISO '{date_str}': {e}")
                        continue
            
            # If it's already a datetime object
            elif isinstance(date_value, datetime):
                logger.warning(f"extract_date_from_raw_data: Date value is already datetime: {date_value}")
                return date_value
            
            # If it's a date object, convert to datetime
            elif isinstance(date_value, date):
                parsed = datetime.combine(date_value, datetime.min.time())
                logger.warning(f"extract_date_from_raw_data: Converted date to datetime: {parsed}")
                return parsed
            
            # If it's a number (timestamp), try to parse it
            elif isinstance(date_value, (int, float)):
                try:
                    # Try as Unix timestamp (seconds since epoch)
                    if date_value > 1000000000:  # Likely a Unix timestamp
                        parsed = datetime.fromtimestamp(date_value)
                        logger.warning(f"extract_date_from_raw_data: Parsed numeric timestamp: {parsed}")
                        return parsed
                    # Try as YYYYMMDD integer
                    elif 19000101 <= date_value <= 99991231:
                        date_str = str(int(date_value))
                        if len(date_str) == 8:
                            parsed = datetime.strptime(date_str, '%Y%m%d')
                            logger.warning(f"extract_date_from_raw_data: Parsed YYYYMMDD integer: {parsed}")
                            return parsed
                except (ValueError, OSError, OverflowError) as e:
                    logger.warning(f"extract_date_from_raw_data: Failed to parse numeric value {date_value}: {e}")
                    continue
            
            # If it's not a string, datetime, date, or number, log what it is
            else:
                logger.warning(f"extract_date_from_raw_data: Field '{field_name}' has unexpected type: {type(date_value).__name__}, value: {repr(date_value)}")
                continue
            
        except (ValueError, TypeError, AttributeError) as e:
            logger.warning(f"extract_date_from_raw_data: Exception processing field '{field_name}': {e}")
            continue
    
    logger.debug(f"extract_date_from_raw_data: No valid date found in raw_data. Checked {len(fields_to_check)} fields")
    return None


def extract_and_serialize_date_from_raw_data(raw_data: Optional[Dict[str, Any]]) -> Optional[str]:
    """
    Extract a date from raw_data and serialize it to YYYY-MM-DD format.
    
    This is a convenience function that combines extract_date_from_raw_data and serialize_date.
    
    Args:
        raw_data: Dictionary containing raw contribution data
        
    Returns:
        A string in YYYY-MM-DD format, or None if no valid date is found
    """
    date_obj = extract_date_from_raw_data(raw_data)
    return serialize_date(date_obj)


def cycle_to_date_range(cycle: int) -> Dict[str, str]:
    """
    Convert an FEC election cycle to a date range.
    
    FEC cycles: For cycle YYYY, the cycle includes contributions from (YYYY-1)-01-01 to YYYY-12-31
    Example: Cycle 2026 includes contributions from 2025-01-01 through 2026-12-31
    
    Args:
        cycle: Election cycle year (e.g., 2026)
        
    Returns:
        Dictionary with 'min_date' and 'max_date' in YYYY-MM-DD format
    """
    cycle_year = cycle
    min_date = f"{cycle_year - 1}-01-01"
    max_date = f"{cycle_year}-12-31"
    return {"min_date": min_date, "max_date": max_date}

