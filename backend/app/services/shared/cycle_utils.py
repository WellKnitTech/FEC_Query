"""Utilities for FEC cycle conversion and date handling"""
import logging
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


def convert_cycle_to_date_range(cycle: int) -> Tuple[str, str]:
    """
    Convert FEC cycle to date range.
    
    FEC cycles: For cycle YYYY, the cycle includes contributions from (YYYY-1)-01-01 to YYYY-12-31
    Example: Cycle 2026 includes contributions from 2025-01-01 through 2026-12-31
    
    Args:
        cycle: Election cycle year
        
    Returns:
        Tuple of (min_date, max_date) as strings in YYYY-MM-DD format
    """
    cycle_year = cycle
    min_date = f"{cycle_year - 1}-01-01"
    max_date = f"{cycle_year}-12-31"
    logger.debug(f"Converted cycle {cycle} to date range: {min_date} to {max_date}")
    return min_date, max_date


def should_convert_cycle(cycle: Optional[int], min_date: Optional[str], max_date: Optional[str]) -> bool:
    """
    Determine if cycle should be converted to date range.
    
    Args:
        cycle: Election cycle year
        min_date: Optional start date
        max_date: Optional end date
        
    Returns:
        True if cycle should be converted (cycle is provided and no explicit dates given)
    """
    return cycle is not None and not min_date and not max_date

