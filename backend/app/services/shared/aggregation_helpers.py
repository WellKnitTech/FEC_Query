"""Reusable aggregation helper functions"""
import pandas as pd
from typing import List, Dict, Any, Optional
from sqlalchemy.sql import Select
from sqlalchemy import func, select


def calculate_distribution_bins(
    amounts: List[float],
    bins: Optional[List[float]] = None,
    labels: Optional[List[str]] = None
) -> Dict[str, int]:
    """
    Calculate contribution distribution using bins.
    
    Args:
        amounts: List of contribution amounts
        bins: Bin edges (default: standard FEC contribution bins)
        labels: Bin labels (default: standard FEC contribution labels)
        
    Returns:
        Dictionary mapping bin labels to counts
    """
    if not amounts:
        return {}
    
    if bins is None:
        bins = [0, 50, 100, 200, 500, 1000, 2700, float('inf')]
    if labels is None:
        labels = ['$0-50', '$50-100', '$100-200', '$200-500', '$500-1000', '$1000-2700', '$2700+']
    
    df_amounts = pd.Series(amounts)
    df_amounts_binned = pd.cut(df_amounts, bins=bins, labels=labels, right=False)
    contribution_distribution = df_amounts_binned.value_counts().to_dict()
    return {str(k): int(v) for k, v in contribution_distribution.items()}


def aggregate_by_date_from_rows(rows: List[Any], date_key: str = 'date', amount_key: str = 'amount') -> Dict[str, float]:
    """
    Aggregate amounts by date from query result rows.
    
    Args:
        rows: Query result rows with date and amount attributes
        date_key: Attribute name for date
        amount_key: Attribute name for amount
        
    Returns:
        Dictionary mapping date strings to total amounts
    """
    result = {}
    for row in rows:
        date = getattr(row, date_key, None)
        amount = getattr(row, amount_key, None)
        if date:
            date_str = str(date)
            result[date_str] = result.get(date_str, 0.0) + float(amount or 0.0)
    return result


def aggregate_by_field_from_rows(rows: List[Any], field_key: str, amount_key: str = 'amount') -> Dict[str, float]:
    """
    Aggregate amounts by field from query result rows.
    
    Args:
        rows: Query result rows with field and amount attributes
        field_key: Attribute name for the grouping field
        amount_key: Attribute name for amount
        
    Returns:
        Dictionary mapping field values to total amounts
    """
    result = {}
    for row in rows:
        field_value = getattr(row, field_key, None)
        amount = getattr(row, amount_key, None)
        if field_value:
            result[field_value] = result.get(field_value, 0.0) + float(amount or 0.0)
    return result

