#!/usr/bin/env python3
"""
Field mappings for bulk data imports.

This module defines expected field mappings from source file columns to database fields
for each data type, used for validation during import verification.
"""
from typing import Dict, List, Optional, Callable, Tuple
from app.services.bulk_data_config import DataType
import pandas as pd


class FieldMapping:
    """Represents a field mapping from source to database"""
    
    def __init__(
        self,
        source_field: str,
        db_field: str,
        transform: Optional[Callable] = None,
        required: bool = False
    ):
        """
        Initialize field mapping
        
        Args:
            source_field: Source file column name
            db_field: Database field name
            transform: Optional transformation function
            required: Whether this field is required
        """
        self.source_field = source_field
        self.db_field = db_field
        self.transform = transform
        self.required = required


# Field mappings for each data type
FIELD_MAPPINGS: Dict[DataType, List[FieldMapping]] = {
    DataType.CANDIDATE_MASTER: [
        FieldMapping('CAND_ID', 'candidate_id', required=True),
        FieldMapping('CAND_NAME', 'name'),
        FieldMapping('CAND_OFFICE', 'office'),
        FieldMapping('CAND_OFFICE_ST', 'state'),
        FieldMapping('CAND_OFFICE_DISTRICT', 'district'),
        FieldMapping('PTY_CD', 'party'),
        FieldMapping('CAND_ELECTION_YR', 'election_years', transform=lambda x: [int(x)] if pd.notna(x) else None),
    ],
    
    DataType.COMMITTEE_MASTER: [
        FieldMapping('CMTE_ID', 'committee_id', required=True),
        FieldMapping('CMTE_NM', 'name'),
        FieldMapping('CMTE_TP', 'committee_type'),
        FieldMapping('CMTE_PTY_AFFILIATION', 'party'),
        FieldMapping('CMTE_ST', 'state'),
        FieldMapping('CAND_ID', 'candidate_ids', transform=lambda x: [x] if x and str(x).strip() else []),
    ],
    
    DataType.CANDIDATE_COMMITTEE_LINKAGE: [
        FieldMapping('CAND_ID', 'candidate_id', required=True),
        FieldMapping('CMTE_ID', 'committee_id', required=True),
        # Note: This is stored in Committee.candidate_ids JSON array
    ],
    
    DataType.INDIVIDUAL_CONTRIBUTIONS: [
        FieldMapping('SUB_ID', 'contribution_id', required=True),
        FieldMapping('CMTE_ID', 'committee_id'),
        FieldMapping('CAND_ID', 'candidate_id'),
        FieldMapping('NAME', 'contributor_name'),
        FieldMapping('CITY', 'contributor_city'),
        FieldMapping('STATE', 'contributor_state'),
        FieldMapping('ZIP_CODE', 'contributor_zip'),
        FieldMapping('EMPLOYER', 'contributor_employer'),
        FieldMapping('OCCUPATION', 'contributor_occupation'),
        FieldMapping('TRANSACTION_AMT', 'contribution_amount', transform=lambda x: float(x) if pd.notna(x) else 0.0),
        FieldMapping('TRANSACTION_DT', 'contribution_date'),
        FieldMapping('TRAN_TP', 'contribution_type'),
        FieldMapping('AMNDT_IND', 'amendment_indicator'),
        FieldMapping('RPT_TP', 'report_type'),
        FieldMapping('TRAN_ID', 'transaction_id'),
        FieldMapping('ENTITY_TP_CODE', 'entity_type'),
        FieldMapping('FILE_NUM', 'file_number'),
    ],
    
    DataType.INDEPENDENT_EXPENDITURES: [
        FieldMapping('expenditure_id', 'expenditure_id', required=True),
        FieldMapping('CMTE_ID', 'committee_id'),
        FieldMapping('CAND_ID', 'candidate_id'),
        FieldMapping('CAND_NM', 'candidate_name'),
        FieldMapping('SUPPORT_OPPOSE_IND', 'support_oppose_indicator'),
        FieldMapping('EXPENDITURE_AMOUNT', 'expenditure_amount', transform=lambda x: float(x) if pd.notna(x) else 0.0),
        FieldMapping('EXPENDITURE_DATE', 'expenditure_date'),
        FieldMapping('PAYEE_NM', 'payee_name'),
        FieldMapping('EXPENDITURE_PURPOSE_DESC', 'expenditure_purpose'),
    ],
    
    DataType.OPERATING_EXPENDITURES: [
        FieldMapping('SUB_ID', 'expenditure_id', required=True),
        FieldMapping('CMTE_ID', 'committee_id'),
        FieldMapping('PAYEE_NM', 'payee_name'),
        FieldMapping('EXPENDITURE_AMOUNT', 'expenditure_amount', transform=lambda x: float(x) if pd.notna(x) else 0.0),
        FieldMapping('EXPENDITURE_DATE', 'expenditure_date'),
        FieldMapping('EXPENDITURE_PURPOSE_DESC', 'expenditure_purpose'),
        FieldMapping('AMNDT_IND', 'amendment_indicator'),
        FieldMapping('RPT_TP', 'report_type'),
        FieldMapping('TRAN_ID', 'transaction_id'),
        FieldMapping('CATEGORY', 'category'),
        FieldMapping('ENTITY_TP', 'entity_type'),
    ],
    
    DataType.CANDIDATE_SUMMARY: [
        FieldMapping('candidate_id', 'candidate_id', required=True),
        FieldMapping('candidate_name', 'candidate_name'),
        FieldMapping('office', 'office'),
        FieldMapping('party', 'party'),
        FieldMapping('state', 'state'),
        FieldMapping('district', 'district'),
        FieldMapping('total_receipts', 'total_receipts', transform=lambda x: float(x) if pd.notna(x) else 0.0),
        FieldMapping('total_disbursements', 'total_disbursements', transform=lambda x: float(x) if pd.notna(x) else 0.0),
        FieldMapping('cash_on_hand', 'cash_on_hand', transform=lambda x: float(x) if pd.notna(x) else 0.0),
    ],
    
    DataType.COMMITTEE_SUMMARY: [
        FieldMapping('committee_id', 'committee_id', required=True),
        FieldMapping('committee_name', 'committee_name'),
        FieldMapping('committee_type', 'committee_type'),
        FieldMapping('total_receipts', 'total_receipts', transform=lambda x: float(x) if pd.notna(x) else 0.0),
        FieldMapping('total_disbursements', 'total_disbursements', transform=lambda x: float(x) if pd.notna(x) else 0.0),
        FieldMapping('cash_on_hand', 'cash_on_hand', transform=lambda x: float(x) if pd.notna(x) else 0.0),
    ],
    
    DataType.ELECTIONEERING_COMM: [
        FieldMapping('CMTE_ID', 'committee_id'),
        FieldMapping('CAND_ID', 'candidate_id'),
        FieldMapping('CAND_NM', 'candidate_name'),
        FieldMapping('COMMUNICATION_DATE', 'communication_date'),
        FieldMapping('COMMUNICATION_COST', 'communication_amount', transform=lambda x: float(x) if pd.notna(x) else 0.0),
    ],
    
    DataType.COMMUNICATION_COSTS: [
        FieldMapping('CMTE_ID', 'committee_id'),
        FieldMapping('CAND_ID', 'candidate_id'),
        FieldMapping('CAND_NM', 'candidate_name'),
        FieldMapping('COMMUNICATION_DATE', 'communication_date'),
        FieldMapping('COMMUNICATION_COST', 'communication_amount', transform=lambda x: float(x) if pd.notna(x) else 0.0),
    ],
    
    # Data types stored only in metadata (no specific table)
    DataType.PAC_SUMMARY: [],
    DataType.OTHER_TRANSACTIONS: [],
    DataType.PAS2: [],
}


def get_field_mappings(data_type: DataType) -> List[FieldMapping]:
    """
    Get field mappings for a data type
    
    Args:
        data_type: Data type to get mappings for
    
    Returns:
        List of FieldMapping objects
    """
    return FIELD_MAPPINGS.get(data_type, [])


def get_required_fields(data_type: DataType) -> List[str]:
    """
    Get list of required source fields for a data type
    
    Args:
        data_type: Data type
    
    Returns:
        List of required source field names
    """
    mappings = get_field_mappings(data_type)
    return [m.source_field for m in mappings if m.required]


def get_db_fields(data_type: DataType) -> List[str]:
    """
    Get list of database fields for a data type
    
    Args:
        data_type: Data type
    
    Returns:
        List of database field names
    """
    mappings = get_field_mappings(data_type)
    return [m.db_field for m in mappings]


def validate_field_mapping(
    source_value: any,
    mapping: FieldMapping
) -> Tuple[bool, any, Optional[str]]:
    """
    Validate a field mapping transformation
    
    Args:
        source_value: Source field value
        mapping: Field mapping to validate
    
    Returns:
        Tuple of (is_valid, transformed_value, error_message)
    """
    try:
        if mapping.transform:
            transformed = mapping.transform(source_value)
        else:
            transformed = source_value
        
        # Check required fields
        if mapping.required:
            if pd.isna(source_value) or (isinstance(source_value, str) and not source_value.strip()):
                return False, None, f"Required field {mapping.source_field} is missing or empty"
        
        return True, transformed, None
    except Exception as e:
        return False, None, f"Error transforming {mapping.source_field}: {str(e)}"


if __name__ == "__main__":
    # Print all field mappings
    for data_type, mappings in FIELD_MAPPINGS.items():
        print(f"\n{data_type.value}:")
        print("-" * 80)
        for mapping in mappings:
            required = " (required)" if mapping.required else ""
            transform = " (transformed)" if mapping.transform else ""
            print(f"  {mapping.source_field} -> {mapping.db_field}{required}{transform}")

