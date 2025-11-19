#!/usr/bin/env python3
"""
Validate data integrity by sampling records from source files and comparing with database.

This module samples records from source files, validates field mappings, checks data
transformations, and verifies records exist in the database.
"""
import logging
import random
from typing import Dict, List, Optional, Tuple, Any
from pathlib import Path
import pandas as pd
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.database import (
    AsyncSessionLocal,
    Candidate,
    Committee,
    Contribution,
    IndependentExpenditure,
    OperatingExpenditure,
    CandidateSummary,
    CommitteeSummary,
    ElectioneeringComm,
    CommunicationCost,
)
from app.services.bulk_data_config import DataType
from app.utils.thread_pool import async_read_csv
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from verify_field_mappings import get_field_mappings, validate_field_mapping, FieldMapping
from verify_bulk_file_counts import BulkFileCounter

logger = logging.getLogger(__name__)


class DataIntegrityValidator:
    """Validate data integrity by sampling and comparing records"""
    
    def __init__(self, bulk_data_dir: Optional[str] = None):
        """
        Initialize data integrity validator
        
        Args:
            bulk_data_dir: Directory containing bulk data files
        """
        self.file_counter = BulkFileCounter(bulk_data_dir)
    
    async def sample_file_records(
        self,
        file_path: Path,
        data_type: DataType,
        sample_size: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Sample records from a source file
        
        Args:
            file_path: Path to source file
            data_type: Data type
            sample_size: Number of records to sample
        
        Returns:
            List of sampled records as dictionaries
        """
        separator, has_header = self.file_counter._get_file_format_info(data_type)
        
        try:
            # Read entire file (for small files) or sample
            # For large files, we'll read a chunk and sample from it
            df = await async_read_csv(
                str(file_path),
                sep=separator,
                header=0 if has_header else None,
                dtype=str,
                low_memory=False,
                on_bad_lines='skip',
                encoding='utf-8',
                encoding_errors='replace'
            )
            
            # If file is small, read all and sample
            if len(df) <= sample_size * 2:
                records = df.to_dict('records')
                return random.sample(records, min(len(records), sample_size))
            
            # For large files, sample random rows
            sample_indices = random.sample(range(len(df)), sample_size)
            sampled_df = df.iloc[sample_indices]
            return sampled_df.to_dict('records')
            
        except Exception as e:
            logger.error(f"Error sampling records from {file_path}: {e}", exc_info=True)
            return []
    
    async def validate_field_mappings(
        self,
        source_records: List[Dict[str, Any]],
        data_type: DataType
    ) -> Dict[str, Any]:
        """
        Validate field mappings for sampled records
        
        Args:
            source_records: List of source records
            data_type: Data type
        
        Returns:
            Dictionary with validation results
        """
        mappings = get_field_mappings(data_type)
        if not mappings:
            return {
                'valid': True,
                'errors': [],
                'warnings': ['No field mappings defined for this data type']
            }
        
        errors = []
        warnings = []
        validated_count = 0
        
        for record in source_records:
            for mapping in mappings:
                source_value = record.get(mapping.source_field)
                is_valid, transformed, error = validate_field_mapping(source_value, mapping)
                
                if not is_valid:
                    errors.append({
                        'record': record.get('SUB_ID') or record.get('CAND_ID') or record.get('CMTE_ID') or 'unknown',
                        'field': mapping.source_field,
                        'error': error
                    })
                elif error:
                    warnings.append({
                        'record': record.get('SUB_ID') or record.get('CAND_ID') or record.get('CMTE_ID') or 'unknown',
                        'field': mapping.source_field,
                        'warning': error
                    })
                else:
                    validated_count += 1
        
        return {
            'valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings,
            'validated_fields': validated_count,
            'total_fields': len(source_records) * len(mappings)
        }
    
    async def verify_record_in_database(
        self,
        session: AsyncSession,
        source_record: Dict[str, Any],
        data_type: DataType
    ) -> Tuple[bool, Optional[str], Optional[Dict[str, Any]]]:
        """
        Verify a source record exists in the database
        
        Args:
            session: Database session
            source_record: Source record dictionary
            data_type: Data type
        
        Returns:
            Tuple of (exists, error_message, db_record)
        """
        try:
            if data_type == DataType.CANDIDATE_MASTER:
                candidate_id = source_record.get('CAND_ID')
                if not candidate_id:
                    return False, "Missing CAND_ID", None
                
                result = await session.execute(
                    select(Candidate).where(Candidate.candidate_id == str(candidate_id).strip())
                )
                db_record = result.scalar_one_or_none()
                if db_record:
                    return True, None, {
                        'candidate_id': db_record.candidate_id,
                        'name': db_record.name,
                        'office': db_record.office,
                        'state': db_record.state
                    }
                return False, f"Candidate {candidate_id} not found in database", None
            
            elif data_type == DataType.COMMITTEE_MASTER:
                committee_id = source_record.get('CMTE_ID')
                if not committee_id:
                    return False, "Missing CMTE_ID", None
                
                result = await session.execute(
                    select(Committee).where(Committee.committee_id == str(committee_id).strip())
                )
                db_record = result.scalar_one_or_none()
                if db_record:
                    return True, None, {
                        'committee_id': db_record.committee_id,
                        'name': db_record.name,
                        'committee_type': db_record.committee_type
                    }
                return False, f"Committee {committee_id} not found in database", None
            
            elif data_type == DataType.INDIVIDUAL_CONTRIBUTIONS:
                contribution_id = source_record.get('SUB_ID')
                if not contribution_id:
                    return False, "Missing SUB_ID", None
                
                result = await session.execute(
                    select(Contribution).where(Contribution.contribution_id == str(contribution_id).strip())
                )
                db_record = result.scalar_one_or_none()
                if db_record:
                    return True, None, {
                        'contribution_id': db_record.contribution_id,
                        'committee_id': db_record.committee_id,
                        'candidate_id': db_record.candidate_id,
                        'amount': db_record.contribution_amount
                    }
                return False, f"Contribution {contribution_id} not found in database", None
            
            elif data_type == DataType.INDEPENDENT_EXPENDITURES:
                exp_id = source_record.get('expenditure_id') or source_record.get('SUB_ID')
                if not exp_id:
                    return False, "Missing expenditure_id", None
                
                result = await session.execute(
                    select(IndependentExpenditure).where(IndependentExpenditure.expenditure_id == str(exp_id).strip())
                )
                db_record = result.scalar_one_or_none()
                if db_record:
                    return True, None, {
                        'expenditure_id': db_record.expenditure_id,
                        'committee_id': db_record.committee_id,
                        'amount': db_record.expenditure_amount
                    }
                return False, f"Independent expenditure {exp_id} not found in database", None
            
            elif data_type == DataType.OPERATING_EXPENDITURES:
                exp_id = source_record.get('SUB_ID')
                if not exp_id:
                    return False, "Missing SUB_ID", None
                
                result = await session.execute(
                    select(OperatingExpenditure).where(OperatingExpenditure.expenditure_id == str(exp_id).strip())
                )
                db_record = result.scalar_one_or_none()
                if db_record:
                    return True, None, {
                        'expenditure_id': db_record.expenditure_id,
                        'committee_id': db_record.committee_id,
                        'amount': db_record.expenditure_amount
                    }
                return False, f"Operating expenditure {exp_id} not found in database", None
            
            elif data_type == DataType.CANDIDATE_SUMMARY:
                candidate_id = source_record.get('candidate_id')
                cycle = source_record.get('cycle')
                if not candidate_id:
                    return False, "Missing candidate_id", None
                
                query = select(CandidateSummary).where(CandidateSummary.candidate_id == str(candidate_id).strip())
                if cycle:
                    query = query.where(CandidateSummary.cycle == int(cycle))
                
                result = await session.execute(query)
                db_record = result.scalar_one_or_none()
                if db_record:
                    return True, None, {
                        'candidate_id': db_record.candidate_id,
                        'cycle': db_record.cycle,
                        'total_receipts': db_record.total_receipts
                    }
                return False, f"Candidate summary {candidate_id} not found in database", None
            
            elif data_type == DataType.COMMITTEE_SUMMARY:
                committee_id = source_record.get('committee_id')
                cycle = source_record.get('cycle')
                if not committee_id:
                    return False, "Missing committee_id", None
                
                query = select(CommitteeSummary).where(CommitteeSummary.committee_id == str(committee_id).strip())
                if cycle:
                    query = query.where(CommitteeSummary.cycle == int(cycle))
                
                result = await session.execute(query)
                db_record = result.scalar_one_or_none()
                if db_record:
                    return True, None, {
                        'committee_id': db_record.committee_id,
                        'cycle': db_record.cycle,
                        'total_receipts': db_record.total_receipts
                    }
                return False, f"Committee summary {committee_id} not found in database", None
            
            elif data_type == DataType.ELECTIONEERING_COMM:
                # No unique ID, check by committee and candidate
                committee_id = source_record.get('CMTE_ID')
                candidate_id = source_record.get('CAND_ID')
                if not committee_id or not candidate_id:
                    return False, "Missing CMTE_ID or CAND_ID", None
                
                result = await session.execute(
                    select(ElectioneeringComm).where(
                        ElectioneeringComm.committee_id == str(committee_id).strip(),
                        ElectioneeringComm.candidate_id == str(candidate_id).strip()
                    ).limit(1)
                )
                db_record = result.scalar_one_or_none()
                if db_record:
                    return True, None, {
                        'committee_id': db_record.committee_id,
                        'candidate_id': db_record.candidate_id
                    }
                return False, f"Electioneering comm not found for committee {committee_id}, candidate {candidate_id}", None
            
            elif data_type == DataType.COMMUNICATION_COSTS:
                # No unique ID, check by committee and candidate
                committee_id = source_record.get('CMTE_ID')
                candidate_id = source_record.get('CAND_ID')
                if not committee_id or not candidate_id:
                    return False, "Missing CMTE_ID or CAND_ID", None
                
                result = await session.execute(
                    select(CommunicationCost).where(
                        CommunicationCost.committee_id == str(committee_id).strip(),
                        CommunicationCost.candidate_id == str(candidate_id).strip()
                    ).limit(1)
                )
                db_record = result.scalar_one_or_none()
                if db_record:
                    return True, None, {
                        'committee_id': db_record.committee_id,
                        'candidate_id': db_record.candidate_id
                    }
                return False, f"Communication cost not found for committee {committee_id}, candidate {candidate_id}", None
            
            else:
                return False, f"Data type {data_type.value} not supported for record verification", None
        
        except Exception as e:
            return False, f"Error verifying record: {str(e)}", None
    
    async def validate_data_integrity(
        self,
        data_type: DataType,
        cycle: int,
        sample_size: int = 100
    ) -> Dict[str, Any]:
        """
        Validate data integrity for a data type and cycle
        
        Args:
            data_type: Data type to validate
            cycle: Election cycle
            sample_size: Number of records to sample
        
        Returns:
            Dictionary with validation results
        """
        # Get file path
        file_path = self.file_counter._get_file_path(data_type, cycle)
        if not file_path or not file_path.exists():
            return {
                'valid': False,
                'error': f"File not found for {data_type.value} cycle {cycle}",
                'sample_size': 0,
                'field_validation': {},
                'database_verification': {}
            }
        
        # Sample records from file
        source_records = await self.sample_file_records(file_path, data_type, sample_size)
        if not source_records:
            return {
                'valid': False,
                'error': f"Could not sample records from file",
                'sample_size': 0,
                'field_validation': {},
                'database_verification': {}
            }
        
        # Validate field mappings
        field_validation = await self.validate_field_mappings(source_records, data_type)
        
        # Verify records in database
        database_verification = {
            'found': 0,
            'missing': 0,
            'errors': []
        }
        
        async with AsyncSessionLocal() as session:
            for record in source_records[:min(50, len(source_records))]:  # Limit to 50 for performance
                exists, error, db_record = await self.verify_record_in_database(session, record, data_type)
                if exists:
                    database_verification['found'] += 1
                else:
                    database_verification['missing'] += 1
                    database_verification['errors'].append({
                        'record_id': record.get('SUB_ID') or record.get('CAND_ID') or record.get('CMTE_ID') or 'unknown',
                        'error': error
                    })
        
        return {
            'valid': field_validation['valid'] and database_verification['missing'] == 0,
            'sample_size': len(source_records),
            'field_validation': field_validation,
            'database_verification': database_verification
        }


async def main():
    """Test the data integrity validator"""
    import asyncio
    import sys
    
    cycle = int(sys.argv[1]) if len(sys.argv) > 1 else 2026
    data_type_str = sys.argv[2] if len(sys.argv) > 2 else "candidate_master"
    
    data_type = DataType(data_type_str)
    
    validator = DataIntegrityValidator()
    results = await validator.validate_data_integrity(data_type, cycle, sample_size=50)
    
    print(f"\nData Integrity Validation for {data_type.value} (cycle {cycle}):")
    print("=" * 80)
    print(f"Valid: {results['valid']}")
    print(f"Sample Size: {results['sample_size']}")
    print(f"\nField Validation:")
    print(f"  Validated: {results['field_validation'].get('validated_fields', 0)}")
    print(f"  Errors: {len(results['field_validation'].get('errors', []))}")
    print(f"\nDatabase Verification:")
    print(f"  Found: {results['database_verification']['found']}")
    print(f"  Missing: {results['database_verification']['missing']}")


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())

