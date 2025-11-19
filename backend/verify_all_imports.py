#!/usr/bin/env python3
"""
Comprehensive verification script for all bulk data imports.

This script orchestrates verification by:
1. Counting records in source files
2. Counting records in database
3. Comparing counts and identifying discrepancies
4. Validating data integrity
5. Generating verification reports
"""
import asyncio
import logging
import sys
from datetime import datetime
from typing import Dict, List, Optional
from pathlib import Path

from app.services.bulk_data_config import DataType
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from verify_bulk_file_counts import BulkFileCounter
from verify_database_counts import DatabaseCounter
from verify_data_integrity import DataIntegrityValidator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ImportVerifier:
    """Comprehensive import verification"""
    
    def __init__(self, bulk_data_dir: Optional[str] = None):
        """
        Initialize import verifier
        
        Args:
            bulk_data_dir: Directory containing bulk data files
        """
        self.file_counter = BulkFileCounter(bulk_data_dir)
        self.db_counter = DatabaseCounter()
        self.integrity_validator = DataIntegrityValidator(bulk_data_dir)
    
    async def verify_cycle(
        self,
        cycle: int,
        validate_integrity: bool = True,
        integrity_sample_size: int = 50
    ) -> Dict[str, any]:
        """
        Verify all imports for a cycle
        
        Args:
            cycle: Election cycle year
            validate_integrity: Whether to run data integrity validation
            integrity_sample_size: Number of records to sample for integrity validation
        
        Returns:
            Dictionary with verification results
        """
        logger.info(f"Starting verification for cycle {cycle}")
        
        results = {
            'cycle': cycle,
            'timestamp': datetime.now().isoformat(),
            'file_counts': {},
            'database_counts': {},
            'metadata_counts': {},
            'status_counts': {},
            'comparisons': {},
            'integrity_validation': {},
            'summary': {
                'total_data_types': 0,
                'passed': 0,
                'failed': 0,
                'warnings': 0
            }
        }
        
        # 1. Count records in source files
        logger.info("Counting records in source files...")
        try:
            results['file_counts'] = await self.file_counter.count_all_files(cycle)
        except Exception as e:
            logger.error(f"Error counting file records: {e}", exc_info=True)
            results['file_counts'] = {}
        
        # 2. Count records in database
        logger.info("Counting records in database...")
        try:
            results['database_counts'] = await self.db_counter.count_all_tables(cycle)
        except Exception as e:
            logger.error(f"Error counting database records: {e}", exc_info=True)
            results['database_counts'] = {}
        
        # 3. Get metadata counts
        logger.info("Getting metadata counts...")
        try:
            results['metadata_counts'] = await self.db_counter.get_all_metadata_counts(cycle)
        except Exception as e:
            logger.error(f"Error getting metadata counts: {e}", exc_info=True)
            results['metadata_counts'] = {}
        
        # 4. Get status counts
        logger.info("Getting import status counts...")
        try:
            results['status_counts'] = await self.db_counter.get_all_status_counts(cycle)
        except Exception as e:
            logger.error(f"Error getting status counts: {e}", exc_info=True)
            results['status_counts'] = {}
        
        # 5. Compare counts
        logger.info("Comparing counts...")
        results['comparisons'] = self._compare_counts(
            results['file_counts'],
            results['database_counts'],
            results['metadata_counts'],
            results['status_counts']
        )
        
        # 6. Validate data integrity (optional, can be slow)
        if validate_integrity:
            logger.info("Validating data integrity (this may take a while)...")
            results['integrity_validation'] = await self._validate_integrity(
                cycle,
                integrity_sample_size
            )
        else:
            results['integrity_validation'] = {}
        
        # 7. Generate summary
        results['summary'] = self._generate_summary(results)
        
        logger.info("Verification complete")
        return results
    
    def _compare_counts(
        self,
        file_counts: Dict[str, int],
        database_counts: Dict[str, int],
        metadata_counts: Dict[str, Optional[int]],
        status_counts: Dict[str, Optional[int]]
    ) -> Dict[str, Dict[str, any]]:
        """
        Compare file counts with database counts
        
        Returns:
            Dictionary mapping data_type to comparison results
        """
        comparisons = {}
        
        for data_type in DataType:
            data_type_str = data_type.value
            file_count = file_counts.get(data_type_str, 0)
            db_count = database_counts.get(data_type_str, 0)
            metadata_count = metadata_counts.get(data_type_str)
            status_count = status_counts.get(data_type_str)
            
            # Determine expected count (prefer database count, fall back to metadata/status)
            expected_count = db_count
            if expected_count == 0 and metadata_count is not None:
                expected_count = metadata_count
            if expected_count == 0 and status_count is not None:
                expected_count = status_count
            
            # Calculate difference
            difference = file_count - expected_count
            percent_diff = (difference / file_count * 100) if file_count > 0 else 0
            
            # Determine status
            # Allow small differences due to:
            # - Skipped rows (empty, malformed)
            # - Duplicate handling
            # - Data type specific issues
            tolerance = self._get_tolerance(data_type, file_count)
            
            if abs(difference) <= tolerance:
                status = 'pass'
            elif file_count == 0 and expected_count == 0:
                status = 'warning'  # Both zero (may be expected for some data types)
            elif file_count > 0 and expected_count == 0:
                status = 'fail'  # File has records but database doesn't
            else:
                status = 'warning'  # Significant difference but not zero
            
            comparisons[data_type_str] = {
                'file_count': file_count,
                'database_count': db_count,
                'metadata_count': metadata_count,
                'status_count': status_count,
                'expected_count': expected_count,
                'difference': difference,
                'percent_difference': percent_diff,
                'status': status,
                'tolerance': tolerance
            }
        
        return comparisons
    
    def _get_tolerance(self, data_type: DataType, file_count: int) -> int:
        """
        Get acceptable tolerance for count differences
        
        Args:
            data_type: Data type
            file_count: Total file count
        
        Returns:
            Acceptable difference in record count
        """
        # Base tolerance: 0.1% or 100 records, whichever is larger
        base_tolerance = max(int(file_count * 0.001), 100)
        
        # Special cases
        if data_type == DataType.CANDIDATE_COMMITTEE_LINKAGE:
            # Linkage can have duplicates (same candidate-committee pair)
            return max(base_tolerance, 500)
        elif data_type in [DataType.OTHER_TRANSACTIONS, DataType.PAS2, DataType.PAC_SUMMARY]:
            # These are stored in metadata only, may have different counting
            return max(base_tolerance, 1000)
        elif data_type in [DataType.CANDIDATE_SUMMARY, DataType.ELECTIONEERING_COMM]:
            # These may legitimately have 0 records
            return max(base_tolerance, 0)
        
        return base_tolerance
    
    async def _validate_integrity(
        self,
        cycle: int,
        sample_size: int
    ) -> Dict[str, Dict[str, any]]:
        """
        Validate data integrity for all data types
        
        Args:
            cycle: Election cycle
            sample_size: Number of records to sample per data type
        
        Returns:
            Dictionary mapping data_type to integrity validation results
        """
        integrity_results = {}
        
        # Only validate data types that have specific tables (skip metadata-only types)
        validate_types = [
            DataType.CANDIDATE_MASTER,
            DataType.COMMITTEE_MASTER,
            DataType.INDIVIDUAL_CONTRIBUTIONS,
            DataType.INDEPENDENT_EXPENDITURES,
            DataType.OPERATING_EXPENDITURES,
            DataType.CANDIDATE_SUMMARY,
            DataType.COMMITTEE_SUMMARY,
            DataType.ELECTIONEERING_COMM,
            DataType.COMMUNICATION_COSTS,
        ]
        
        for data_type in validate_types:
            try:
                logger.info(f"Validating integrity for {data_type.value}...")
                result = await self.integrity_validator.validate_data_integrity(
                    data_type,
                    cycle,
                    sample_size
                )
                integrity_results[data_type.value] = result
            except Exception as e:
                logger.error(f"Error validating integrity for {data_type.value}: {e}", exc_info=True)
                integrity_results[data_type.value] = {
                    'valid': False,
                    'error': str(e)
                }
        
        return integrity_results
    
    def _generate_summary(self, results: Dict[str, any]) -> Dict[str, any]:
        """
        Generate summary statistics
        
        Args:
            results: Verification results
        
        Returns:
            Summary dictionary
        """
        comparisons = results.get('comparisons', {})
        
        total = len(comparisons)
        passed = sum(1 for c in comparisons.values() if c['status'] == 'pass')
        failed = sum(1 for c in comparisons.values() if c['status'] == 'fail')
        warnings = sum(1 for c in comparisons.values() if c['status'] == 'warning')
        
        # Calculate overall accuracy
        total_file_records = sum(results.get('file_counts', {}).values())
        total_db_records = sum(results.get('database_counts', {}).values())
        overall_accuracy = (total_db_records / total_file_records * 100) if total_file_records > 0 else 0
        
        return {
            'total_data_types': total,
            'passed': passed,
            'failed': failed,
            'warnings': warnings,
            'total_file_records': total_file_records,
            'total_database_records': total_db_records,
            'overall_accuracy_percent': overall_accuracy
        }


async def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Verify bulk data imports')
    parser.add_argument('cycle', type=int, help='Election cycle to verify')
    parser.add_argument('--no-integrity', action='store_true', help='Skip data integrity validation')
    parser.add_argument('--sample-size', type=int, default=50, help='Sample size for integrity validation')
    parser.add_argument('--output', type=str, help='Output file for JSON results')
    
    args = parser.parse_args()
    
    verifier = ImportVerifier()
    results = await verifier.verify_cycle(
        args.cycle,
        validate_integrity=not args.no_integrity,
        integrity_sample_size=args.sample_size
    )
    
    # Print summary
    print("\n" + "=" * 100)
    print("VERIFICATION SUMMARY")
    print("=" * 100)
    print(f"Cycle: {results['cycle']}")
    print(f"Timestamp: {results['timestamp']}")
    print(f"\nSummary:")
    print(f"  Total Data Types: {results['summary']['total_data_types']}")
    print(f"  Passed: {results['summary']['passed']}")
    print(f"  Failed: {results['summary']['failed']}")
    print(f"  Warnings: {results['summary']['warnings']}")
    print(f"\nRecord Counts:")
    print(f"  Total File Records: {results['summary']['total_file_records']:,}")
    print(f"  Total Database Records: {results['summary']['total_database_records']:,}")
    print(f"  Overall Accuracy: {results['summary']['overall_accuracy_percent']:.2f}%")
    
    print("\n" + "=" * 100)
    print("DETAILED COMPARISONS")
    print("=" * 100)
    print(f"{'Data Type':<40} {'File':<15} {'Database':<15} {'Diff':<15} {'Status':<10}")
    print("-" * 100)
    
    for data_type, comp in sorted(results['comparisons'].items()):
        status_symbol = {
            'pass': '✓',
            'fail': '✗',
            'warning': '⚠'
        }.get(comp['status'], '?')
        
        print(f"{data_type:<40} {comp['file_count']:>15,} {comp['expected_count']:>15,} "
              f"{comp['difference']:>15,} {status_symbol} {comp['status']:<10}")
    
    # Save JSON output if requested
    if args.output:
        import json
        with open(args.output, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        print(f"\nResults saved to {args.output}")
    
    # Exit with error code if failures found
    sys.exit(1 if results['summary']['failed'] > 0 else 0)


if __name__ == "__main__":
    asyncio.run(main())

