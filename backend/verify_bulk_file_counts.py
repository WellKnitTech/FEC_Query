#!/usr/bin/env python3
"""
Count records in bulk data source files.

This module counts records in FEC bulk data files to verify import accuracy.
Supports both extracted files (.txt, .csv) and ZIP archives.
"""
import os
import zipfile
import logging
from pathlib import Path
from typing import Dict, Optional, Tuple
from app.services.bulk_data_config import DataType, get_config, FileFormat
from app.utils.thread_pool import async_read_csv

logger = logging.getLogger(__name__)


class BulkFileCounter:
    """Count records in bulk data source files"""
    
    def __init__(self, bulk_data_dir: Optional[str] = None):
        """
        Initialize bulk file counter
        
        Args:
            bulk_data_dir: Directory containing bulk data files (default: ./data/bulk)
        """
        if bulk_data_dir is None:
            bulk_data_dir = os.getenv("BULK_DATA_DIR", "./data/bulk")
        self.bulk_data_dir = Path(bulk_data_dir)
    
    def _get_file_path(self, data_type: DataType, cycle: int) -> Optional[Path]:
        """
        Get path to bulk data file for a data type and cycle
        
        Returns path to extracted file (.txt or .csv) if it exists,
        otherwise returns path to ZIP file.
        """
        config = get_config(data_type)
        if not config:
            return None
        
        # Try extracted file first
        if config.file_format == FileFormat.ZIP:
            extracted_path = self.bulk_data_dir / f"{data_type.value}_{cycle}.txt"
            if extracted_path.exists():
                return extracted_path
            # Fall back to ZIP file
            zip_path = self.bulk_data_dir / f"{data_type.value}_{cycle}.zip"
            if zip_path.exists():
                return zip_path
        else:  # CSV
            csv_path = self.bulk_data_dir / f"{data_type.value}_{cycle}.csv"
            if csv_path.exists():
                return csv_path
        
        return None
    
    def _get_file_format_info(self, data_type: DataType) -> Tuple[str, bool]:
        """
        Get file format information for a data type
        
        Returns:
            Tuple of (separator, has_header)
        """
        # Pipe-delimited files (no headers)
        pipe_delimited = [
            DataType.CANDIDATE_MASTER,
            DataType.COMMITTEE_MASTER,
            DataType.CANDIDATE_COMMITTEE_LINKAGE,
            DataType.OPERATING_EXPENDITURES,
            DataType.OTHER_TRANSACTIONS,
            DataType.PAS2,
            DataType.PAC_SUMMARY,
            DataType.INDIVIDUAL_CONTRIBUTIONS,
        ]
        
        # CSV files with headers
        csv_with_headers = [
            DataType.CANDIDATE_SUMMARY,
            DataType.COMMITTEE_SUMMARY,
            DataType.INDEPENDENT_EXPENDITURES,
            DataType.ELECTIONEERING_COMM,
            DataType.COMMUNICATION_COSTS,
        ]
        
        if data_type in pipe_delimited:
            return ('|', False)
        elif data_type in csv_with_headers:
            return (',', True)
        else:
            # Default to pipe-delimited
            return ('|', False)
    
    async def count_file_records(
        self,
        file_path: Path,
        data_type: DataType,
        chunk_size: int = 100000
    ) -> int:
        """
        Count records in a file
        
        Args:
            file_path: Path to file (can be .txt, .csv, or .zip)
            data_type: Data type for format determination
            chunk_size: Number of rows to read per chunk (for memory efficiency)
        
        Returns:
            Number of records in file
        """
        separator, has_header = self._get_file_format_info(data_type)
        
        # Handle ZIP files
        if file_path.suffix == '.zip':
            return await self._count_zip_records(file_path, data_type, separator, has_header, chunk_size)
        
        # Handle regular files
        return await self._count_regular_file_records(file_path, separator, has_header, chunk_size)
    
    async def _count_zip_records(
        self,
        zip_path: Path,
        data_type: DataType,
        separator: str,
        has_header: bool,
        chunk_size: int
    ) -> int:
        """Count records in a ZIP file"""
        config = get_config(data_type)
        if not config or not config.zip_internal_file:
            logger.warning(f"No zip_internal_file configured for {data_type.value}")
            return 0
        
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_file:
                # Find the internal file
                internal_file = None
                for name in zip_file.namelist():
                    if name.endswith(config.zip_internal_file) or name == config.zip_internal_file:
                        internal_file = name
                        break
                
                if not internal_file:
                    logger.warning(f"Could not find {config.zip_internal_file} in {zip_path}")
                    return 0
                
                # Extract to temporary location and count
                import tempfile
                with tempfile.TemporaryDirectory() as temp_dir:
                    extracted_path = Path(temp_dir) / internal_file
                    with zip_file.open(internal_file) as source, open(extracted_path, 'wb') as target:
                        target.write(source.read())
                    
                    return await self._count_regular_file_records(
                        extracted_path, separator, has_header, chunk_size
                    )
        except Exception as e:
            logger.error(f"Error counting records in ZIP file {zip_path}: {e}", exc_info=True)
            return 0
    
    async def _count_regular_file_records(
        self,
        file_path: Path,
        separator: str,
        has_header: bool,
        chunk_size: int
    ) -> int:
        """Count records in a regular file"""
        try:
            total_count = 0
            
            # Read file in chunks for memory efficiency
            chunk_reader = await async_read_csv(
                str(file_path),
                sep=separator,
                header=0 if has_header else None,
                chunksize=chunk_size,
                dtype=str,
                low_memory=False,
                on_bad_lines='skip',
                encoding='utf-8',
                encoding_errors='replace'
            )
            
            async for chunk in chunk_reader:
                # Count non-empty rows (filter out completely empty rows)
                chunk_count = len(chunk)
                # Filter out rows where all values are NaN or empty
                if chunk_count > 0:
                    # Check if row has at least one non-null value
                    non_empty = chunk.notna().any(axis=1).sum()
                    total_count += non_empty
            
            return total_count
        except UnicodeDecodeError:
            # Try with different encoding
            try:
                total_count = 0
                chunk_reader = await async_read_csv(
                    str(file_path),
                    sep=separator,
                    header=0 if has_header else None,
                    chunksize=chunk_size,
                    dtype=str,
                    low_memory=False,
                    on_bad_lines='skip',
                    encoding='latin-1',
                    encoding_errors='replace'
                )
                
                async for chunk in chunk_reader:
                    chunk_count = len(chunk)
                    if chunk_count > 0:
                        non_empty = chunk.notna().any(axis=1).sum()
                        total_count += non_empty
                
                return total_count
            except Exception as e:
                logger.error(f"Error counting records in file {file_path} with latin-1: {e}", exc_info=True)
                return 0
        except Exception as e:
            logger.error(f"Error counting records in file {file_path}: {e}", exc_info=True)
            return 0
    
    async def count_all_files(self, cycle: int) -> Dict[str, int]:
        """
        Count records in all bulk data files for a cycle
        
        Args:
            cycle: Election cycle year
        
        Returns:
            Dictionary mapping data_type.value to record count
        """
        results = {}
        
        for data_type in DataType:
            file_path = self._get_file_path(data_type, cycle)
            if file_path and file_path.exists():
                count = await self.count_file_records(file_path, data_type)
                results[data_type.value] = count
                logger.info(f"Counted {count:,} records in {data_type.value} for cycle {cycle}")
            else:
                results[data_type.value] = 0
                logger.warning(f"File not found for {data_type.value} cycle {cycle}")
        
        return results


async def main():
    """Test the file counter"""
    import asyncio
    import sys
    
    cycle = int(sys.argv[1]) if len(sys.argv) > 1 else 2026
    
    counter = BulkFileCounter()
    results = await counter.count_all_files(cycle)
    
    print(f"\nRecord counts for cycle {cycle}:")
    print("=" * 80)
    for data_type, count in sorted(results.items()):
        print(f"{data_type:<40} {count:>15,}")


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())

