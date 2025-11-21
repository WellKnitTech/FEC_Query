"""
Bulk Data Service - Main service for FEC bulk data imports.

This module provides the BulkDataService class which coordinates the bulk data import process.
It delegates to specialized modules for different aspects of the import process:
- bulk_data.downloader: File downloading
- bulk_data.job_manager: Job tracking and status
- bulk_data.storage: Metadata and status storage
- bulk_data.cycle_manager: Cycle availability management
- bulk_data_parsers: Data parsing and storage

The service maintains backward compatibility with existing code while using a refactored
modular architecture for better maintainability.
"""
import asyncio
import gc
import hashlib
import json
import logging
import os
import uuid
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import httpx
import pandas as pd
from sqlalchemy import and_, or_, select, text
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from app.db.database import (
    AsyncSessionLocal,
    AvailableCycle,
    BulkDataImportStatus,
    BulkDataMetadata,
    BulkImportJob,
    Committee,
    Contribution,
)
from app.services.bulk_data_config import (
    DataType,
    DataTypeConfig,
    FileFormat,
    get_config,
    get_high_priority_types,
)
from app.services.bulk_data_parsers import GenericBulkDataParser

# Import refactored modules
# Use relative imports to avoid circular dependency
from .bulk_data.cycle_manager import CycleManager
from .bulk_data.downloader import BulkDataDownloader
from .bulk_data.job_manager import JobManager, _cancelled_jobs, _running_tasks
from .bulk_data.storage import BulkDataStorage

logger = logging.getLogger(__name__)

# Export for backward compatibility
__all__ = ["BulkDataService", "_running_tasks", "_cancelled_jobs"]


class BulkDataService:
    """
    Service for downloading and managing FEC bulk data imports.
    
    This service coordinates the bulk data import process, delegating to specialized modules:
    - BulkDataDownloader: Handles file downloads from FEC
    - JobManager: Manages import job tracking and status
    - BulkDataStorage: Manages metadata and status storage
    - CycleManager: Manages cycle availability checking
    - GenericBulkDataParser: Parses and stores data files
    
    The service maintains backward compatibility while using the refactored modular architecture.
    """
    
    def __init__(self):
        self.bulk_data_dir = Path(os.getenv("BULK_DATA_DIR", "./data/bulk"))
        self.bulk_data_dir.mkdir(parents=True, exist_ok=True)
        self.base_url = os.getenv(
            "FEC_BULK_DATA_BASE_URL",
            "https://www.fec.gov/files/bulk-downloads/"
        )
        
        # Initialize refactored modules
        self.downloader = BulkDataDownloader(
            bulk_data_dir=self.bulk_data_dir,
            base_url=self.base_url,
            cancelled_jobs=_cancelled_jobs
        )
        self.job_manager = JobManager(cancelled_jobs=_cancelled_jobs)
        self.storage = BulkDataStorage()
        self.cycle_manager = CycleManager(
            check_availability_func=self.downloader.check_cycle_availability
        )
        
        # Initialize parser
        self.parser = GenericBulkDataParser(self)
        
        # Expose for backward compatibility
        self.client = self.downloader.client
        self._cancelled_jobs = _cancelled_jobs
    
    def get_latest_csv_url(self, cycle: int) -> str:
        """Get FEC bulk data URL for Schedule A CSV for a specific cycle (legacy method)"""
        return self.downloader.get_latest_csv_url(cycle)
    
    def get_data_type_url(self, data_type: DataType, cycle: int) -> str:
        """Get URL for a specific data type and cycle"""
        return self.downloader.get_data_type_url(data_type, cycle)
    
    async def check_cycle_availability(self, cycle: int) -> bool:
        """Check if bulk data is available for a specific cycle"""
        return await self.downloader.check_cycle_availability(cycle)
    
    async def get_available_cycles_from_fec(self) -> List[int]:
        """Query FEC bulk data URLs to determine which cycles have data available"""
        return await self.cycle_manager.get_available_cycles_from_fec()
    
    async def download_schedule_a_csv(self, cycle: int, job_id: Optional[str] = None) -> Optional[str]:
        """Download Schedule A CSV for a specific cycle with progress tracking"""
        return await self.downloader.download_schedule_a_csv(
            cycle=cycle,
            job_id=job_id,
            update_progress_func=self._update_download_progress_wrapper
        )
    
    async def _update_download_progress_wrapper(self, job_id, cycle, downloaded_mb, total_mb):
        """Wrapper for download progress updates"""
        await self._update_download_progress(job_id, cycle, downloaded_mb, total_mb)
    
    async def _calculate_file_hash(self, file_path: Path) -> Optional[str]:
        """
        Calculate MD5 hash of a file (runs in thread pool to avoid blocking)
        
        Args:
            file_path: Path to the file
            
        Returns:
            MD5 hash as hex string, or None if file doesn't exist or error occurs
        """
        if not file_path.exists():
            return None
        
        def _hash_file():
            """Calculate hash in thread pool"""
            hash_md5 = hashlib.md5()
            try:
                with open(file_path, 'rb') as f:
                    # Read in chunks to handle large files
                    for chunk in iter(lambda: f.read(4096), b""):
                        hash_md5.update(chunk)
                return hash_md5.hexdigest()
            except Exception as e:
                logger.warning(f"Error calculating hash for {file_path}: {e}")
                return None
        
        # Run in thread pool to avoid blocking
        from app.utils.thread_pool import run_in_thread_pool
        return await run_in_thread_pool(_hash_file)
    
    async def download_bulk_data_file(
        self,
        data_type: DataType,
        cycle: int,
        job_id: Optional[str] = None,
        force_download: bool = False
    ) -> Optional[str]:
        """
        Generic method to download any FEC bulk data file type
        
        Returns path to extracted/ready-to-use file, or None if download failed
        """
        config = get_config(data_type)
        if not config:
            raise ValueError(f"Unknown data type: {data_type}")
        
        url = config.get_url(cycle, self.base_url)
        
        # Determine file paths
        if config.file_format == FileFormat.ZIP:
            download_path = self.bulk_data_dir / f"{data_type.value}_{cycle}.zip"
            if config.zip_internal_file:
                # Extract to a specific file name
                extracted_path = self.bulk_data_dir / f"{data_type.value}_{cycle}.txt"
            else:
                # Extract all files to a directory
                extracted_path = self.bulk_data_dir / f"{data_type.value}_{cycle}"
        else:  # CSV
            download_path = self.bulk_data_dir / f"{data_type.value}_{cycle}.csv"
            extracted_path = download_path
        
        # Check if file already imported (by hash)
        if not force_download:
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    select(BulkDataMetadata).where(
                        and_(
                            BulkDataMetadata.cycle == cycle,
                            BulkDataMetadata.data_type == data_type.value
                        )
                    )
                )
                metadata = result.scalar_one_or_none()
                
                # If file exists and we have a hash, check if it matches
                if metadata and metadata.file_hash and extracted_path.exists():
                    current_hash = await self._calculate_file_hash(extracted_path)
                    if current_hash and current_hash == metadata.file_hash and metadata.imported:
                        logger.info(
                            f"Skipping download and import for {data_type.value} cycle {cycle}: "
                            f"file already imported (hash: {current_hash[:16]}...)"
                        )
                        return str(extracted_path)
        
        # Check if we should skip download (file size matches and file exists)
        # Skip this check if force_download is True
        if not force_download:
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    select(BulkDataMetadata).where(
                        and_(
                            BulkDataMetadata.cycle == cycle,
                            BulkDataMetadata.data_type == data_type.value
                        )
                    )
                )
                metadata = result.scalar_one_or_none()
                
                if metadata and metadata.file_size is not None:
                    # Check remote file size
                    try:
                        head_response = await self.client.head(url, follow_redirects=True)
                        head_response.raise_for_status()
                        remote_size = int(head_response.headers.get("content-length", 0))
                        
                        # If file size matches and file exists, skip download
                        if remote_size > 0 and metadata.file_size == remote_size:
                            if extracted_path.exists():
                                logger.info(
                                    f"Skipping download for {data_type.value} cycle {cycle}: "
                                    f"file size matches ({remote_size / (1024*1024):.1f} MB) and file exists"
                                )
                                return str(extracted_path)
                            else:
                                logger.warning(
                                    f"Metadata indicates file size matches but file missing: {extracted_path}. "
                                    f"Proceeding with download."
                                )
                        else:
                            logger.info(
                                f"File size changed for {data_type.value} cycle {cycle}: "
                                f"stored={metadata.file_size}, remote={remote_size}. Re-downloading."
                            )
                    except Exception as e:
                        logger.warning(f"Could not check remote file size for {data_type.value} cycle {cycle}: {e}. Proceeding with download.")
        
        logger.info(f"Downloading {data_type.value} for cycle {cycle} from {url}")
        
        try:
            async with self.client.stream('GET', url, follow_redirects=True) as response:
                if response.status_code == 404:
                    final_url = str(response.url)
                    if final_url != url:
                        logger.warning(
                            f"File not found for {data_type.value} cycle {cycle} at redirected URL {final_url} "
                            f"(original: {url}). The data may not be available yet for this cycle."
                        )
                    else:
                        logger.warning(f"File not found for {data_type.value} cycle {cycle} at {url}")
                    
                    if job_id:
                        await self._update_job_progress(
                            job_id,
                            status='failed',
                            error_message=f"File not found for {data_type.value} cycle {cycle}. The data may not be available yet."
                        )
                    return None
                
                response.raise_for_status()
                
                total_size = int(response.headers.get("content-length", 0))
                downloaded_size = 0
                
                # Stream download
                with open(download_path, 'wb') as f:
                    async for chunk in response.aiter_bytes():
                        if job_id and job_id in _cancelled_jobs:
                            logger.info(f"Download cancelled for job {job_id}")
                            return None
                        
                        f.write(chunk)
                        downloaded_size += len(chunk)
                        
                        # Update progress every 10MB
                        if downloaded_size % (10 * 1024 * 1024) == 0:
                            downloaded_mb = downloaded_size / (1024 * 1024)
                            if total_size > 0:
                                progress_pct = (downloaded_size / total_size) * 100
                                logger.info(f"Downloaded {downloaded_mb:.1f} MB ({progress_pct:.1f}%)")
                                if job_id:
                                    await self._update_download_progress(job_id, cycle, downloaded_mb, total_size / (1024 * 1024))
                            else:
                                logger.info(f"Downloaded {downloaded_mb:.1f} MB")
                                if job_id:
                                    await self._update_download_progress(job_id, cycle, downloaded_mb, None)
                
                final_size_mb = downloaded_size / (1024 * 1024)
                logger.info(f"Downloaded {final_size_mb:.1f} MB to {download_path}")
                
                # Use total_size from header if available, otherwise use downloaded_size
                file_size_to_store = total_size if total_size > 0 else downloaded_size
                
                # Update metadata with file size immediately after download
                if file_size_to_store > 0:
                    async with AsyncSessionLocal() as session:
                        result = await session.execute(
                            select(BulkDataMetadata).where(
                                and_(
                                    BulkDataMetadata.cycle == cycle,
                                    BulkDataMetadata.data_type == data_type.value
                                )
                            )
                        )
                        metadata = result.scalar_one_or_none()
                        
                        if metadata:
                            metadata.file_size = file_size_to_store
                            metadata.download_date = datetime.utcnow()
                        else:
                            metadata = BulkDataMetadata(
                                cycle=cycle,
                                data_type=data_type.value,
                                file_size=file_size_to_store,
                                download_date=datetime.utcnow()
                            )
                            session.add(metadata)
                        
                        await session.commit()
                
                # For CSV files, calculate hash immediately after download
                if config.file_format == FileFormat.CSV and download_path.exists():
                    file_hash = await self._calculate_file_hash(download_path)
                    if file_hash:
                        async with AsyncSessionLocal() as session:
                            result = await session.execute(
                                select(BulkDataMetadata).where(
                                    and_(
                                        BulkDataMetadata.cycle == cycle,
                                        BulkDataMetadata.data_type == data_type.value
                                    )
                                )
                            )
                            metadata = result.scalar_one_or_none()
                            if metadata:
                                # Reset imported flag if hash changed
                                if metadata.file_hash and metadata.file_hash != file_hash:
                                    logger.info(f"File hash changed for {data_type.value} cycle {cycle}, resetting imported flag")
                                    metadata.imported = False
                                metadata.file_hash = file_hash
                                metadata.file_path = str(download_path)
                                await session.commit()
                            else:
                                metadata = BulkDataMetadata(
                                    cycle=cycle,
                                    data_type=data_type.value,
                                    file_path=str(download_path),
                                    file_hash=file_hash,
                                    imported=False,
                                    file_size=file_size_to_store,
                                    download_date=datetime.utcnow()
                                )
                                session.add(metadata)
                                await session.commit()
                        logger.info(f"Calculated file hash: {file_hash[:16]}... for {data_type.value} cycle {cycle}")
                
                # Extract if ZIP
                if config.file_format == FileFormat.ZIP:
                    if job_id:
                        await self._update_job_progress(
                            job_id,
                            progress_data={"status": "extracting", "cycle": cycle, "data_type": data_type.value}
                        )
                    
                    logger.info(f"Extracting {config.zip_internal_file or 'all files'} from {download_path}")
                    with zipfile.ZipFile(download_path, 'r') as zip_ref:
                        if config.zip_internal_file:
                            # Extract specific file - first try exact name, then search for similar
                            file_list = zip_ref.namelist()
                            target_file = None
                            
                            # Try exact match first
                            if config.zip_internal_file in file_list:
                                target_file = config.zip_internal_file
                            else:
                                # Search for files with similar names (case-insensitive, partial match)
                                target_basename = config.zip_internal_file.lower()
                                for file_name in file_list:
                                    if target_basename in file_name.lower() or file_name.lower().endswith(target_basename.split('.')[-1]):
                                        target_file = file_name
                                        logger.info(f"Found alternative file name in ZIP: {file_name} (expected {config.zip_internal_file})")
                                        break
                            
                            if target_file:
                                zip_ref.extract(target_file, path=self.bulk_data_dir)
                                extracted_file = self.bulk_data_dir / target_file
                                if extracted_file.exists():
                                    extracted_file.rename(extracted_path)
                                    logger.info(f"Extracted {target_file} and renamed to {extracted_path}")
                                else:
                                    logger.error(f"Extracted file {target_file} not found after extraction")
                                    if job_id:
                                        await self._update_job_progress(
                                            job_id,
                                            status='failed',
                                            error_message=f"Failed to extract {target_file} from ZIP file for {data_type.value} cycle {cycle}"
                                        )
                                    return None
                            else:
                                logger.error(f"{config.zip_internal_file} not found in ZIP file {download_path}. Available files: {file_list[:10]}")
                                if job_id:
                                    await self._update_job_progress(
                                        job_id,
                                        status='failed',
                                        error_message=f"{config.zip_internal_file} not found in ZIP file for {data_type.value} cycle {cycle}. Available files: {', '.join(file_list[:5])}"
                                    )
                                return None
                        else:
                            # Extract all files to directory
                            extracted_path.mkdir(exist_ok=True)
                            zip_ref.extractall(extracted_path)
                            logger.info(f"Extracted all files to {extracted_path}")
                
                # Calculate file hash after file is ready
                final_file_path = Path(extracted_path)
                if final_file_path.exists():
                    file_hash = await self._calculate_file_hash(final_file_path)
                    if file_hash:
                        # Update metadata with hash
                        async with AsyncSessionLocal() as session:
                            result = await session.execute(
                                select(BulkDataMetadata).where(
                                    and_(
                                        BulkDataMetadata.cycle == cycle,
                                        BulkDataMetadata.data_type == data_type.value
                                    )
                                )
                            )
                            metadata = result.scalar_one_or_none()
                            if metadata:
                                metadata.file_hash = file_hash
                                await session.commit()
                            else:
                                # Create metadata entry if it doesn't exist
                                metadata = BulkDataMetadata(
                                    cycle=cycle,
                                    data_type=data_type.value,
                                    file_path=str(extracted_path),
                                    file_hash=file_hash,
                                    download_date=datetime.utcnow()
                                )
                                session.add(metadata)
                                await session.commit()
                        logger.info(f"Calculated file hash: {file_hash[:16]}... for {data_type.value} cycle {cycle}")
                
                return str(extracted_path)
                
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                final_url = str(e.response.url)
                if final_url != url:
                    logger.warning(
                        f"File not found for {data_type.value} cycle {cycle} at redirected URL {final_url}. "
                        f"The data may not be available yet for this cycle."
                    )
                else:
                    logger.warning(f"File not found for {data_type.value} cycle {cycle} at {url}")
                
                if job_id:
                    await self._update_job_progress(
                        job_id,
                        status='failed',
                        error_message=f"File not found for {data_type.value} cycle {cycle}. The data may not be available yet."
                    )
                return None
            
            logger.error(f"HTTP error downloading {data_type.value} for cycle {cycle}: {e}")
            if job_id:
                await self._update_job_progress(job_id, status='failed', error_message=f"HTTP error: {str(e)}")
            return None
        except zipfile.BadZipFile as e:
            logger.error(f"Invalid ZIP file for {data_type.value} cycle {cycle}: {e}", exc_info=True)
            if job_id:
                await self._update_job_progress(job_id, status='failed', error_message=f"Invalid ZIP file: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Error downloading or extracting {data_type.value} for cycle {cycle}: {e}", exc_info=True)
            if job_id:
                await self._update_job_progress(job_id, status='failed', error_message=str(e))
            return None
    
    async def download_header_file(self, header_file_url: str) -> Optional[List[str]]:
        """
        Download and parse a header file to get column names
        
        Returns list of column names, or None if download failed
        """
        url = f"{self.base_url}{header_file_url}"
        header_path = self.bulk_data_dir / "headers" / header_file_url.split("/")[-1]
        header_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            async with self.client.stream('GET', url, follow_redirects=True) as response:
                if response.status_code == 404:
                    logger.warning(f"Header file not found: {url}")
                    return None
                
                response.raise_for_status()
                
                with open(header_path, 'wb') as f:
                    async for chunk in response.aiter_bytes():
                        f.write(chunk)
                
                # Parse header file (typically CSV with column names)
                try:
                    from app.utils.thread_pool import async_read_csv
                    df = await async_read_csv(header_path, nrows=0)  # Just read headers
                    columns = df.columns.tolist()
                    logger.info(f"Loaded {len(columns)} columns from header file {header_file_url}")
                    return columns
                except Exception as e:
                    logger.warning(f"Could not parse header file {header_file_url}: {e}")
                    # Try reading as plain text, one column name per line
                    with open(header_path, 'r') as f:
                        columns = [line.strip() for line in f if line.strip()]
                    return columns if columns else None
                    
        except Exception as e:
            logger.warning(f"Error downloading header file {header_file_url}: {e}")
            return None
    
    async def check_csv_freshness(self, cycle: int) -> Optional[Dict]:
        """Check if local CSV is up-to-date"""
        return await self.storage.check_csv_freshness(cycle)
    
    def _parse_date_vectorized(self, date_series: pd.Series) -> pd.Series:
        """Parse dates vectorized - handles MMDDYYYY and YYYYMMDD formats"""
        result = pd.Series([None] * len(date_series), dtype='object')
        
        # Convert to string and strip
        date_strs = date_series.astype(str).str.strip()
        
        # Filter out obviously non-date values (common data quality issues)
        # Skip values that contain letters (except for valid date formats)
        # Skip common non-date strings
        non_date_patterns = ['NOT EMPLOYED', 'N/A', 'NA', 'NULL', 'NONE', 'UNKNOWN', 'RETIRED', 'SELF']
        is_not_date = date_strs.str.upper().isin([p.upper() for p in non_date_patterns])
        
        # Filter valid 8-digit dates (must be exactly 8 digits, all numeric)
        valid_mask = (date_strs.str.len() == 8) & date_strs.str.isdigit() & ~is_not_date
        
        if valid_mask.any():
            valid_dates = date_strs[valid_mask]
            
            # Try MMDDYYYY format first
            try:
                parsed_mmddyyyy = pd.to_datetime(valid_dates, format='%m%d%Y', errors='coerce')
                result[valid_mask] = parsed_mmddyyyy
            except Exception as e:
                logger.debug(f"Date parsing failed for MMDDYYYY format, trying next: {e}")
            
            # For any that failed, try YYYYMMDD
            failed_mask = valid_mask & result.isna()
            if failed_mask.any():
                try:
                    failed_dates = date_strs[failed_mask]
                    parsed_yyyymmdd = pd.to_datetime(failed_dates, format='%Y%m%d', errors='coerce')
                    result[failed_mask] = parsed_yyyymmdd
                except Exception as e:
                    logger.debug(f"Date parsing failed for YYYYMMDD format: {e}")
        
        return result

    async def parse_and_store_csv(
        self,
        file_path: str,
        cycle: int,
        job_id: Optional[str] = None,
        batch_size: int = 50000,
        resume: bool = False
    ) -> int:
        """
        Parse and store Schedule A (individual contributions) CSV file.
        
        This method handles the parsing of individual contributions data:
        - Reads CSV in chunks to manage memory
        - Validates and cleans data fields
        - Handles date parsing with multiple formats
        - Validates committee IDs
        - Stores records in batches with error recovery
        - Supports resuming from a previous position
        
        Uses optimized bulk inserts with vectorized operations for performance.
        
        Args:
            file_path: Path to CSV file
            cycle: Election cycle year
            job_id: Optional job ID for progress tracking
            batch_size: Number of records per chunk
            resume: If True, resume from last checkpoint (for job_id)
        """
        logger.info(f"Parsing CSV file: {file_path} (resume={resume})")
        
        # FEC Schedule A CSV columns (pipe-delimited, no headers)
        # Note: File has 21 fields (not 20 as in older documentation)
        # Field 4: IMAGE_NUM (values like "P", "G2024") - this is the extra field added
        # Field 6: ENTITY_TP code (e.g., "15")
        # Field 7: ENTITY_TP description (e.g., "IND" for Individual)
        # Field 21: SUB_ID (contribution ID) - this is the last field
        # Updated to match actual file structure (21 fields)
        fec_columns = [
            'CMTE_ID', 'AMNDT_IND', 'RPT_TP', 'IMAGE_NUM', 'TRAN_ID', 
            'ENTITY_TP_CODE', 'ENTITY_TP_DESC', 'NAME',
            'CITY', 'STATE', 'ZIP_CODE', 'EMPLOYER', 'OCCUPATION', 'TRANSACTION_DT',
            'TRANSACTION_AMT', 'OTHER_ID', 'CAND_ID', 'TRAN_TP', 'FILE_NUM',
            'MEMO_CD', 'SUB_ID'  # Note: MEMO_TEXT was removed - file only has 21 fields, SUB_ID is last
        ]
        
        try:
            # Check for resume checkpoint
            rows_to_skip = 0
            initial_records = 0
            if resume and job_id:
                async with AsyncSessionLocal() as session:
                    result = await session.execute(
                        select(BulkImportJob).where(BulkImportJob.id == job_id)
                    )
                    job = result.scalar_one_or_none()
                    if job and job.file_position > 0:
                        # file_position stores rows processed (not bytes for CSV)
                        rows_to_skip = job.file_position
                        initial_records = job.imported_records or 0
                        logger.info(f"Resuming import from row {rows_to_skip} ({initial_records} records already imported)")
            
            chunk_count = 0
            total_records = initial_records
            skipped_duplicates = 0
            
            # Estimate total chunks for progress tracking
            file_size = os.path.getsize(file_path)
            estimated_rows = file_size // 200  # Rough estimate: ~200 bytes per row
            estimated_chunks = max(1, estimated_rows // batch_size)
            
            if job_id:
                await self._update_job_progress(
                    job_id,
                    current_cycle=cycle,
                    total_chunks=estimated_chunks,
                    data_type='individual_contributions',
                    file_path=file_path,
                    progress_data={"status": "parsing", "cycle": cycle, "resuming": resume}
                )
            
            async with AsyncSessionLocal() as session:
                # Read CSV in chunks - optimized for memory
                # Use skiprows to resume from checkpoint (use callable for efficiency)
                skiprows_func = None
                if rows_to_skip > 0:
                    # Create a callable that returns True for rows to skip
                    skiprows_func = lambda x: x < rows_to_skip
                
                from app.utils.thread_pool import async_read_csv
                chunk_reader = await async_read_csv(
                    file_path,
                    sep='|',
                    header=None,
                    names=fec_columns,
                    chunksize=batch_size,
                    dtype=str,
                    low_memory=False,
                    on_bad_lines='skip',
                    skiprows=skiprows_func
                )
                async for chunk in chunk_reader:
                    # Check for cancellation before processing each chunk
                    if job_id and job_id in _cancelled_jobs:
                        logger.info(f"Import cancelled for job {job_id} during chunk processing")
                        await self._update_job_progress(job_id, status='cancelled')
                        return total_records
                    
                    chunk_count += 1
                    
                    # Vectorized processing with pandas - MUCH faster than iterrows()
                    # Filter out rows without SUB_ID
                    chunk = chunk[chunk['SUB_ID'].notna() & (chunk['SUB_ID'].astype(str).str.strip() != '')]
                    if len(chunk) == 0:
                        del chunk
                        gc.collect()
                        continue
                    
                    # Vectorized field transformations
                    chunk['contribution_id'] = chunk['SUB_ID'].astype(str).str.strip()
                    
                    # Handle nullable string fields - convert to string, strip, then replace empty with None
                    def clean_str_field(series):
                        """Convert series to string, strip, and replace empty strings with None"""
                        result = series.astype(str).str.strip()
                        result = result.replace('', None).replace('nan', None)
                        return result
                    
                    chunk['candidate_id'] = clean_str_field(chunk['CAND_ID'])
                    chunk['committee_id'] = clean_str_field(chunk['CMTE_ID'])
                    
                    # Backfill candidate_id from Committee table for rows where it's missing
                    # Many contributions in bulk data don't have CAND_ID but are linked via committee_id
                    missing_candidate_mask = chunk['candidate_id'].isna() | (chunk['candidate_id'].astype(str) == '')
                    if missing_candidate_mask.any():
                        # Get unique committee_ids that need lookup
                        committees_to_lookup = chunk.loc[missing_candidate_mask, 'committee_id'].dropna().unique().tolist()
                        
                        if committees_to_lookup:
                            # Query Committee table for candidate_ids
                            # Note: select is already imported at module level
                            from app.db.database import Committee
                            
                            # Use the same session to avoid nested async context issues
                            result = await session.execute(
                                select(Committee.committee_id, Committee.candidate_ids)
                                .where(Committee.committee_id.in_(committees_to_lookup))
                            )
                            # Create mapping: committee_id -> candidate_id (use first candidate if multiple)
                            committee_map = {}
                            for row in result:
                                if row.candidate_ids and len(row.candidate_ids) > 0:
                                    committee_map[row.committee_id] = row.candidate_ids[0]  # Use first candidate
                            
                            # Backfill candidate_id using the mapping
                            if committee_map:
                                # Update only rows that need backfilling
                                for idx in chunk.index:
                                    if missing_candidate_mask.loc[idx]:
                                        comm_id = chunk.loc[idx, 'committee_id']
                                        if pd.notna(comm_id) and comm_id in committee_map:
                                            chunk.loc[idx, 'candidate_id'] = committee_map[comm_id]
                                
                                backfilled_count = chunk.loc[missing_candidate_mask, 'candidate_id'].notna().sum()
                                if backfilled_count > 0:
                                    logger.debug(f"Backfilled candidate_id for {int(backfilled_count)} contributions using committee linkages")
                    chunk['contributor_name'] = clean_str_field(chunk['NAME'])
                    chunk['contributor_city'] = clean_str_field(chunk['CITY'])
                    chunk['contributor_state'] = clean_str_field(chunk['STATE'])
                    chunk['contributor_zip'] = clean_str_field(chunk['ZIP_CODE'])
                    chunk['contributor_employer'] = clean_str_field(chunk['EMPLOYER'])
                    chunk['contributor_occupation'] = clean_str_field(chunk['OCCUPATION'])
                    chunk['contribution_type'] = clean_str_field(chunk['TRAN_TP'])
                    
                    # Extract additional FEC fields
                    chunk['amendment_indicator'] = clean_str_field(chunk.get('AMNDT_IND', pd.Series([''] * len(chunk))))
                    chunk['report_type'] = clean_str_field(chunk.get('RPT_TP', pd.Series([''] * len(chunk))))
                    chunk['transaction_id'] = clean_str_field(chunk.get('TRAN_ID', pd.Series([''] * len(chunk))))
                    # Combine ENTITY_TP_CODE and ENTITY_TP_DESC for entity_type
                    # Use CODE if available, otherwise use DESC, or combine both
                    entity_tp_code = chunk.get('ENTITY_TP_CODE', pd.Series([''] * len(chunk)))
                    entity_tp_desc = chunk.get('ENTITY_TP_DESC', pd.Series([''] * len(chunk)))
                    # Combine code and description (e.g., "15|IND" or just use code)
                    chunk['entity_type'] = (entity_tp_code.astype(str).str.strip() + 
                                           entity_tp_desc.astype(str).str.strip().replace('', '')).str.strip()
                    chunk['entity_type'] = chunk['entity_type'].replace('', None)
                    chunk['other_id'] = clean_str_field(chunk.get('OTHER_ID', pd.Series([''] * len(chunk))))
                    chunk['file_number'] = clean_str_field(chunk.get('FILE_NUM', pd.Series([''] * len(chunk))))
                    chunk['memo_code'] = clean_str_field(chunk.get('MEMO_CD', pd.Series([''] * len(chunk))))
                    # MEMO_TEXT is not a separate field in the 21-field format - set to None
                    chunk['memo_text'] = None
                    
                    # Vectorized amount parsing
                    chunk['contribution_amount'] = pd.to_numeric(
                        chunk['TRANSACTION_AMT'].astype(str).str.replace('$', '', regex=False).str.replace(',', '', regex=False).str.strip(),
                        errors='coerce'
                    ).fillna(0.0)
                    
                    # Vectorized date parsing
                    chunk['contribution_date'] = self._parse_date_vectorized(chunk['TRANSACTION_DT'])
                    # Convert pandas NaN to None for SQLite compatibility
                    chunk['contribution_date'] = chunk['contribution_date'].where(pd.notna(chunk['contribution_date']), None)
                    
                    # Build raw_data more efficiently - prepare columns for raw_data dict
                    # Convert to records list using vectorized operations
                    records_df = chunk[[
                        'contribution_id', 'candidate_id', 'committee_id', 'contributor_name',
                        'contributor_city', 'contributor_state', 'contributor_zip',
                        'contributor_employer', 'contributor_occupation', 'contribution_amount',
                        'contribution_date', 'contribution_type', 'amendment_indicator',
                        'report_type', 'transaction_id', 'entity_type', 'other_id',
                        'file_number', 'memo_code', 'memo_text'
                    ]].copy()
                    
                    # Convert to dict records
                    records = records_df.to_dict('records')
                    
                    # Clean NaN dates in records (pandas NaN -> None for SQLite compatibility)
                    for record in records:
                        if 'contribution_date' in record and (pd.isna(record['contribution_date']) or record['contribution_date'] is pd.NA):
                            record['contribution_date'] = None
                    
                    # Build raw_data efficiently using vectorized operations
                    # Create a DataFrame with ALL 20 source fields from Schedule A
                    raw_data_df = pd.DataFrame({
                        'CMTE_ID': chunk['CMTE_ID'].astype(str).where(chunk['CMTE_ID'].notna(), None),
                        'AMNDT_IND': chunk['AMNDT_IND'].astype(str).where(chunk['AMNDT_IND'].notna(), None),
                        'RPT_TP': chunk['RPT_TP'].astype(str).where(chunk['RPT_TP'].notna(), None),
                        'TRAN_ID': chunk['TRAN_ID'].astype(str).where(chunk['TRAN_ID'].notna(), None),
                        'ENTITY_TP_CODE': chunk.get('ENTITY_TP_CODE', pd.Series([None] * len(chunk))).astype(str).where(
                            chunk.get('ENTITY_TP_CODE', pd.Series([None] * len(chunk))).notna(), None),
                        'ENTITY_TP_DESC': chunk.get('ENTITY_TP_DESC', pd.Series([None] * len(chunk))).astype(str).where(
                            chunk.get('ENTITY_TP_DESC', pd.Series([None] * len(chunk))).notna(), None),
                        'IMAGE_NUM': chunk.get('IMAGE_NUM', pd.Series([None] * len(chunk))).astype(str).where(
                            chunk.get('IMAGE_NUM', pd.Series([None] * len(chunk))).notna(), None),
                        'NAME': chunk['NAME'].astype(str).where(chunk['NAME'].notna(), None),
                        'CITY': chunk['CITY'].astype(str).where(chunk['CITY'].notna(), None),
                        'STATE': chunk['STATE'].astype(str).where(chunk['STATE'].notna(), None),
                        'ZIP_CODE': chunk['ZIP_CODE'].astype(str).where(chunk['ZIP_CODE'].notna(), None),
                        'EMPLOYER': chunk['EMPLOYER'].astype(str).where(chunk['EMPLOYER'].notna(), None),
                        'OCCUPATION': chunk['OCCUPATION'].astype(str).where(chunk['OCCUPATION'].notna(), None),
                        # Convert empty strings to None for TRANSACTION_DT (FEC data has some missing dates)
                        'TRANSACTION_DT': chunk['TRANSACTION_DT'].astype(str).where(
                            (chunk['TRANSACTION_DT'].notna()) & (chunk['TRANSACTION_DT'] != ''), None
                        ),
                        'TRANSACTION_AMT': chunk['TRANSACTION_AMT'].astype(str).where(chunk['TRANSACTION_AMT'].notna(), None),
                        'OTHER_ID': chunk['OTHER_ID'].astype(str).where(chunk['OTHER_ID'].notna(), None),
                        'CAND_ID': chunk['CAND_ID'].astype(str).where(chunk['CAND_ID'].notna(), None),
                        'TRAN_TP': chunk['TRAN_TP'].astype(str).where(chunk['TRAN_TP'].notna(), None),
                        'FILE_NUM': chunk['FILE_NUM'].astype(str).where(chunk['FILE_NUM'].notna(), None),
                        'MEMO_CD': chunk['MEMO_CD'].astype(str).where(chunk['MEMO_CD'].notna(), None),
                        # MEMO_TEXT is not a field in 21-field format - don't include it in raw_data
                        'SUB_ID': chunk['SUB_ID'].astype(str).where(chunk['SUB_ID'].notna(), None),
                    })
                    
                    # Convert to dict records and add to main records
                    raw_data_records = raw_data_df.to_dict('records')
                    for i, record in enumerate(records):
                        record['raw_data'] = raw_data_records[i]
                    
                    # Bulk insert/update with smart merge (SQLite)
                    if records:
                        try:
                            # Get contribution IDs for this batch
                            contribution_ids = [r['contribution_id'] for r in records]
                            
                            # Fetch existing contributions in this batch
                            existing_query = select(Contribution).where(
                                Contribution.contribution_id.in_(contribution_ids)
                            )
                            existing_result = await session.execute(existing_query)
                            existing_contribs = {c.contribution_id: c for c in existing_result.scalars().all()}
                            
                            # Separate new and existing records
                            new_records = []
                            updated_count = 0
                            
                            # Import FECClient for smart merge
                            from app.services.fec_client import FECClient
                            fec_client = FECClient()
                            
                            for record in records:
                                contrib_id = record['contribution_id']
                                
                                if contrib_id in existing_contribs:
                                    # Use smart merge for existing records
                                    # Clean NaN dates before smart merge
                                    merge_record = {**record}
                                    if 'contribution_date' in merge_record and (pd.isna(merge_record['contribution_date']) or merge_record['contribution_date'] is pd.NA):
                                        merge_record['contribution_date'] = None
                                    
                                    existing_contrib = existing_contribs[contrib_id]
                                    fec_client._smart_merge_contribution(existing_contrib, merge_record, 'bulk')
                                    updated_count += 1
                                else:
                                    # New record - prepare for insert
                                    # Clean NaN dates before adding to new_records
                                    new_record = {**record}
                                    if 'contribution_date' in new_record and (pd.isna(new_record['contribution_date']) or new_record['contribution_date'] is pd.NA):
                                        new_record['contribution_date'] = None
                                    
                                    new_records.append({
                                        **new_record,
                                        'raw_data': new_record['raw_data'],  # Store as dict, SQLAlchemy JSON handles it
                                        'created_at': datetime.utcnow(),
                                        'data_source': 'bulk',
                                        'last_updated_from': 'bulk'
                                    })
                            
                            # Use savepoint for error recovery - if this chunk fails, rollback just this chunk
                            try:
                                async with session.begin_nested():
                                    # Bulk insert new records
                                    if new_records:
                                        await session.execute(
                                            text("""
                                                INSERT INTO contributions 
                                                (contribution_id, candidate_id, committee_id, contributor_name, 
                                                 contributor_city, contributor_state, contributor_zip, 
                                                 contributor_employer, contributor_occupation, contribution_amount,
                                                 contribution_date, contribution_type, amendment_indicator,
                                                 report_type, transaction_id, entity_type, other_id,
                                                 file_number, memo_code, memo_text, raw_data, created_at,
                                                 data_source, last_updated_from)
                                                VALUES 
                                                (:contribution_id, :candidate_id, :committee_id, :contributor_name,
                                                 :contributor_city, :contributor_state, :contributor_zip,
                                                 :contributor_employer, :contributor_occupation, :contribution_amount,
                                                 :contribution_date, :contribution_type, :amendment_indicator,
                                                 :report_type, :transaction_id, :entity_type, :other_id,
                                                 :file_number, :memo_code, :memo_text, :raw_data, :created_at,
                                                 :data_source, :last_updated_from)
                                            """),
                                            new_records
                                        )
                                    
                                    # Commit the savepoint (nested transaction)
                                    # The outer transaction will be committed below
                            except Exception as e:
                                # Rollback to savepoint (automatic with begin_nested context)
                                logger.warning(f"Failed to insert contribution chunk {chunk_count}: {e}. Skipping this chunk.")
                                # Continue with next chunk instead of failing entire operation
                                continue
                            
                            # Commit outer transaction after successful chunk processing
                            await session.commit()
                            
                            inserted = len(new_records) if new_records else 0
                            total_records += inserted + updated_count
                            
                            # Update progress every 5 chunks to reduce overhead
                            # Track file position as rows processed (for resume capability)
                            rows_processed = total_records
                            if job_id and chunk_count % 5 == 0:
                                # Calculate estimated progress percentage
                                estimated_progress = 0.0
                                if estimated_chunks > 0:
                                    estimated_progress = (chunk_count / estimated_chunks) * 100
                                
                                await self._update_job_progress(
                                    job_id,
                                    current_chunk=chunk_count,
                                    imported_records=total_records,
                                    file_position=rows_processed,  # Store rows processed for resume
                                    progress_data={
                                        "status": "importing",
                                        "cycle": cycle,
                                        "chunks_processed": chunk_count,
                                        "total_chunks_estimated": estimated_chunks,
                                        "records_imported": total_records,
                                        "records_skipped": skipped_duplicates,
                                        "rows_processed": rows_processed,
                                        "estimated_progress": min(100, estimated_progress)
                                    }
                                )
                            
                            # Log every 10 chunks to reduce I/O
                            if chunk_count % 10 == 0:
                                logger.info(
                                    f"Imported {chunk_count} chunks: {total_records} total records"
                                )
                        except Exception as e:
                            await session.rollback()
                            logger.error(f"Error committing chunk {chunk_count}: {e}")
                            # Fallback: try individual inserts/updates with smart merge for this chunk
                            # Import select with alias to avoid scoping issues
                            from sqlalchemy import select as sql_select
                            from app.services.fec_client import FECClient
                            fec_client = FECClient()
                            
                            for record in records:
                                try:
                                    contrib_id = record['contribution_id']
                                    
                                    # Check if exists
                                    existing_query = sql_select(Contribution).where(
                                        Contribution.contribution_id == contrib_id
                                    )
                                    existing_result = await session.execute(existing_query)
                                    existing_contrib = existing_result.scalar_one_or_none()
                                    
                                    if existing_contrib:
                                        # Use smart merge
                                        fec_client._smart_merge_contribution(existing_contrib, record, 'bulk')
                                        await session.commit()
                                        total_records += 1
                                    else:
                                        # Insert new
                                        # Clean NaN dates before insert
                                        insert_record = {**record}
                                        if 'contribution_date' in insert_record and (pd.isna(insert_record['contribution_date']) or insert_record['contribution_date'] is pd.NA):
                                            insert_record['contribution_date'] = None
                                        
                                        await session.execute(
                                            text("""
                                                INSERT INTO contributions 
                                                (contribution_id, candidate_id, committee_id, contributor_name, 
                                                 contributor_city, contributor_state, contributor_zip, 
                                                 contributor_employer, contributor_occupation, contribution_amount,
                                                 contribution_date, contribution_type, amendment_indicator,
                                                 report_type, transaction_id, entity_type, other_id,
                                                 file_number, memo_code, memo_text, raw_data, created_at,
                                                 data_source, last_updated_from)
                                                VALUES 
                                                (:contribution_id, :candidate_id, :committee_id, :contributor_name,
                                                 :contributor_city, :contributor_state, :contributor_zip,
                                                 :contributor_employer, :contributor_occupation, :contribution_amount,
                                                 :contribution_date, :contribution_type, :amendment_indicator,
                                                 :report_type, :transaction_id, :entity_type, :other_id,
                                                 :file_number, :memo_code, :memo_text, :raw_data, :created_at,
                                                 :data_source, :last_updated_from)
                                            """),
                                            {
                                                **insert_record,
                                                'raw_data': insert_record['raw_data'],  # Store as dict, SQLAlchemy JSON handles it
                                                'created_at': datetime.utcnow(),
                                                'data_source': 'bulk',
                                                'last_updated_from': 'bulk'
                                            }
                                        )
                                        await session.commit()
                                        total_records += 1
                                except Exception:
                                    await session.rollback()
                                    skipped_duplicates += 1
                    
                    # Clear memory explicitly after processing each chunk
                    # This helps prevent memory buildup during large imports
                    del chunk, records
                    gc.collect()
                    
                    # Log memory usage periodically for monitoring
                    if chunk_count % 10 == 0:
                        import sys
                        try:
                            memory_mb = sys.getsizeof(records) / (1024 * 1024) if 'records' in locals() else 0
                            logger.debug(
                                f"Memory cleanup after chunk {chunk_count}: "
                                f"~{memory_mb:.2f}MB freed",
                                extra={
                                    "chunk_count": chunk_count,
                                    "cycle": cycle,
                                    "memory_freed_mb": round(memory_mb, 2),
                                    "operation": "bulk_import_contributions"
                                }
                            )
                        except Exception:
                            pass  # Don't fail on memory logging
                    
                logger.info(
                    f"CSV import complete: {total_records} records imported, "
                    f"{skipped_duplicates} duplicates skipped, "
                    f"{chunk_count} chunks processed for cycle {cycle}"
                )
                
                # Checkpoint WAL after large import to prevent WAL file growth
                if total_records > 10000:  # Only checkpoint after large imports
                    try:
                        from app.lifecycle.tasks import checkpoint_wal_after_import
                        await checkpoint_wal_after_import()
                    except Exception as e:
                        logger.warning(f"Could not checkpoint WAL after import: {e}")
                
                # Log summary statistics for verification
                if total_records > 0:
                    async with AsyncSessionLocal() as session:
                        # Count total contributions for this candidate/cycle in database
                        # Note: select, func, and_ are already imported at module level
                        from sqlalchemy import func
                        from datetime import datetime
                        
                        # Count all contributions for this cycle
                        cycle_start = datetime(cycle - 1, 1, 1)
                        cycle_end = datetime(cycle, 12, 31)
                        count_query = select(func.count(Contribution.id)).where(
                            or_(
                                and_(
                                    Contribution.contribution_date >= cycle_start,
                                    Contribution.contribution_date <= cycle_end
                                ),
                                Contribution.contribution_date.is_(None)
                            )
                        )
                        total_in_db = await session.execute(count_query)
                        db_count = total_in_db.scalar() or 0
                        
                        # Sum total amounts
                        sum_query = select(func.sum(Contribution.contribution_amount)).where(
                            or_(
                                and_(
                                    Contribution.contribution_date >= cycle_start,
                                    Contribution.contribution_date <= cycle_end
                                ),
                                Contribution.contribution_date.is_(None)
                            )
                        )
                        total_amount_result = await session.execute(sum_query)
                        db_total_amount = float(total_amount_result.scalar() or 0)
                        
                        logger.info(
                            f"Bulk import verification for cycle {cycle}: "
                            f"Imported {total_records} records, "
                            f"Total in DB for cycle: {db_count} contributions, "
                            f"Total amount: ${db_total_amount:,.2f}"
                        )
            
            # Extract and cache unique committee IDs from contributions
            await self._extract_and_cache_committees()
            
            # Update metadata
            await self._update_metadata(cycle, file_path, total_records)
            
            if job_id:
                await self._update_job_progress(
                    job_id,
                    status='completed',
                    imported_records=total_records,
                    skipped_records=skipped_duplicates,
                    file_position=total_records,  # Final position
                    completed_at=datetime.utcnow()
                )
            
            return total_records
        except Exception as e:
            logger.error(f"Error parsing CSV file {file_path}: {e}", exc_info=True)
            if job_id:
                # Save current progress before marking as failed
                await self._update_job_progress(
                    job_id, 
                    status='failed', 
                    error_message=str(e),
                    file_position=total_records  # Save progress for potential resume
                )
            raise
    
    def _is_valid_committee_id(self, committee_id: str) -> bool:
        """Validate committee ID format: must start with 'C' followed by 8 digits"""
        if not committee_id or not isinstance(committee_id, str):
            return False
        # Remove whitespace
        committee_id = committee_id.strip()
        # Must start with 'C' followed by exactly 8 digits
        import re
        pattern = r'^C\d{8}$'
        return bool(re.match(pattern, committee_id))
    
    def _attempt_correct_committee_id(self, committee_id: str) -> Optional[str]:
        """
        Attempt to correct common committee ID format issues
        
        Common issues:
        - Missing leading 'C'
        - Missing leading zeros (e.g., C12345 instead of C00012345)
        - Extra whitespace
        - Lowercase 'c' instead of 'C'
        - Extra characters or formatting
        
        Returns corrected ID if fixable, None otherwise
        """
        if not committee_id or not isinstance(committee_id, str):
            return None
        
        import re
        
        # Remove all whitespace
        corrected = committee_id.strip().replace(' ', '').replace('\t', '').replace('\n', '')
        
        # If empty after cleaning, return None
        if not corrected:
            return None
        
        # If already valid, return as-is
        if self._is_valid_committee_id(corrected):
            return corrected
        
        # Try to fix common issues
        
        # 1. Missing leading 'C' but has digits
        if re.match(r'^\d+$', corrected):
            # Pad to 8 digits and add 'C'
            digits = corrected.zfill(8)
            if len(digits) == 8:
                corrected = 'C' + digits
                if self._is_valid_committee_id(corrected):
                    return corrected
        
        # 2. Has 'C' but wrong case or missing digits
        if corrected.upper().startswith('C'):
            # Normalize to uppercase 'C'
            corrected = 'C' + corrected[1:].lstrip('C').lstrip('c')
            
            # Extract digits only
            digits = re.sub(r'\D', '', corrected)
            if digits:
                # Pad to 8 digits
                digits = digits.zfill(8)
                if len(digits) == 8:
                    corrected = 'C' + digits
                    if self._is_valid_committee_id(corrected):
                        return corrected
        
        # 3. Has 'C' but too many or too few digits
        match = re.match(r'^[Cc](\d+)$', corrected)
        if match:
            digits = match.group(1)
            # If too many digits, take first 8
            if len(digits) > 8:
                digits = digits[:8]
            # Pad to 8 digits
            digits = digits.zfill(8)
            if len(digits) == 8:
                corrected = 'C' + digits
                if self._is_valid_committee_id(corrected):
                    return corrected
        
        # 4. Has 'C' but has non-digit characters - try to extract just digits
        if 'C' in corrected.upper() or 'c' in corrected:
            # Extract all digits after C
            parts = re.split(r'[Cc]', corrected, 1)
            if len(parts) > 1:
                digits = re.sub(r'\D', '', parts[1])
                if digits:
                    digits = digits.zfill(8)
                    if len(digits) == 8:
                        corrected = 'C' + digits
                        if self._is_valid_committee_id(corrected):
                            return corrected
        
        # Could not correct
        return None
    
    async def _extract_and_cache_committees(self):
        """Extract unique committee IDs from contributions and cache them"""
        try:
            async with AsyncSessionLocal() as session:
                # Get unique committee IDs from contributions
                from sqlalchemy import distinct
                result = await session.execute(
                    select(distinct(Contribution.committee_id)).where(
                        Contribution.committee_id.isnot(None)
                    )
                )
                all_committee_ids = [row[0] for row in result if row[0]]
                
                if not all_committee_ids:
                    return
                
                # Filter and attempt to correct invalid committee IDs
                valid_committee_ids = []
                invalid_committee_ids = []
                corrected_ids = {}  # Map of original -> corrected
                
                for cid in all_committee_ids:
                    # Check if already valid
                    if self._is_valid_committee_id(cid):
                        valid_committee_ids.append(cid)
                    else:
                        # Try to correct it
                        corrected = self._attempt_correct_committee_id(cid)
                        if corrected:
                            valid_committee_ids.append(corrected)
                            corrected_ids[cid] = corrected
                            logger.debug(f"Corrected committee ID: '{cid}' -> '{corrected}'")
                        else:
                            invalid_committee_ids.append(cid)
                
                invalid_count = len(invalid_committee_ids)
                corrected_count = len(corrected_ids)
                
                if corrected_count > 0:
                    logger.info(
                        f"Auto-corrected {corrected_count} committee ID(s). "
                        f"Examples: {list(corrected_ids.items())[:5]}"
                    )
                
                if invalid_count > 0:
                    logger.warning(
                        f"Found {invalid_count} uncorrectable invalid committee ID(s) from {len(all_committee_ids)} total. "
                        f"Invalid IDs (first 10): {invalid_committee_ids[:10]}"
                    )
                    # Log all invalid IDs at DEBUG level for investigation
                    logger.debug(f"All invalid committee IDs: {invalid_committee_ids}")
                
                if not valid_committee_ids:
                    logger.debug("No valid committee IDs found in contributions")
                    return
                
                logger.info(f"Found {len(valid_committee_ids)} unique valid committee IDs in contributions")
                
                # If we have corrections, update the database with corrected IDs
                if corrected_ids:
                    logger.info(f"Updating {len(corrected_ids)} contributions with corrected committee IDs")
                    for original_id, corrected_id in corrected_ids.items():
                        try:
                            # Update all contributions with the invalid ID to use the corrected ID
                            result = await session.execute(
                                text("""
                                    UPDATE contributions 
                                    SET committee_id = :corrected_id 
                                    WHERE committee_id = :original_id
                                """),
                                {"corrected_id": corrected_id, "original_id": original_id}
                            )
                            updated_count = result.rowcount
                            if updated_count > 0:
                                logger.debug(f"Updated {updated_count} contributions: '{original_id}' -> '{corrected_id}'")
                        except Exception as e:
                            logger.warning(f"Error updating committee ID '{original_id}' to '{corrected_id}': {e}")
                    
                    await session.commit()
                    logger.info(f"Successfully updated {len(corrected_ids)} committee ID corrections in database")
                
                # Check which committees we already have
                existing_result = await session.execute(
                    select(Committee.committee_id).where(
                        Committee.committee_id.in_(valid_committee_ids)
                    )
                )
                existing_ids = {row[0] for row in existing_result}
                missing_ids = [cid for cid in valid_committee_ids if cid not in existing_ids]
                
                if missing_ids:
                    logger.info(f"Fetching {len(missing_ids)} missing committees from API")
                    # Fetch missing committees in batches
                    fec_client = FECClient()
                    batch_size = 20
                    for i in range(0, len(missing_ids), batch_size):
                        batch = missing_ids[i:i + batch_size]
                        for comm_id in batch:
                            try:
                                # Fetch committee data
                                committees = await fec_client.get_committees(committee_id=comm_id, limit=1)
                                if committees:
                                    # Committee is already stored by get_committees
                                    pass
                            except Exception as e:
                                logger.debug(f"Could not fetch committee {comm_id}: {e}")
                                continue
                
        except Exception as e:
            logger.warning(f"Error extracting committees: {e}")
    
    async def _update_metadata(
        self,
        cycle: int,
        file_path: str,
        record_count: int
    ):
        """Update or create bulk data metadata"""
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(BulkDataMetadata).where(
                    and_(
                        BulkDataMetadata.cycle == cycle,
                        BulkDataMetadata.data_type == "schedule_a"
                    )
                )
            )
            metadata = result.scalar_one_or_none()
            
            if metadata:
                metadata.download_date = datetime.utcnow()
                metadata.file_path = file_path
                metadata.record_count = record_count
                metadata.last_updated = datetime.utcnow()
            else:
                metadata = BulkDataMetadata(
                    cycle=cycle,
                    data_type="schedule_a",
                    download_date=datetime.utcnow(),
                    file_path=file_path,
                    record_count=record_count,
                    last_updated=datetime.utcnow()
                )
                session.add(metadata)
            
            await session.commit()
    
    async def get_available_cycles_from_db(self) -> Optional[List[int]]:
        """Get available cycles from database"""
        return await self.storage.get_available_cycles_from_db()
    
    async def refresh_available_cycles_from_fec(self) -> List[int]:
        """Fetch available cycles from FEC API and store in database. Returns the list of cycles."""
        logger.info("Refreshing available cycles from FEC API and storing in database")
        
        # Fetch from FEC API
        available_cycles = await self.get_available_cycles_from_fec()
        
        # Store in database
        async with AsyncSessionLocal() as session:
            # Get existing cycles
            result = await session.execute(select(AvailableCycle))
            existing_cycles = {c.cycle for c in result.scalars().all()}
            
            now = datetime.utcnow()
            
            # Update or insert cycles
            for cycle in available_cycles:
                if cycle in existing_cycles:
                    # Update last_updated
                    result = await session.execute(
                        select(AvailableCycle).where(AvailableCycle.cycle == cycle)
                    )
                    cycle_record = result.scalar_one_or_none()
                    if cycle_record:
                        cycle_record.last_updated = now
                else:
                    # Insert new cycle
                    cycle_record = AvailableCycle(cycle=cycle, last_updated=now)
                    session.add(cycle_record)
            
            # Remove cycles that are no longer available (optional - we'll keep old cycles)
            # This allows users to still see historical cycles even if FEC removes them
            
            await session.commit()
        
        logger.info(f"Stored {len(available_cycles)} available cycles in database")
        return available_cycles
    
    async def get_available_cycles(self, use_fec_api: bool = False) -> List[Dict]:
        """Get list of cycles with available bulk data
        
        Args:
            use_fec_api: If True, force refresh from FEC API. If False, use DB (only refresh if DB is empty or >2 years old)
        """
        # First, try to get from database
        db_cycles = await self.get_available_cycles_from_db()
        
        # If DB has cycles and they're recent, use them
        if db_cycles is not None and not use_fec_api:
            logger.debug(f"Using {len(db_cycles)} cycles from database")
            fec_available_cycles = db_cycles
        else:
            # DB is empty, old, or user requested refresh - fetch from API
            if use_fec_api:
                logger.info("User requested refresh from FEC API")
            else:
                logger.info("Database cycles are empty or too old, fetching from FEC API")
            
            try:
                fec_available_cycles = await self.refresh_available_cycles_from_fec()
            except Exception as e:
                logger.warning(f"Failed to refresh cycles from FEC API, trying DB fallback: {e}")
                # Try DB one more time as fallback
                db_cycles = await self.get_available_cycles_from_db()
                if db_cycles is not None:
                    logger.info(f"Using {len(db_cycles)} cycles from database as fallback")
                    fec_available_cycles = db_cycles
                else:
                    # Last resort: generate all even years
                    logger.warning("No cycles in DB, falling back to generating all even years")
                    current_year = datetime.now().year
                    future_years = current_year + 6
                    fec_available_cycles = list(range(2000, future_years + 1, 2))
        
        # Get metadata for cycles that have been downloaded
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(BulkDataMetadata).where(
                    BulkDataMetadata.data_type == "schedule_a"
                )
            )
            metadata_list = result.scalars().all()
            metadata_dict = {m.cycle: m for m in metadata_list}
            
            # Return cycles with metadata if available
            cycles_list = []
            for cycle in sorted(fec_available_cycles, reverse=True):  # Most recent first
                if cycle in metadata_dict:
                    m = metadata_dict[cycle]
                    cycles_list.append({
                        "cycle": cycle,
                        "download_date": m.download_date.isoformat() if m.download_date else None,
                        "record_count": m.record_count,
                        "file_path": m.file_path,
                        "imported": True
                    })
                else:
                    cycles_list.append({
                        "cycle": cycle,
                        "download_date": None,
                        "record_count": 0,
                        "file_path": None,
                        "imported": False
                    })
            
            return cycles_list
    
    async def download_and_import_data_type(
        self,
        data_type: DataType,
        cycle: int,
        job_id: Optional[str] = None,
        batch_size: int = 50000,
        force_download: bool = False
    ) -> Dict[str, Any]:
        """
        Download and import a specific data type for a cycle.
        
        This is the main import method that orchestrates the full import process:
        1. Updates data type status to 'in_progress'
        2. Downloads the file (or skips if already imported by hash)
        3. Parses and stores the data using the appropriate parser
        4. Updates metadata and status
        
        Args:
            data_type: The FEC data type to import
            cycle: Election cycle year
            job_id: Optional job ID for progress tracking
            batch_size: Number of records to process per batch
            force_download: If True, download even if file size matches
        
        Returns:
            Dict with keys: success (bool), data_type, cycle, record_count, error (if failed)
        """
        logger.info(f"Starting download and import for {data_type.value}, cycle {cycle}")
        
        try:
            # Update status to in_progress
            await self.update_data_type_status(data_type, cycle, 'in_progress')
            
            if job_id:
                await self._update_job_progress(
                    job_id,
                    status='running',
                    current_cycle=cycle,
                    data_type=data_type.value,
                    progress_data={"status": "downloading", "cycle": cycle, "data_type": data_type.value}
                )
            
            # Download file
            file_path = await self.download_bulk_data_file(data_type, cycle, job_id=job_id, force_download=force_download)
            if not file_path:
                error_msg = f"Failed to download {data_type.value} for cycle {cycle}"
                logger.error(error_msg)
                await self.update_data_type_status(data_type, cycle, 'failed', error_message=error_msg)
                if job_id:
                    await self._update_job_progress(job_id, status='failed', error_message=error_msg)
                return {
                    "success": False,
                    "data_type": data_type.value,
                    "cycle": cycle,
                    "error": error_msg,
                    "record_count": 0
                }
            
            if job_id:
                await self._update_job_progress(
                    job_id,
                    file_path=file_path,
                    progress_data={"status": "parsing", "cycle": cycle, "data_type": data_type.value}
                )
            
            # Check if file already imported (by hash)
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    select(BulkDataMetadata).where(
                        and_(
                            BulkDataMetadata.cycle == cycle,
                            BulkDataMetadata.data_type == data_type.value
                        )
                    )
                )
                metadata = result.scalar_one_or_none()
                
                if metadata and metadata.file_hash and metadata.imported:
                    # Verify file still exists and hash matches
                    file_path_obj = Path(file_path)
                    if file_path_obj.exists() and file_path_obj.is_file():
                        current_hash = await self._calculate_file_hash(file_path_obj)
                        if current_hash and current_hash == metadata.file_hash:
                            logger.info(
                                f"Skipping import for {data_type.value} cycle {cycle}: "
                                f"file already imported (hash: {current_hash[:16]}...)"
                            )
                            if job_id:
                                await self._update_job_progress(
                                    job_id,
                                    status='completed',
                                    imported_records=metadata.record_count,
                                    completed_at=datetime.utcnow(),
                                    progress_data={"status": "skipped", "reason": "already_imported"}
                                )
                                logger.info(f"Job {job_id} marked as completed (skipped - already imported)")
                            await self.update_data_type_status(data_type, cycle, 'imported', record_count=metadata.record_count)
                            return {
                                "success": True,
                                "data_type": data_type.value,
                                "cycle": cycle,
                                "record_count": metadata.record_count,
                                "file_path": file_path,
                                "skipped": True,
                                "reason": "already_imported"
                            }
            
            # Parse and store
            logger.info(f"Parsing and storing {data_type.value} for cycle {cycle}")
            # Check if we should resume
            resume = False
            if job_id:
                async with AsyncSessionLocal() as session:
                    result = await session.execute(
                        select(BulkImportJob).where(BulkImportJob.id == job_id)
                    )
                    job = result.scalar_one_or_none()
                    if job and job.file_position > 0:
                        resume = True
            
            if data_type == DataType.INDIVIDUAL_CONTRIBUTIONS:
                record_count = await self.parse_and_store_csv(
                    file_path, cycle, job_id=job_id, batch_size=batch_size, resume=resume
                )
            else:
                # Use parser for other data types
                if not self.parser:
                    raise RuntimeError("Parser not initialized. Cannot parse data type.")
                
                if not GenericBulkDataParser.is_parser_implemented(data_type):
                    raise ValueError(f"Parser not implemented for data type: {data_type.value}")
                
                record_count = await self.parser.parse_and_store(
                    data_type, file_path, cycle, job_id=job_id, batch_size=batch_size
                )
            
            # Update metadata
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    select(BulkDataMetadata).where(
                        and_(
                            BulkDataMetadata.cycle == cycle,
                            BulkDataMetadata.data_type == data_type.value
                        )
                    )
                )
                metadata = result.scalar_one_or_none()
                
                if metadata:
                    metadata.download_date = datetime.utcnow()
                    metadata.file_path = file_path
                    metadata.record_count = record_count
                    metadata.last_updated = datetime.utcnow()
                    metadata.imported = True  # Mark as imported after successful import
                    # file_size should already be set during download, but update if missing
                    if metadata.file_size is None:
                        # Try to get file size from downloaded file
                        file_path_obj = Path(file_path)
                        if file_path_obj.exists() and file_path_obj.is_file():
                            metadata.file_size = file_path_obj.stat().st_size
                    # Ensure file_hash is set (should be set during download, but verify)
                    if not metadata.file_hash:
                        file_path_obj = Path(file_path)
                        if file_path_obj.exists() and file_path_obj.is_file():
                            file_hash = await self._calculate_file_hash(file_path_obj)
                            if file_hash:
                                metadata.file_hash = file_hash
                else:
                    # Get file size and hash from downloaded file if not already set
                    file_size = None
                    file_hash = None
                    file_path_obj = Path(file_path)
                    if file_path_obj.exists() and file_path_obj.is_file():
                        file_size = file_path_obj.stat().st_size
                        file_hash = await self._calculate_file_hash(file_path_obj)
                    
                    metadata = BulkDataMetadata(
                        cycle=cycle,
                        data_type=data_type.value,
                        download_date=datetime.utcnow(),
                        file_path=file_path,
                        file_size=file_size,
                        file_hash=file_hash,
                        imported=True,  # Mark as imported after successful import
                        record_count=record_count,
                        last_updated=datetime.utcnow()
                    )
                    session.add(metadata)
                
                await session.commit()
            
            logger.info(f"Successfully imported {record_count} records for {data_type.value}, cycle {cycle}")
            
            # Update status to imported
            await self.update_data_type_status(data_type, cycle, 'imported', record_count=record_count)
            
            # Schedule analysis computation after successful import
            try:
                from app.services.fec_client import FECClient
                from app.services.analysis.orchestrator import AnalysisOrchestratorService
                
                # Get affected candidates if this is a contribution-related import
                affected_candidates = None
                if data_type.value in ['individual_contributions', 'schedule_a']:
                    try:
                        from datetime import datetime
                        # Cycle covers (cycle-1)-01-01 to cycle-12-31
                        cycle_start = datetime(cycle - 1, 1, 1)
                        cycle_end = datetime(cycle, 12, 31)
                        
                        async with AsyncSessionLocal() as session:
                            result = await session.execute(
                                select(Contribution.candidate_id)
                                .where(
                                    and_(
                                        Contribution.candidate_id.isnot(None),
                                        or_(
                                            and_(
                                                Contribution.contribution_date >= cycle_start,
                                                Contribution.contribution_date <= cycle_end
                                            ),
                                            Contribution.contribution_date.is_(None)  # Include undated
                                        )
                                    )
                                )
                                .distinct()
                                .limit(1000)  # Limit to avoid loading too many
                            )
                            candidate_ids = [row[0] for row in result if row[0]]
                            if candidate_ids:
                                affected_candidates = candidate_ids
                    except Exception as e:
                        logger.warning(f"Could not get affected candidates for analysis scheduling: {e}")
                
                # Schedule analysis computation in background
                fec_client = FECClient()
                orchestrator = AnalysisOrchestratorService(fec_client)
                analysis_job_id = await orchestrator.schedule_analysis_after_import(
                    cycle=cycle,
                    data_type=data_type.value,
                    affected_candidates=affected_candidates
                )
                
                if analysis_job_id:
                    logger.info(
                        f"Scheduled analysis computation job {analysis_job_id} "
                        f"for cycle {cycle} after import"
                    )
            except Exception as e:
                # Don't fail the import if analysis scheduling fails
                logger.warning(
                    f"Failed to schedule analysis computation after import: {e}",
                    exc_info=True
                )
            
            # Always mark job as completed on success
            if job_id:
                completed_at = datetime.utcnow()
                await self._update_job_progress(
                    job_id,
                    status='completed',
                    imported_records=record_count,
                    completed_at=completed_at,
                    progress_data={"status": "completed", "cycle": cycle, "data_type": data_type.value}
                )
                logger.info(f"Job {job_id} marked as completed with {record_count} records imported")
            
            return {
                "success": True,
                "data_type": data_type.value,
                "cycle": cycle,
                "record_count": record_count,
                "file_path": file_path
            }
            
        except Exception as e:
            error_msg = f"Error downloading/importing {data_type.value} for cycle {cycle}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            # Update status to failed
            try:
                await self.update_data_type_status(data_type, cycle, 'failed', error_message=error_msg)
            except Exception as status_error:
                logger.error(f"Failed to update data type status: {status_error}", exc_info=True)
            
            # Always mark job as failed on exception
            if job_id:
                try:
                    await self._update_job_progress(
                        job_id,
                        status='failed',
                        error_message=error_msg
                    )
                    logger.info(f"Job {job_id} marked as failed: {error_msg}")
                except Exception as job_error:
                    logger.error(f"Failed to update job {job_id} status: {job_error}", exc_info=True)
            
            return {
                "success": False,
                "data_type": data_type.value,
                "cycle": cycle,
                "error": error_msg,
                "record_count": 0
            }
    
    async def get_all_cycles_list(self, use_fec_api: bool = True) -> List[int]:
        """Get list of all available cycles as integers
        
        Args:
            use_fec_api: If True, query FEC API to check which cycles actually have data.
                        If False or API check fails, falls back to all even years 2000-current+6
        """
        if use_fec_api:
            try:
                return await self.get_available_cycles_from_fec()
            except Exception as e:
                logger.warning(f"Failed to get cycles from FEC API, falling back: {e}")
        
        # Fallback: generate all even years
        current_year = datetime.now().year
        future_years = current_year + 6  # Include next 3 election cycles
        return list(range(2000, future_years + 1, 2))
    
    async def clear_contributions(self, cycle: Optional[int] = None) -> int:
        """Clear contributions from the database
        
        Args:
            cycle: Optional cycle to clear. If None, clears all contributions.
        
        Returns:
            Number of contributions deleted
        """
        async with AsyncSessionLocal() as session:
            from sqlalchemy import delete
            
            if cycle:
                # Clear contributions that might be from a specific cycle
                # Since we don't track cycle directly in contributions, we'll clear all
                # and rely on re-importing for the specific cycle
                result = await session.execute(delete(Contribution))
            else:
                result = await session.execute(delete(Contribution))
            
            await session.commit()
            deleted_count = result.rowcount
            logger.info(f"Cleared {deleted_count} contributions from database")
            return deleted_count
    
    async def clear_all_data(self) -> Dict[str, int]:
        """Clear all data from the database (contributions, committees, candidates, etc.)
        
        Returns:
            Dictionary with counts of deleted records per table
        """
        async with AsyncSessionLocal() as session:
            from sqlalchemy import delete
            from app.db.database import (
                Contribution, Committee, Candidate, FinancialTotal,
                BulkDataMetadata, BulkImportJob, IndependentExpenditure,
                OperatingExpenditure, CandidateSummary, CommitteeSummary,
                ElectioneeringComm, CommunicationCost
            )
            
            deleted_counts = {}
            
            try:
                # Clear in order to respect foreign key constraints
                # Start with dependent tables first
                logger.info("Clearing all data from database...")
                
                # Clear contributions
                result = await session.execute(delete(Contribution))
                deleted_counts['contributions'] = result.rowcount
                logger.info(f"Cleared {result.rowcount} contributions")
                
                # Clear financial totals
                result = await session.execute(delete(FinancialTotal))
                deleted_counts['financial_totals'] = result.rowcount
                logger.info(f"Cleared {result.rowcount} financial totals")
                
                # Clear independent expenditures
                result = await session.execute(delete(IndependentExpenditure))
                deleted_counts['independent_expenditures'] = result.rowcount
                logger.info(f"Cleared {result.rowcount} independent expenditures")
                
                # Clear operating expenditures
                result = await session.execute(delete(OperatingExpenditure))
                deleted_counts['operating_expenditures'] = result.rowcount
                logger.info(f"Cleared {result.rowcount} operating expenditures")
                
                # Clear candidate summaries
                result = await session.execute(delete(CandidateSummary))
                deleted_counts['candidate_summaries'] = result.rowcount
                logger.info(f"Cleared {result.rowcount} candidate summaries")
                
                # Clear committee summaries
                result = await session.execute(delete(CommitteeSummary))
                deleted_counts['committee_summaries'] = result.rowcount
                logger.info(f"Cleared {result.rowcount} committee summaries")
                
                # Clear electioneering communications
                result = await session.execute(delete(ElectioneeringComm))
                deleted_counts['electioneering_comm'] = result.rowcount
                logger.info(f"Cleared {result.rowcount} electioneering communications")
                
                # Clear communication costs
                result = await session.execute(delete(CommunicationCost))
                deleted_counts['communication_costs'] = result.rowcount
                logger.info(f"Cleared {result.rowcount} communication costs")
                
                # Clear committees (after clearing dependent data)
                result = await session.execute(delete(Committee))
                deleted_counts['committees'] = result.rowcount
                logger.info(f"Cleared {result.rowcount} committees")
                
                # Clear candidates (after clearing dependent data)
                result = await session.execute(delete(Candidate))
                deleted_counts['candidates'] = result.rowcount
                logger.info(f"Cleared {result.rowcount} candidates")
                
                # Clear bulk data metadata
                result = await session.execute(delete(BulkDataMetadata))
                deleted_counts['bulk_data_metadata'] = result.rowcount
                logger.info(f"Cleared {result.rowcount} bulk data metadata records")
                
                # Clear import jobs (optional - you might want to keep history)
                result = await session.execute(delete(BulkImportJob))
                deleted_counts['import_jobs'] = result.rowcount
                logger.info(f"Cleared {result.rowcount} import jobs")
                
                await session.commit()
                logger.info("Successfully cleared all data from database")
                
                return deleted_counts
                
            except Exception as e:
                await session.rollback()
                logger.error(f"Error clearing database: {e}", exc_info=True)
                raise
    
    async def _update_job_progress(
        self,
        job_id: str,
        status: Optional[str] = None,
        current_cycle: Optional[int] = None,
        current_chunk: Optional[int] = None,
        total_chunks: Optional[int] = None,
        imported_records: Optional[int] = None,
        skipped_records: Optional[int] = None,
        file_position: Optional[int] = None,
        data_type: Optional[str] = None,
        file_path: Optional[str] = None,
        error_message: Optional[str] = None,
        completed_at: Optional[datetime] = None,
        progress_data: Optional[Dict] = None
    ):
        """Update job progress using job manager"""
        await self.job_manager.update_job_progress(
            job_id=job_id,
            status=status,
            current_cycle=current_cycle,
            current_chunk=current_chunk,
            total_chunks=total_chunks,
            imported_records=imported_records,
            skipped_records=skipped_records,
            file_position=file_position,
            progress_data=progress_data,
            error_message=error_message
        )
    
    async def _update_download_progress(
        self,
        job_id: str,
        cycle: int,
        downloaded_mb: float,
        total_mb: Optional[float]
    ):
        """Update download progress for a cycle"""
        try:
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    select(BulkImportJob).where(BulkImportJob.id == job_id)
                )
                job = result.scalar_one_or_none()
                
                if job:
                    progress_data = job.progress_data or {}
                    cycle_progress = progress_data.get('cycle_progress', {})
                    
                    cycle_progress[str(cycle)] = {
                        "status": "downloading",
                        "downloaded_mb": downloaded_mb,
                        "total_mb": total_mb,
                        "progress_pct": (downloaded_mb / total_mb * 100) if total_mb else None
                    }
                    
                    progress_data['cycle_progress'] = cycle_progress
                    progress_data['current_cycle'] = cycle
                    job.progress_data = progress_data
                    
                    await session.commit()
        except Exception as e:
            logger.warning(f"Error updating download progress: {e}")
    
    async def get_recent_jobs(self, limit: int = 10) -> List[BulkImportJob]:
        """Get recent import jobs (all statuses), ordered by started_at descending"""
        return await self.job_manager.get_recent_jobs(limit)
    
    async def get_incomplete_jobs(self) -> List[BulkImportJob]:
        """Get all incomplete import jobs (running or failed)"""
        return await self.job_manager.get_incomplete_jobs()
    
    async def resume_job(self, job_id: str) -> bool:
        """Resume an incomplete import job
        
        Returns True if job was resumed, False if job not found or not resumable
        """
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(BulkImportJob).where(BulkImportJob.id == job_id)
            )
            job = result.scalar_one_or_none()
            
            if not job:
                logger.warning(f"Job {job_id} not found")
                return False
            
            if job.status not in ['running', 'pending', 'failed']:
                logger.warning(f"Job {job_id} is in status {job.status}, cannot resume")
                return False
            
            if not job.file_path or not os.path.exists(job.file_path):
                logger.warning(f"Job {job_id} file path {job.file_path} does not exist, cannot resume")
                return False
            
            if not job.data_type:
                logger.warning(f"Job {job_id} has no data_type, cannot resume")
                return False
            
            # Resume the job
            logger.info(f"Resuming job {job_id}: {job.data_type} for cycle {job.cycle}")
            job.status = 'running'
            await session.commit()
            
            # Start the import in background
            try:
                if job.data_type == 'individual_contributions':
                    await self.parse_and_store_csv(
                        job.file_path,
                        job.cycle,
                        job_id=job_id,
                        resume=True
                    )
                else:
                    # For other data types, use the parser
                    data_type_enum = DataType(job.data_type)
                    await self.parser.parse_and_store(
                        data_type_enum,
                        job.file_path,
                        job.cycle,
                        job_id=job_id,
                        resume=True
                    )
                return True
            except Exception as e:
                logger.error(f"Error resuming job {job_id}: {e}", exc_info=True)
                await self._update_job_progress(job_id, status='failed', error_message=str(e))
                return False
    
    async def create_job(
        self,
        job_type: str,
        cycle: Optional[int] = None,
        cycles: Optional[List[int]] = None,
        data_type: Optional[str] = None
    ) -> str:
        """Create a new import job and return job_id"""
        job = await self.job_manager.create_job(
            job_type=job_type,
            cycle=cycle,
            cycles=cycles,
            data_type=data_type
        )
        return job.id
    
    async def get_job(self, job_id: str) -> Optional[BulkImportJob]:
        """Get job by ID"""
        return await self.job_manager.get_job(job_id)
    
    async def get_data_type_status(self, cycle: int, data_type: DataType) -> Optional[BulkDataImportStatus]:
        """Get import status for a specific data type and cycle"""
        return await self.storage.get_data_type_status(cycle, data_type)
    
    async def get_all_data_type_statuses(self, cycle: int) -> Dict[str, BulkDataImportStatus]:
        """Get import status for all data types for a cycle"""
        return await self.storage.get_all_data_type_statuses(cycle)
    
    async def update_data_type_status(
        self,
        data_type: DataType,
        cycle: int,
        status: str,
        record_count: int = 0,
        error_message: Optional[str] = None
    ):
        """Update or create import status for a data type and cycle"""
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(BulkDataImportStatus).where(
                    and_(
                        BulkDataImportStatus.data_type == data_type.value,
                        BulkDataImportStatus.cycle == cycle
                    )
                )
            )
            existing = result.scalar_one_or_none()
            
            if existing:
                existing.status = status
                existing.record_count = record_count
                existing.error_message = error_message
                existing.updated_at = datetime.utcnow()
                if status == 'imported':
                    existing.last_imported_at = datetime.utcnow()
            else:
                new_status = BulkDataImportStatus(
                    data_type=data_type.value,
                    cycle=cycle,
                    status=status,
                    record_count=record_count,
                    error_message=error_message,
                    last_imported_at=datetime.utcnow() if status == 'imported' else None
                )
                session.add(new_status)
            
            await session.commit()
    
    async def cancel_job(self, job_id: str) -> bool:
        """Cancel a job"""
        return await self.job_manager.cancel_job(job_id)
    
    async def import_multiple_data_types(
        self,
        cycle: int,
        data_types: List[DataType],
        job_id: Optional[str] = None,
        force_download: bool = False
    ) -> Dict[str, Any]:
        """Import multiple data types for a cycle"""
        results = {}
        for data_type in data_types:
            try:
                result = await self.download_and_import_data_type(data_type, cycle, job_id=job_id, force_download=force_download)
                results[data_type.value] = result
            except Exception as e:
                logger.error(f"Error importing {data_type.value} for cycle {cycle}: {e}", exc_info=True)
                results[data_type.value] = {
                    "success": False,
                    "error": str(e),
                    "record_count": 0
                }
        return results
    
    async def import_all_data_types_for_cycle(
        self,
        cycle: int,
        job_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Import all implemented data types for a cycle"""
        from app.services.bulk_data_parsers import GenericBulkDataParser
        
        # Get all data types that have parsers implemented
        all_types = list(DataType)
        implemented_types = [
            dt for dt in all_types
            if GenericBulkDataParser.is_parser_implemented(dt)
        ]
        
        logger.info(f"Importing {len(implemented_types)} data types for cycle {cycle}")
        return await self.import_multiple_data_types(cycle, implemented_types, job_id=job_id)
    
    async def cleanup(self):
        """Clean up resources"""
        # Close downloader HTTP client
        await self.downloader.close()
        # Also close the exposed client reference (same client, but ensure it's closed)
        if hasattr(self, 'client') and self.client:
            try:
                await self.client.aclose()
            except Exception as e:
                logger.debug(f"Error closing client during cleanup: {e}")

