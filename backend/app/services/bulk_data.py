import httpx
import pandas as pd
import asyncio
import os
import logging
import uuid
import gc
import zipfile
from typing import Optional, Dict, List, Any, Set
from datetime import datetime, timedelta
from pathlib import Path
from sqlalchemy import select, and_, text
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from app.db.database import AsyncSessionLocal, Contribution, BulkDataMetadata, Committee, BulkImportJob, BulkDataImportStatus
from app.services.fec_client import FECClient
from app.services.bulk_data_config import (
    DataType, DataTypeConfig, FileFormat, get_config, get_high_priority_types
)
from app.services.bulk_data_parsers import GenericBulkDataParser
import json

logger = logging.getLogger(__name__)

# Global set to track cancelled jobs
_cancelled_jobs: Set[str] = set()

# Global set to track running background tasks for graceful shutdown
_running_tasks: Set[asyncio.Task] = set()


class BulkDataService:
    """Service for downloading and managing FEC bulk CSV data"""
    
    def __init__(self):
        self.bulk_data_dir = Path(os.getenv("BULK_DATA_DIR", "./data/bulk"))
        self.bulk_data_dir.mkdir(parents=True, exist_ok=True)
        self.base_url = os.getenv(
            "FEC_BULK_DATA_BASE_URL",
            "https://www.fec.gov/files/bulk-downloads/"
        )
        # Enable redirect following (default is True, but being explicit)
        self.client = httpx.AsyncClient(
            timeout=300.0,  # 5 minute timeout for large files
            follow_redirects=True  # Explicitly enable redirect following
        )
        # Initialize parser
        self.parser = GenericBulkDataParser(self)
        # Expose cancelled_jobs for parser access
        self._cancelled_jobs = _cancelled_jobs
    
    def get_latest_csv_url(self, cycle: int) -> str:
        """Get FEC bulk data URL for Schedule A CSV for a specific cycle (legacy method)"""
        # For backward compatibility, use individual contributions
        config = get_config(DataType.INDIVIDUAL_CONTRIBUTIONS)
        return config.get_url(cycle, self.base_url)
    
    def get_data_type_url(self, data_type: DataType, cycle: int) -> str:
        """Get URL for a specific data type and cycle"""
        config = get_config(data_type)
        if not config:
            raise ValueError(f"Unknown data type: {data_type}")
        return config.get_url(cycle, self.base_url)
    
    async def check_cycle_availability(self, cycle: int) -> bool:
        """Check if bulk data is available for a specific cycle by making a HEAD request"""
        try:
            url = self.get_latest_csv_url(cycle)
            # Try HEAD without following redirects first
            response = await self.client.head(url, follow_redirects=False, timeout=10.0)
            
            # If we get a 302, the file exists but redirects to S3
            # However, we need to verify the S3 URL actually works with a GET
            if response.status_code == 302:
                redirect_url = response.headers.get('Location')
                if redirect_url:
                    # Make redirect URL absolute if needed
                    if not redirect_url.startswith('http'):
                        from urllib.parse import urljoin
                        redirect_url = urljoin(url, redirect_url)
                    
                    # Try a GET request to the redirect URL to verify it actually works
                    # Use a small range request to avoid downloading the whole file
                    try:
                        get_response = await self.client.get(
                            redirect_url,
                            headers={'Range': 'bytes=0-0'},  # Just get first byte
                            timeout=10.0,
                            follow_redirects=True
                        )
                        # 206 (Partial Content) or 200 means the file exists
                        return get_response.status_code in [200, 206]
                    except httpx.HTTPStatusError as get_error:
                        # 404 means the file doesn't exist on S3
                        if get_error.response.status_code == 404:
                            return False
                        # Other errors - assume available since we got a redirect
                        return True
                    except Exception:
                        # If GET fails for other reasons, assume available since we got redirect
                        return True
                # Got 302 but no Location header - treat as available
                return True
            
            # 200 means file exists
            if response.status_code == 200:
                return True
            
            # 404 means not available
            return False
            
        except httpx.HTTPStatusError as e:
            # If we get a 404, the file doesn't exist
            if e.response.status_code == 404:
                return False
            # Other errors might mean the file exists but there's an issue
            logger.debug(f"HTTP error checking availability for cycle {cycle}: {e}")
            return False
        except Exception as e:
            logger.debug(f"Error checking availability for cycle {cycle}: {e}")
            return False
    
    async def get_available_cycles_from_fec(self) -> List[int]:
        """Query FEC bulk data URLs to determine which cycles have data available"""
        current_year = datetime.now().year
        # Check cycles from 2000 to current year + 6
        future_years = current_year + 6
        cycles_to_check = list(range(2000, future_years + 1, 2))
        
        available_cycles = []
        
        # Check in batches to avoid too many concurrent requests
        batch_size = 10
        for i in range(0, len(cycles_to_check), batch_size):
            batch = cycles_to_check[i:i + batch_size]
            # Check cycles in parallel
            tasks = [self.check_cycle_availability(cycle) for cycle in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for cycle, is_available in zip(batch, results):
                if isinstance(is_available, bool) and is_available:
                    available_cycles.append(cycle)
                elif isinstance(is_available, Exception):
                    logger.debug(f"Exception checking cycle {cycle}: {is_available}")
        
        logger.info(f"Found {len(available_cycles)} cycles with available bulk data: {available_cycles}")
        return sorted(available_cycles, reverse=True)  # Most recent first
    
    async def download_schedule_a_csv(self, cycle: int, job_id: Optional[str] = None) -> Optional[str]:
        """Download Schedule A CSV for a specific cycle with progress tracking"""
        url = self.get_latest_csv_url(cycle)
        zip_path = self.bulk_data_dir / f"indiv{cycle}.zip"
        extracted_path = self.bulk_data_dir / f"schedule_a_{cycle}.txt"
        
        logger.info(f"Downloading Schedule A ZIP for cycle {cycle} from {url}")
        
        try:
            # Stream the download directly with redirect following enabled
            # httpx will automatically follow redirects with follow_redirects=True
            async with self.client.stream('GET', url, follow_redirects=True) as response:
                # Check for 404 - this can happen if the file doesn't exist on S3 after redirect
                if response.status_code == 404:
                    # Check if we were redirected
                    final_url = str(response.url)
                    if final_url != url:
                        logger.warning(
                            f"ZIP file not found for cycle {cycle} at redirected URL {final_url} "
                            f"(original: {url}). The data may not be available yet for this cycle."
                        )
                    else:
                        logger.warning(f"ZIP file not found for cycle {cycle} at {url}")
                    
                    if job_id:
                        await self._update_job_progress(
                            job_id,
                            status='failed',
                            error_message=f"File not found for cycle {cycle}. The data may not be available yet."
                        )
                    return None
                
                response.raise_for_status()
                
                total_size = int(response.headers.get("content-length", 0))
                downloaded_size = 0
                
                # Stream download to ZIP file
                with open(zip_path, 'wb') as f:
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
                logger.info(f"Downloaded {final_size_mb:.1f} MB ZIP to {zip_path}")
                
                # Extract itcont.txt from the ZIP file
                if job_id:
                    await self._update_job_progress(
                        job_id,
                        progress_data={"status": "extracting", "cycle": cycle}
                    )
                
                logger.info(f"Extracting itcont.txt from {zip_path}")
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    # The ZIP contains itcont.txt at the root
                    zip_ref.extract('itcont.txt', path=self.bulk_data_dir)
                    # Rename the extracted file to our standard naming
                    extracted_file = self.bulk_data_dir / 'itcont.txt'
                    if extracted_file.exists():
                        extracted_file.rename(extracted_path)
                        logger.info(f"Extracted and renamed to {extracted_path}")
                    else:
                        logger.error(f"itcont.txt not found in ZIP file {zip_path}")
                        if job_id:
                            await self._update_job_progress(
                                job_id,
                                status='failed',
                                error_message=f"itcont.txt not found in ZIP file for cycle {cycle}"
                            )
                        return None
                
                # Optionally remove the ZIP file to save space (keep it for now in case we need to re-extract)
                # os.remove(zip_path)
                
                return str(extracted_path)
                
        except httpx.HTTPStatusError as e:
            # Check if it's a 404 after redirect
            if e.response.status_code == 404:
                final_url = str(e.response.url)
                if final_url != url:
                    logger.warning(
                        f"ZIP file not found for cycle {cycle} at redirected URL {final_url}. "
                        f"The data may not be available yet for this cycle."
                    )
                else:
                    logger.warning(f"ZIP file not found for cycle {cycle} at {url}")
                
                if job_id:
                    await self._update_job_progress(
                        job_id,
                        status='failed',
                        error_message=f"File not found for cycle {cycle}. The data may not be available yet."
                    )
                return None
            
            logger.error(f"HTTP error downloading ZIP for cycle {cycle}: {e}")
            if job_id:
                await self._update_job_progress(job_id, status='failed', error_message=f"HTTP error: {str(e)}")
            return None
        except zipfile.BadZipFile as e:
            logger.error(f"Invalid ZIP file for cycle {cycle}: {e}", exc_info=True)
            if job_id:
                await self._update_job_progress(job_id, status='failed', error_message=f"Invalid ZIP file: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Error downloading or extracting ZIP for cycle {cycle}: {e}", exc_info=True)
            if job_id:
                await self._update_job_progress(job_id, status='failed', error_message=str(e))
            return None
    
    async def download_bulk_data_file(
        self,
        data_type: DataType,
        cycle: int,
        job_id: Optional[str] = None
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
                            # Extract specific file
                            zip_ref.extract(config.zip_internal_file, path=self.bulk_data_dir)
                            extracted_file = self.bulk_data_dir / config.zip_internal_file
                            if extracted_file.exists():
                                extracted_file.rename(extracted_path)
                                logger.info(f"Extracted and renamed to {extracted_path}")
                            else:
                                logger.error(f"{config.zip_internal_file} not found in ZIP file {download_path}")
                                if job_id:
                                    await self._update_job_progress(
                                        job_id,
                                        status='failed',
                                        error_message=f"{config.zip_internal_file} not found in ZIP file for {data_type.value} cycle {cycle}"
                                    )
                                return None
                        else:
                            # Extract all files to directory
                            extracted_path.mkdir(exist_ok=True)
                            zip_ref.extractall(extracted_path)
                            logger.info(f"Extracted all files to {extracted_path}")
                
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
                    df = pd.read_csv(header_path, nrows=0)  # Just read headers
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
                return {
                    "cycle": metadata.cycle,
                    "download_date": metadata.download_date,
                    "file_path": metadata.file_path,
                    "record_count": metadata.record_count,
                    "last_updated": metadata.last_updated
                }
        return None
    
    def _parse_date_vectorized(self, date_series: pd.Series) -> pd.Series:
        """Parse dates vectorized - handles MMDDYYYY and YYYYMMDD formats"""
        result = pd.Series([None] * len(date_series), dtype='object')
        
        # Convert to string and strip
        date_strs = date_series.astype(str).str.strip()
        
        # Filter valid 8-digit dates
        valid_mask = (date_strs.str.len() == 8) & date_strs.str.isdigit()
        
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
        batch_size: int = 50000
    ) -> int:
        """Parse CSV and store in Contribution table with optimized bulk inserts using vectorized operations"""
        logger.info(f"Parsing CSV file: {file_path}")
        
        # FEC Schedule A CSV columns (pipe-delimited, no headers)
        fec_columns = [
            'CMTE_ID', 'AMNDT_IND', 'RPT_TP', 'TRAN_ID', 'ENTITY_TP', 'NAME',
            'CITY', 'STATE', 'ZIP_CODE', 'EMPLOYER', 'OCCUPATION', 'TRANSACTION_DT',
            'TRANSACTION_AMT', 'OTHER_ID', 'CAND_ID', 'TRAN_TP', 'FILE_NUM',
            'MEMO_CD', 'MEMO_TEXT', 'SUB_ID'
        ]
        
        try:
            chunk_count = 0
            total_records = 0
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
                    progress_data={"status": "parsing", "cycle": cycle}
                )
            
            async with AsyncSessionLocal() as session:
                # Read CSV in chunks - optimized for memory
                for chunk in pd.read_csv(
                    file_path,
                    sep='|',
                    header=None,
                    names=fec_columns,
                    chunksize=batch_size,
                    dtype=str,
                    low_memory=False,
                    on_bad_lines='skip'
                ):
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
                    chunk['contributor_name'] = clean_str_field(chunk['NAME'])
                    chunk['contributor_city'] = clean_str_field(chunk['CITY'])
                    chunk['contributor_state'] = clean_str_field(chunk['STATE'])
                    chunk['contributor_zip'] = clean_str_field(chunk['ZIP_CODE'])
                    chunk['contributor_employer'] = clean_str_field(chunk['EMPLOYER'])
                    chunk['contributor_occupation'] = clean_str_field(chunk['OCCUPATION'])
                    chunk['contribution_type'] = clean_str_field(chunk['TRAN_TP'])
                    
                    # Vectorized amount parsing
                    chunk['contribution_amount'] = pd.to_numeric(
                        chunk['TRANSACTION_AMT'].astype(str).str.replace('$', '', regex=False).str.replace(',', '', regex=False).str.strip(),
                        errors='coerce'
                    ).fillna(0.0)
                    
                    # Vectorized date parsing
                    chunk['contribution_date'] = self._parse_date_vectorized(chunk['TRANSACTION_DT'])
                    
                    # Build raw_data more efficiently - prepare columns for raw_data dict
                    # Convert to records list using vectorized operations
                    records_df = chunk[[
                        'contribution_id', 'candidate_id', 'committee_id', 'contributor_name',
                        'contributor_city', 'contributor_state', 'contributor_zip',
                        'contributor_employer', 'contributor_occupation', 'contribution_amount',
                        'contribution_date', 'contribution_type'
                    ]].copy()
                    
                    # Convert to dict records
                    records = records_df.to_dict('records')
                    
                    # Build raw_data efficiently using vectorized operations
                    # Create a DataFrame with just the raw_data fields
                    raw_data_df = pd.DataFrame({
                        'SUB_ID': chunk['SUB_ID'].astype(str).where(chunk['SUB_ID'].notna(), None),
                        'CAND_ID': chunk['CAND_ID'].astype(str).where(chunk['CAND_ID'].notna(), None),
                        'CMTE_ID': chunk['CMTE_ID'].astype(str).where(chunk['CMTE_ID'].notna(), None),
                        'NAME': chunk['NAME'].astype(str).where(chunk['NAME'].notna(), None),
                        'TRANSACTION_AMT': chunk['TRANSACTION_AMT'].astype(str).where(chunk['TRANSACTION_AMT'].notna(), None),
                        'TRANSACTION_DT': chunk['TRANSACTION_DT'].astype(str).where(chunk['TRANSACTION_DT'].notna(), None),
                    })
                    
                    # Convert to dict records and add to main records
                    raw_data_records = raw_data_df.to_dict('records')
                    for i, record in enumerate(records):
                        record['raw_data'] = raw_data_records[i]
                    
                    # Bulk insert with ON CONFLICT handling (SQLite)
                    if records:
                        try:
                            # Use bulk_insert_mappings for better performance
                            await session.execute(
                                text("""
                                    INSERT OR IGNORE INTO contributions 
                                    (contribution_id, candidate_id, committee_id, contributor_name, 
                                     contributor_city, contributor_state, contributor_zip, 
                                     contributor_employer, contributor_occupation, contribution_amount,
                                     contribution_date, contribution_type, raw_data, created_at)
                                    VALUES 
                                    (:contribution_id, :candidate_id, :committee_id, :contributor_name,
                                     :contributor_city, :contributor_state, :contributor_zip,
                                     :contributor_employer, :contributor_occupation, :contribution_amount,
                                     :contribution_date, :contribution_type, :raw_data, :created_at)
                                """),
                                [
                                    {
                                        **r,
                                        'raw_data': json.dumps(r['raw_data']),
                                        'created_at': datetime.utcnow()
                                    }
                                    for r in records
                                ]
                            )
                            await session.commit()
                            
                            inserted = len(records)
                            total_records += inserted
                            
                            # Update progress every 5 chunks to reduce overhead
                            if job_id and chunk_count % 5 == 0:
                                await self._update_job_progress(
                                    job_id,
                                    current_chunk=chunk_count,
                                    imported_records=total_records,
                                    progress_data={
                                        "status": "importing",
                                        "cycle": cycle,
                                        "chunks_processed": chunk_count,
                                        "records_imported": total_records,
                                        "records_skipped": skipped_duplicates
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
                            # Fallback: try individual inserts for this chunk
                            for record in records:
                                try:
                                    await session.execute(
                                        text("""
                                            INSERT OR IGNORE INTO contributions 
                                            (contribution_id, candidate_id, committee_id, contributor_name, 
                                             contributor_city, contributor_state, contributor_zip, 
                                             contributor_employer, contributor_occupation, contribution_amount,
                                             contribution_date, contribution_type, raw_data, created_at)
                                            VALUES 
                                            (:contribution_id, :candidate_id, :committee_id, :contributor_name,
                                             :contributor_city, :contributor_state, :contributor_zip,
                                             :contributor_employer, :contributor_occupation, :contribution_amount,
                                             :contribution_date, :contribution_type, :raw_data, :created_at)
                                        """),
                                        {
                                            **record,
                                            'raw_data': json.dumps(record['raw_data']),
                                            'created_at': datetime.utcnow()
                                        }
                                    )
                                    await session.commit()
                                    total_records += 1
                                except Exception:
                                    await session.rollback()
                                    skipped_duplicates += 1
                    
                    # Clear memory explicitly
                    del chunk, records
                    gc.collect()
                    
            logger.info(
                f"CSV import complete: {total_records} records imported, "
                f"{skipped_duplicates} duplicates skipped"
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
                    completed_at=datetime.utcnow()
                )
            
            return total_records
        except Exception as e:
            logger.error(f"Error parsing CSV file {file_path}: {e}", exc_info=True)
            if job_id:
                await self._update_job_progress(job_id, status='failed', error_message=str(e))
            raise
    
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
                committee_ids = [row[0] for row in result if row[0]]
                
                if not committee_ids:
                    return
                
                logger.info(f"Found {len(committee_ids)} unique committee IDs in contributions")
                
                # Check which committees we already have
                existing_result = await session.execute(
                    select(Committee.committee_id).where(
                        Committee.committee_id.in_(committee_ids)
                    )
                )
                existing_ids = {row[0] for row in existing_result}
                missing_ids = [cid for cid in committee_ids if cid not in existing_ids]
                
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
    
    async def get_available_cycles(self, use_fec_api: bool = True) -> List[Dict]:
        """Get list of cycles with available bulk data
        
        Args:
            use_fec_api: If True, query FEC API to check which cycles actually have data.
                        If False or API check fails, falls back to all even years 2000-current+6
        """
        # Try to get available cycles from FEC API
        fec_available_cycles = []
        if use_fec_api:
            try:
                fec_available_cycles = await self.get_available_cycles_from_fec()
                logger.info(f"FEC API check found {len(fec_available_cycles)} available cycles")
            except Exception as e:
                logger.warning(f"Failed to check FEC API for available cycles, falling back to default: {e}")
                # Fall back to generating all even years
                current_year = datetime.now().year
                future_years = current_year + 6
                fec_available_cycles = list(range(2000, future_years + 1, 2))
        else:
            # Generate all possible cycles from 2000 to current year + 6 (even years)
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
        batch_size: int = 50000
    ) -> Dict[str, Any]:
        """
        Download and import a specific data type for a cycle
        
        Returns dict with success status, record count, and any errors
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
                    progress_data={"status": "downloading", "cycle": cycle, "data_type": data_type.value}
                )
            
            # Download file
            file_path = await self.download_bulk_data_file(data_type, cycle, job_id=job_id)
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
                    progress_data={"status": "parsing", "cycle": cycle, "data_type": data_type.value}
                )
            
            # Parse and store
            logger.info(f"Parsing and storing {data_type.value} for cycle {cycle}")
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
                else:
                    metadata = BulkDataMetadata(
                        cycle=cycle,
                        data_type=data_type.value,
                        download_date=datetime.utcnow(),
                        file_path=file_path,
                        record_count=record_count,
                        last_updated=datetime.utcnow()
                    )
                    session.add(metadata)
                
                await session.commit()
            
            logger.info(f"Successfully imported {record_count} records for {data_type.value}, cycle {cycle}")
            
            # Update status to imported
            await self.update_data_type_status(data_type, cycle, 'imported', record_count=record_count)
            
            if job_id:
                await self._update_job_progress(
                    job_id,
                    status='completed',
                    imported_records=record_count,
                    completed_at=datetime.utcnow(),
                    progress_data={"status": "completed", "cycle": cycle, "data_type": data_type.value}
                )
            
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
    
    async def _update_job_progress(
        self,
        job_id: str,
        status: Optional[str] = None,
        current_cycle: Optional[int] = None,
        current_chunk: Optional[int] = None,
        total_chunks: Optional[int] = None,
        imported_records: Optional[int] = None,
        skipped_records: Optional[int] = None,
        error_message: Optional[str] = None,
        completed_at: Optional[datetime] = None,
        progress_data: Optional[Dict] = None
    ):
        """Update job progress in database"""
        try:
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    select(BulkImportJob).where(BulkImportJob.id == job_id)
                )
                job = result.scalar_one_or_none()
                
                if job:
                    if status:
                        job.status = status
                    if current_cycle is not None:
                        job.current_cycle = current_cycle
                    if current_chunk is not None:
                        job.current_chunk = current_chunk
                    if total_chunks is not None:
                        job.total_chunks = total_chunks
                    if imported_records is not None:
                        job.imported_records = imported_records
                    if skipped_records is not None:
                        job.skipped_records = skipped_records
                    if error_message:
                        job.error_message = error_message
                    if completed_at:
                        job.completed_at = completed_at
                    if progress_data:
                        # Merge with existing progress_data
                        existing = job.progress_data or {}
                        existing.update(progress_data)
                        job.progress_data = existing
                    
                    await session.commit()
        except Exception as e:
            logger.warning(f"Error updating job progress: {e}")
    
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
    
    async def create_job(
        self,
        job_type: str,
        cycle: Optional[int] = None,
        cycles: Optional[List[int]] = None
    ) -> str:
        """Create a new import job and return job_id"""
        job_id = str(uuid.uuid4())
        
        async with AsyncSessionLocal() as session:
            job = BulkImportJob(
                id=job_id,
                job_type=job_type,
                status='pending',
                cycle=cycle,
                cycles=cycles,
                total_cycles=len(cycles) if cycles else (1 if cycle else 0),
                progress_data={}
            )
            session.add(job)
            await session.commit()
        
        return job_id
    
    async def get_job(self, job_id: str) -> Optional[BulkImportJob]:
        """Get job by ID"""
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(BulkImportJob).where(BulkImportJob.id == job_id)
            )
            return result.scalar_one_or_none()
    
    async def get_data_type_status(self, cycle: int, data_type: DataType) -> Optional[BulkDataImportStatus]:
        """Get import status for a specific data type and cycle"""
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(BulkDataImportStatus).where(
                    and_(
                        BulkDataImportStatus.data_type == data_type.value,
                        BulkDataImportStatus.cycle == cycle
                    )
                )
            )
            return result.scalar_one_or_none()
    
    async def get_all_data_type_statuses(self, cycle: int) -> Dict[str, BulkDataImportStatus]:
        """Get import status for all data types for a cycle"""
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(BulkDataImportStatus).where(
                    BulkDataImportStatus.cycle == cycle
                )
            )
            statuses = result.scalars().all()
            return {status.data_type: status for status in statuses}
    
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
        global _cancelled_jobs
        _cancelled_jobs.add(job_id)
        
        try:
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    select(BulkImportJob).where(BulkImportJob.id == job_id)
                )
                job = result.scalar_one_or_none()
                
                if job and job.status == 'running':
                    job.status = 'cancelled'
                    job.completed_at = datetime.utcnow()
                    await session.commit()
                    return True
        except Exception as e:
            logger.error(f"Error cancelling job: {e}")
        
        return False
    
    async def import_multiple_data_types(
        self,
        cycle: int,
        data_types: List[DataType],
        job_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Import multiple data types for a cycle"""
        results = {}
        for data_type in data_types:
            try:
                result = await self.download_and_import_data_type(data_type, cycle, job_id=job_id)
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
        await self.client.aclose()

