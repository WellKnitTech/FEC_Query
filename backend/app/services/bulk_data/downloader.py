"""
File downloading for bulk data
"""
import httpx
import zipfile
import logging
from pathlib import Path
from typing import Optional, Set
from app.services.bulk_data_config import DataType, get_config

logger = logging.getLogger(__name__)


class BulkDataDownloader:
    """Handles downloading of bulk data files"""
    
    def __init__(
        self,
        bulk_data_dir: Path,
        base_url: str,
        cancelled_jobs: Set[str],
        timeout: float = 300.0
    ):
        """
        Initialize bulk data downloader
        
        Args:
            bulk_data_dir: Directory for storing downloaded files
            base_url: Base URL for FEC bulk downloads
            cancelled_jobs: Set of cancelled job IDs
            timeout: Request timeout in seconds
        """
        self.bulk_data_dir = bulk_data_dir
        self.base_url = base_url
        self.cancelled_jobs = cancelled_jobs
        self.client = httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=True
        )
    
    def get_latest_csv_url(self, cycle: int) -> str:
        """Get FEC bulk data URL for Schedule A CSV for a specific cycle (legacy method)"""
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
            if response.status_code == 302:
                redirect_url = response.headers.get('Location')
                if redirect_url:
                    # Make redirect URL absolute if needed
                    if not redirect_url.startswith('http'):
                        from urllib.parse import urljoin
                        redirect_url = urljoin(url, redirect_url)
                    
                    # Try a GET request to the redirect URL to verify it actually works
                    try:
                        get_response = await self.client.get(
                            redirect_url,
                            headers={'Range': 'bytes=0-0'},
                            timeout=10.0,
                            follow_redirects=True
                        )
                        return get_response.status_code in [200, 206]
                    except httpx.HTTPStatusError as get_error:
                        if get_error.response.status_code == 404:
                            return False
                        return True
                    except Exception:
                        return True
                return True
            
            # 200 means file exists
            if response.status_code == 200:
                return True
            
            # 404 means not available
            return False
            
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return False
            logger.debug(f"HTTP error checking availability for cycle {cycle}: {e}")
            return False
        except Exception as e:
            logger.debug(f"Error checking availability for cycle {cycle}: {e}")
            return False
    
    async def download_schedule_a_csv(
        self,
        cycle: int,
        job_id: Optional[str] = None,
        update_progress_func: Optional[callable] = None
    ) -> Optional[str]:
        """Download Schedule A CSV for a specific cycle with progress tracking"""
        url = self.get_latest_csv_url(cycle)
        zip_path = self.bulk_data_dir / f"indiv{cycle}.zip"
        extracted_path = self.bulk_data_dir / f"schedule_a_{cycle}.txt"
        
        logger.info(f"Downloading Schedule A ZIP for cycle {cycle} from {url}")
        
        try:
            async with self.client.stream('GET', url, follow_redirects=True) as response:
                if response.status_code == 404:
                    logger.warning(f"ZIP file not found for cycle {cycle} at {url}")
                    if update_progress_func:
                        await update_progress_func(
                            job_id,
                            status='failed',
                            error_message=f"File not found for cycle {cycle}"
                        )
                    return None
                
                response.raise_for_status()
                
                total_size = int(response.headers.get("content-length", 0))
                downloaded_size = 0
                
                # Stream download to ZIP file
                with open(zip_path, 'wb') as f:
                    async for chunk in response.aiter_bytes():
                        if job_id and job_id in self.cancelled_jobs:
                            logger.info(f"Download cancelled for job {job_id}")
                            return None
                        
                        f.write(chunk)
                        downloaded_size += len(chunk)
                        
                        # Update progress every 10MB
                        if downloaded_size % (10 * 1024 * 1024) == 0:
                            downloaded_mb = downloaded_size / (1024 * 1024)
                            if update_progress_func:
                                await update_progress_func(
                                    job_id,
                                    cycle,
                                    downloaded_mb,
                                    total_size / (1024 * 1024) if total_size > 0 else None
                                )
                
                # Extract itcont.txt from the ZIP file
                logger.info(f"Extracting itcont.txt from {zip_path}")
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extract('itcont.txt', path=self.bulk_data_dir)
                    extracted_file = self.bulk_data_dir / 'itcont.txt'
                    if extracted_file.exists():
                        extracted_file.rename(extracted_path)
                        logger.info(f"Extracted and renamed to {extracted_path}")
                    else:
                        logger.error(f"itcont.txt not found in ZIP file {zip_path}")
                        if update_progress_func:
                            await update_progress_func(
                                job_id,
                                status='failed',
                                error_message=f"itcont.txt not found in ZIP file for cycle {cycle}"
                            )
                        return None
                
                return str(extracted_path)
                
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error downloading cycle {cycle}: {e}")
            if update_progress_func:
                await update_progress_func(
                    job_id,
                    status='failed',
                    error_message=f"HTTP error: {e.response.status_code}"
                )
            return None
        except Exception as e:
            logger.error(f"Error downloading cycle {cycle}: {e}", exc_info=True)
            if update_progress_func:
                await update_progress_func(
                    job_id,
                    status='failed',
                    error_message=str(e)
                )
            return None
    
    async def close(self):
        """Close the HTTP client"""
        await self.client.aclose()

