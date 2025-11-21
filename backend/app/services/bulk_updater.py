import asyncio
import os
import logging
from typing import Optional, Dict, List
from datetime import datetime, timedelta
from app.services.bulk_data import BulkDataService
from app.db.database import AsyncSessionLocal, BulkDataMetadata, Contribution
from sqlalchemy import select, and_, func

logger = logging.getLogger(__name__)


class BulkUpdaterService:
    """Background service for updating bulk CSV data"""
    
    def __init__(self):
        self.bulk_data_service = BulkDataService()
        self.update_interval_hours = int(os.getenv("BULK_DATA_UPDATE_INTERVAL_HOURS", "24"))
        self.max_concurrent_cycles = int(os.getenv("BULK_DATA_MAX_CONCURRENT_CYCLES", "2"))
        self._running = False
        self._task = None
        self._semaphore = asyncio.Semaphore(self.max_concurrent_cycles)
    
    async def download_and_import_cycle(self, cycle: int, job_id: Optional[str] = None) -> Dict:
        """Download and import CSV for a specific cycle with progress tracking"""
        async with self._semaphore:  # Limit concurrent cycles
            try:
                logger.info(f"Starting download and import for cycle {cycle}" + (f" (job {job_id})" if job_id else ""))
                
                if job_id:
                    try:
                        await self.bulk_data_service._update_job_progress(
                            job_id,
                            status='running',
                            current_cycle=cycle,
                            progress_data={"status": "downloading", "cycle": cycle}
                        )
                    except Exception as progress_error:
                        logger.warning(f"Failed to update job progress: {progress_error}")
                
                # Download CSV
                file_path = await self.bulk_data_service.download_schedule_a_csv(cycle, job_id=job_id)
                if not file_path:
                    error_msg = f"Failed to download CSV file for cycle {cycle}. The file may not be available yet for this cycle."
                    logger.error(error_msg)
                    if job_id:
                        try:
                            await self.bulk_data_service._update_job_progress(
                                job_id,
                                status='failed',
                                error_message=error_msg
                            )
                        except Exception as progress_error:
                            logger.warning(f"Failed to update job progress: {progress_error}", exc_info=True)
                    return {
                        "success": False,
                        "cycle": cycle,
                        "error": error_msg
                    }
                
                if job_id:
                    try:
                        await self.bulk_data_service._update_job_progress(
                            job_id,
                            progress_data={"status": "parsing", "cycle": cycle}
                        )
                    except Exception as progress_error:
                        logger.warning(f"Failed to update job progress: {progress_error}")
                
                # Parse and store
                record_count = await self.bulk_data_service.parse_and_store_csv(
                    file_path, cycle, job_id=job_id
                )
                
                logger.info(f"Successfully imported {record_count} records for cycle {cycle}" + (f" (job {job_id})" if job_id else ""))
                
                # Mark job as completed if we have a job_id
                if job_id:
                    try:
                        await self.bulk_data_service._update_job_progress(
                            job_id,
                            status='completed',
                            imported_records=record_count,
                            completed_at=datetime.utcnow(),
                            progress_data={"status": "completed", "cycle": cycle}
                        )
                    except Exception as progress_error:
                        logger.warning(f"Failed to update job progress: {progress_error}")
                
                return {
                    "success": True,
                    "cycle": cycle,
                    "record_count": record_count,
                    "file_path": file_path
                }
                
            except Exception as e:
                error_msg = f"Error downloading/importing cycle {cycle}: {str(e)}"
                logger.error(error_msg, exc_info=True)
                if job_id:
                    try:
                        await self.bulk_data_service._update_job_progress(
                            job_id,
                            status='failed',
                            error_message=error_msg
                        )
                    except Exception as progress_error:
                        logger.error(f"Failed to update job progress after error: {progress_error}", exc_info=True)
                return {
                    "success": False,
                    "cycle": cycle,
                    "error": error_msg
                }
    
    async def check_and_update_cycles(self, cycles: Optional[List[int]] = None, job_id: Optional[str] = None) -> List[Dict]:
        """Check for updates and download missing/stale cycles with parallel processing"""
        if cycles is None:
            # Default to current and recent cycles
            current_year = datetime.now().year
            current_cycle = ((current_year - 1) // 2) * 2 + 1
            cycles = [current_cycle, current_cycle - 2, current_cycle - 4]  # Current + 2 previous
        
        # Filter cycles that need updating
        cycles_to_update = []
        for cycle in cycles:
            try:
                metadata = await self.bulk_data_service.check_csv_freshness(cycle)
                
                if metadata:
                    # Check if update is needed
                    update_interval = timedelta(hours=self.update_interval_hours)
                    if metadata.get("last_updated"):
                        last_updated = metadata["last_updated"]
                        if isinstance(last_updated, str):
                            last_updated = datetime.fromisoformat(last_updated.replace('Z', '+00:00'))
                        if datetime.utcnow() - last_updated < update_interval:
                            logger.info(f"Cycle {cycle} is up to date, skipping")
                            continue
                
                cycles_to_update.append(cycle)
            except Exception as e:
                logger.error(f"Error checking cycle {cycle}: {e}")
        
        if not cycles_to_update:
            return []
        
        # Process cycles in parallel with semaphore limit
        async def process_cycle(cycle: int) -> Dict:
            try:
                return await self.download_and_import_cycle(cycle, job_id=job_id)
            except Exception as e:
                logger.error(f"Error processing cycle {cycle}: {e}", exc_info=True)
                return {
                    "success": False,
                    "cycle": cycle,
                    "error": str(e)
                }
        
        # Use gather with semaphore-controlled concurrency
        tasks = [process_cycle(cycle) for cycle in cycles_to_update]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Convert exceptions to error dicts
        final_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                final_results.append({
                    "success": False,
                    "cycle": cycles_to_update[i],
                    "error": str(result)
                })
            else:
                final_results.append(result)
        
        return final_results
    
    async def start_background_updates(self):
        """Start background task for periodic updates"""
        if self._running:
            logger.warning("Background updater is already running")
            return
        
        self._running = True
        self._task = asyncio.create_task(self._update_loop())
        logger.info("Background bulk data updater started")
    
    async def stop_background_updates(self):
        """Stop background update task"""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                logger.debug("Bulk updater task was cancelled")
        logger.info("Background bulk data updater stopped")
    
    async def _update_loop(self):
        """Background loop for periodic updates"""
        while self._running:
            try:
                await self.check_and_update_cycles()
            except Exception as e:
                logger.error(f"Error in background update loop: {e}", exc_info=True)
            
            # Wait for next update interval
            await asyncio.sleep(self.update_interval_hours * 3600)
    
    async def get_status(self) -> Dict:
        """Get current status of bulk data"""
        try:
            cycles = await self.bulk_data_service.get_available_cycles()
            
            # Get total record count
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    select(func.count(Contribution.id))
                )
                total_records = result.scalar() or 0
            
            return {
                "enabled": os.getenv("BULK_DATA_ENABLED", "true").lower() == "true",
                "available_cycles": cycles,
                "total_records": total_records,
                "update_interval_hours": self.update_interval_hours,
                "background_updates_running": self._running
            }
        except Exception as e:
            logger.error(f"Error getting bulk data status: {e}", exc_info=True)
            return {
                "enabled": False,
                "error": str(e)
            }
    
    async def cleanup(self):
        """Clean up resources"""
        await self.stop_background_updates()
        await self.bulk_data_service.cleanup()

