"""
Job management for bulk data imports
"""
import asyncio
import logging
from typing import Optional, Set, List
from datetime import datetime
from sqlalchemy import select, and_, desc
from app.db.database import AsyncSessionLocal, BulkImportJob, BulkDataImportStatus
from app.services.bulk_data_config import DataType

logger = logging.getLogger(__name__)

# Global set to track cancelled jobs
_cancelled_jobs: Set[str] = set()

# Global set to track running background tasks for graceful shutdown
_running_tasks: Set[asyncio.Task] = set()


class JobManager:
    """Manages bulk import job tracking and status"""
    
    def __init__(self, cancelled_jobs: Optional[Set[str]] = None):
        """
        Initialize job manager
        
        Args:
            cancelled_jobs: Optional set of cancelled job IDs (uses global if None)
        """
        self._cancelled_jobs = cancelled_jobs if cancelled_jobs is not None else _cancelled_jobs
    
    async def create_job(
        self,
        job_type: str,
        cycle: Optional[int] = None,
        cycles: Optional[List[int]] = None,
        data_type: Optional[str] = None
    ) -> BulkImportJob:
        """Create a new bulk import job"""
        async with AsyncSessionLocal() as session:
            job = BulkImportJob(
                id=f"{job_type}_{datetime.utcnow().timestamp()}",
                job_type=job_type,
                cycle=cycle,
                cycles=cycles,
                data_type=data_type,
                status='pending',
                total_cycles=len(cycles) if cycles else (1 if cycle else 0)
            )
            session.add(job)
            await session.commit()
            await session.refresh(job)
            return job
    
    async def get_job(self, job_id: str) -> Optional[BulkImportJob]:
        """Get a job by ID"""
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(BulkImportJob).where(BulkImportJob.id == job_id)
            )
            return result.scalar_one_or_none()
    
    async def update_job_progress(
        self,
        job_id: str,
        status: Optional[str] = None,
        imported_records: Optional[int] = None,
        skipped_records: Optional[int] = None,
        current_cycle: Optional[int] = None,
        completed_cycles: Optional[int] = None,
        current_chunk: Optional[int] = None,
        total_chunks: Optional[int] = None,
        file_position: Optional[int] = None,
        progress_data: Optional[dict] = None,
        error_message: Optional[str] = None
    ):
        """Update job progress"""
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(BulkImportJob).where(BulkImportJob.id == job_id)
            )
            job = result.scalar_one_or_none()
            if not job:
                logger.warning(f"Job {job_id} not found for progress update")
                return
            
            if status:
                job.status = status
            if imported_records is not None:
                job.imported_records = imported_records
            if skipped_records is not None:
                job.skipped_records = skipped_records
            if current_cycle is not None:
                job.current_cycle = current_cycle
            if completed_cycles is not None:
                job.completed_cycles = completed_cycles
            if current_chunk is not None:
                job.current_chunk = current_chunk
            if total_chunks is not None:
                job.total_chunks = total_chunks
            if file_position is not None:
                job.file_position = file_position
            if progress_data is not None:
                job.progress_data = progress_data
            if error_message is not None:
                job.error_message = error_message
            
            if status in ['completed', 'failed', 'cancelled']:
                job.completed_at = datetime.utcnow()
            
            await session.commit()
    
    async def cancel_job(self, job_id: str) -> bool:
        """Cancel a job"""
        self._cancelled_jobs.add(job_id)
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(BulkImportJob).where(BulkImportJob.id == job_id)
            )
            job = result.scalar_one_or_none()
            if job and job.status in ['pending', 'running']:
                job.status = 'cancelled'
                job.completed_at = datetime.utcnow()
                await session.commit()
                return True
            return False
    
    async def get_incomplete_jobs(self) -> List[BulkImportJob]:
        """Get all incomplete jobs"""
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(BulkImportJob).where(
                    BulkImportJob.status.in_(['pending', 'running'])
                ).order_by(desc(BulkImportJob.started_at))
            )
            return list(result.scalars().all())
    
    async def get_recent_jobs(self, limit: int = 10) -> List[BulkImportJob]:
        """Get recent jobs"""
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(BulkImportJob).order_by(desc(BulkImportJob.started_at)).limit(limit)
            )
            return list(result.scalars().all())
    
    def is_job_cancelled(self, job_id: str) -> bool:
        """Check if a job is cancelled"""
        return job_id in self._cancelled_jobs

