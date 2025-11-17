"""
Storage operations for bulk data metadata
"""
import logging
from typing import Optional, Dict, List
from datetime import datetime
from sqlalchemy import select, and_, desc
from app.db.database import AsyncSessionLocal, BulkDataMetadata, BulkDataImportStatus
from app.services.bulk_data_config import DataType

logger = logging.getLogger(__name__)


class BulkDataStorage:
    """Manages bulk data metadata storage operations"""
    
    async def check_csv_freshness(self, cycle: int) -> Optional[Dict]:
        """Check if CSV data is fresh for a cycle"""
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(BulkDataMetadata).where(
                    and_(
                        BulkDataMetadata.cycle == cycle,
                        BulkDataMetadata.data_type == "schedule_a"
                    )
                ).order_by(desc(BulkDataMetadata.download_date))
            )
            metadata = result.scalar_one_or_none()
            if metadata:
                return {
                    "cycle": metadata.cycle,
                    "data_type": metadata.data_type,
                    "download_date": metadata.download_date.isoformat() if metadata.download_date else None,
                    "file_path": metadata.file_path,
                    "file_size": metadata.file_size,
                    "record_count": metadata.record_count,
                    "last_updated": metadata.last_updated.isoformat() if metadata.last_updated else None
                }
            return None
    
    async def get_data_type_status(
        self,
        cycle: int,
        data_type: DataType
    ) -> Optional[BulkDataImportStatus]:
        """Get import status for a data type and cycle"""
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
        """Get all data type statuses for a cycle"""
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(BulkDataImportStatus).where(
                    BulkDataImportStatus.cycle == cycle
                )
            )
            statuses = result.scalars().all()
            return {status.data_type: status for status in statuses}
    
    async def get_available_cycles_from_db(self) -> Optional[List[int]]:
        """Get available cycles from database"""
        async with AsyncSessionLocal() as session:
            from app.db.database import AvailableCycle
            result = await session.execute(
                select(AvailableCycle.cycle).order_by(desc(AvailableCycle.cycle))
            )
            cycles = [row[0] for row in result.fetchall()]
            return cycles if cycles else None

