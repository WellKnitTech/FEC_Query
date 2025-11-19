#!/usr/bin/env python3
"""
Count records in database tables for bulk data imports.

This module counts records in database tables and metadata to verify import accuracy.
"""
import logging
from typing import Dict, Optional
from sqlalchemy import select, func, text
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
    BulkDataMetadata,
    BulkDataImportStatus,
)
from app.services.bulk_data_config import DataType

logger = logging.getLogger(__name__)


class DatabaseCounter:
    """Count records in database tables and metadata"""
    
    async def count_table_records(
        self,
        session: AsyncSession,
        data_type: DataType,
        cycle: Optional[int] = None
    ) -> int:
        """
        Count records in the appropriate table for a data type
        
        Args:
            session: Database session
            data_type: Data type to count
            cycle: Optional cycle filter
        
        Returns:
            Number of records
        """
        if data_type == DataType.CANDIDATE_MASTER:
            return await self._count_candidates(session, cycle)
        elif data_type == DataType.COMMITTEE_MASTER:
            return await self._count_committees(session, cycle)
        elif data_type == DataType.CANDIDATE_COMMITTEE_LINKAGE:
            return await self._count_linkages(session, cycle)
        elif data_type == DataType.INDIVIDUAL_CONTRIBUTIONS:
            return await self._count_contributions(session, cycle)
        elif data_type == DataType.INDEPENDENT_EXPENDITURES:
            return await self._count_independent_expenditures(session, cycle)
        elif data_type == DataType.OPERATING_EXPENDITURES:
            return await self._count_operating_expenditures(session, cycle)
        elif data_type == DataType.CANDIDATE_SUMMARY:
            return await self._count_candidate_summaries(session, cycle)
        elif data_type == DataType.COMMITTEE_SUMMARY:
            return await self._count_committee_summaries(session, cycle)
        elif data_type == DataType.ELECTIONEERING_COMM:
            return await self._count_electioneering_comm(session, cycle)
        elif data_type == DataType.COMMUNICATION_COSTS:
            return await self._count_communication_costs(session, cycle)
        elif data_type in [DataType.PAC_SUMMARY, DataType.OTHER_TRANSACTIONS, DataType.PAS2]:
            # These are stored in metadata only
            return await self._count_metadata_records(session, data_type, cycle)
        else:
            logger.warning(f"Unknown data type: {data_type.value}")
            return 0
    
    async def _count_candidates(self, session: AsyncSession, cycle: Optional[int]) -> int:
        """Count candidates in database"""
        result = await session.execute(select(func.count(Candidate.id)))
        return result.scalar() or 0
    
    async def _count_committees(self, session: AsyncSession, cycle: Optional[int]) -> int:
        """Count committees in database"""
        result = await session.execute(select(func.count(Committee.id)))
        return result.scalar() or 0
    
    async def _count_linkages(self, session: AsyncSession, cycle: Optional[int]) -> int:
        """
        Count candidate-committee linkages
        
        This counts the total number of candidate_id entries across all committees'
        candidate_ids JSON arrays.
        """
        # Get all committees with candidate_ids
        result = await session.execute(
            select(Committee.committee_id, Committee.candidate_ids)
        )
        committees = result.all()
        
        total_linkages = 0
        for comm in committees:
            if comm.candidate_ids and isinstance(comm.candidate_ids, list):
                # Count unique candidate IDs for this committee
                unique_candidates = len(set(cid for cid in comm.candidate_ids if cid and cid.strip()))
                total_linkages += unique_candidates
        
        return total_linkages
    
    async def _count_contributions(self, session: AsyncSession, cycle: Optional[int]) -> int:
        """Count contributions in database"""
        query = select(func.count(Contribution.id))
        if cycle:
            # Filter by cycle using contribution_date
            from datetime import datetime
            cycle_start = datetime(cycle - 1, 1, 1)
            cycle_end = datetime(cycle + 1, 12, 31)
            query = query.where(
                Contribution.contribution_date >= cycle_start,
                Contribution.contribution_date <= cycle_end
            )
        result = await session.execute(query)
        return result.scalar() or 0
    
    async def _count_independent_expenditures(
        self,
        session: AsyncSession,
        cycle: Optional[int]
    ) -> int:
        """Count independent expenditures"""
        query = select(func.count(IndependentExpenditure.id))
        if cycle:
            query = query.where(IndependentExpenditure.cycle == cycle)
        result = await session.execute(query)
        return result.scalar() or 0
    
    async def _count_operating_expenditures(
        self,
        session: AsyncSession,
        cycle: Optional[int]
    ) -> int:
        """Count operating expenditures"""
        query = select(func.count(OperatingExpenditure.id))
        if cycle:
            query = query.where(OperatingExpenditure.cycle == cycle)
        result = await session.execute(query)
        return result.scalar() or 0
    
    async def _count_candidate_summaries(
        self,
        session: AsyncSession,
        cycle: Optional[int]
    ) -> int:
        """Count candidate summaries"""
        query = select(func.count(CandidateSummary.id))
        if cycle:
            query = query.where(CandidateSummary.cycle == cycle)
        result = await session.execute(query)
        return result.scalar() or 0
    
    async def _count_committee_summaries(
        self,
        session: AsyncSession,
        cycle: Optional[int]
    ) -> int:
        """Count committee summaries"""
        query = select(func.count(CommitteeSummary.id))
        if cycle:
            query = query.where(CommitteeSummary.cycle == cycle)
        result = await session.execute(query)
        return result.scalar() or 0
    
    async def _count_electioneering_comm(
        self,
        session: AsyncSession,
        cycle: Optional[int]
    ) -> int:
        """Count electioneering communications"""
        query = select(func.count(ElectioneeringComm.id))
        if cycle:
            query = query.where(ElectioneeringComm.cycle == cycle)
        result = await session.execute(query)
        return result.scalar() or 0
    
    async def _count_communication_costs(
        self,
        session: AsyncSession,
        cycle: Optional[int]
    ) -> int:
        """Count communication costs"""
        query = select(func.count(CommunicationCost.id))
        if cycle:
            query = query.where(CommunicationCost.cycle == cycle)
        result = await session.execute(query)
        return result.scalar() or 0
    
    async def _count_metadata_records(
        self,
        session: AsyncSession,
        data_type: DataType,
        cycle: Optional[int]
    ) -> int:
        """
        Count records from metadata (for data types stored only in metadata)
        
        This includes: pac_summary, other_transactions, pas2
        """
        query = select(BulkDataMetadata.record_count).where(
            BulkDataMetadata.data_type == data_type.value
        )
        if cycle:
            query = query.where(BulkDataMetadata.cycle == cycle)
        
        result = await session.execute(query)
        records = result.scalars().all()
        return sum(record_count or 0 for record_count in records)
    
    async def get_metadata_count(
        self,
        session: AsyncSession,
        data_type: DataType,
        cycle: Optional[int]
    ) -> Optional[int]:
        """
        Get record count from BulkDataMetadata table
        
        Returns:
            Record count from metadata, or None if not found
        """
        query = select(BulkDataMetadata.record_count).where(
            BulkDataMetadata.data_type == data_type.value,
            BulkDataMetadata.imported == True
        )
        if cycle:
            query = query.where(BulkDataMetadata.cycle == cycle)
        
        query = query.order_by(BulkDataMetadata.download_date.desc()).limit(1)
        result = await session.execute(query)
        record_count = result.scalar_one_or_none()
        return record_count
    
    async def get_import_status_count(
        self,
        session: AsyncSession,
        data_type: DataType,
        cycle: Optional[int]
    ) -> Optional[int]:
        """
        Get record count from BulkDataImportStatus table
        
        Returns:
            Record count from import status, or None if not found
        """
        query = select(BulkDataImportStatus.record_count).where(
            BulkDataImportStatus.data_type == data_type.value
        )
        if cycle:
            query = query.where(BulkDataImportStatus.cycle == cycle)
        
        query = query.order_by(BulkDataImportStatus.updated_at.desc()).limit(1)
        result = await session.execute(query)
        record_count = result.scalar_one_or_none()
        return record_count
    
    async def count_all_tables(self, cycle: Optional[int] = None) -> Dict[str, int]:
        """
        Count records in all tables for a cycle
        
        Args:
            cycle: Optional cycle filter
        
        Returns:
            Dictionary mapping data_type.value to record count
        """
        results = {}
        
        async with AsyncSessionLocal() as session:
            for data_type in DataType:
                count = await self.count_table_records(session, data_type, cycle)
                results[data_type.value] = count
                logger.info(f"Counted {count:,} records in database for {data_type.value}")
        
        return results
    
    async def get_all_metadata_counts(
        self,
        cycle: Optional[int] = None
    ) -> Dict[str, Optional[int]]:
        """
        Get record counts from metadata for all data types
        
        Returns:
            Dictionary mapping data_type.value to metadata record count
        """
        results = {}
        
        async with AsyncSessionLocal() as session:
            for data_type in DataType:
                count = await self.get_metadata_count(session, data_type, cycle)
                results[data_type.value] = count
        
        return results
    
    async def get_all_status_counts(
        self,
        cycle: Optional[int] = None
    ) -> Dict[str, Optional[int]]:
        """
        Get record counts from import status for all data types
        
        Returns:
            Dictionary mapping data_type.value to status record count
        """
        results = {}
        
        async with AsyncSessionLocal() as session:
            for data_type in DataType:
                count = await self.get_import_status_count(session, data_type, cycle)
                results[data_type.value] = count
        
        return results


async def main():
    """Test the database counter"""
    import asyncio
    import sys
    
    cycle = int(sys.argv[1]) if len(sys.argv) > 1 else None
    
    counter = DatabaseCounter()
    results = await counter.count_all_tables(cycle)
    
    print(f"\nDatabase record counts{' for cycle ' + str(cycle) if cycle else ''}:")
    print("=" * 80)
    for data_type, count in sorted(results.items()):
        print(f"{data_type:<40} {count:>15,}")


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())

