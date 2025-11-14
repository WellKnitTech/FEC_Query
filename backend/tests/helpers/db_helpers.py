"""
Database helper functions for tests
"""
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, distinct
from typing import Optional, Dict, Any, List
from app.db.database import (
    Contribution, Candidate, Committee, BulkDataMetadata,
    BulkImportJob, SavedSearch, FinancialTotal
)


async def get_contribution_count(
    session: AsyncSession,
    candidate_id: Optional[str] = None,
    committee_id: Optional[str] = None,
    **filters
) -> int:
    """Get count of contributions matching filters"""
    query = select(func.count()).select_from(Contribution)
    
    if candidate_id:
        query = query.where(Contribution.candidate_id == candidate_id)
    if committee_id:
        query = query.where(Contribution.committee_id == committee_id)
    
    for key, value in filters.items():
        if hasattr(Contribution, key):
            query = query.where(getattr(Contribution, key) == value)
    
    result = await session.execute(query)
    return result.scalar_one() or 0


async def get_contributions(
    session: AsyncSession,
    candidate_id: Optional[str] = None,
    committee_id: Optional[str] = None,
    limit: int = 100,
    **filters
) -> List[Contribution]:
    """Get contributions matching filters"""
    query = select(Contribution)
    
    if candidate_id:
        query = query.where(Contribution.candidate_id == candidate_id)
    if committee_id:
        query = query.where(Contribution.committee_id == committee_id)
    
    for key, value in filters.items():
        if hasattr(Contribution, key):
            query = query.where(getattr(Contribution, key) == value)
    
    query = query.limit(limit)
    result = await session.execute(query)
    return list(result.scalars().all())


async def get_candidate(session: AsyncSession, candidate_id: str) -> Optional[Candidate]:
    """Get candidate by ID"""
    query = select(Candidate).where(Candidate.candidate_id == candidate_id)
    result = await session.execute(query)
    return result.scalar_one_or_none()


async def get_committee(session: AsyncSession, committee_id: str) -> Optional[Committee]:
    """Get committee by ID"""
    query = select(Committee).where(Committee.committee_id == committee_id)
    result = await session.execute(query)
    return result.scalar_one_or_none()


async def get_unique_contributors(
    session: AsyncSession,
    search_term: str,
    limit: int = 100
) -> List[Dict[str, Any]]:
    """Get unique contributors matching search term"""
    query = select(
        distinct(Contribution.contributor_name),
        func.sum(Contribution.contribution_amount).label('total_amount'),
        func.count(Contribution.id).label('contribution_count')
    ).where(
        Contribution.contributor_name.ilike(f"%{search_term}%")
    ).group_by(
        Contribution.contributor_name
    ).order_by(
        func.sum(Contribution.contribution_amount).desc()
    ).limit(limit)
    
    result = await session.execute(query)
    contributors = []
    for row in result:
        if row.contributor_name:
            contributors.append({
                "name": row.contributor_name,
                "total_amount": float(row.total_amount or 0),
                "contribution_count": int(row.contribution_count or 0)
            })
    return contributors


async def get_bulk_data_status(
    session: AsyncSession,
    cycle: int,
    data_type: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Get bulk data import status"""
    query = select(BulkDataMetadata).where(BulkDataMetadata.cycle == cycle)
    if data_type:
        query = query.where(BulkDataMetadata.data_type == data_type)
    
    result = await session.execute(query)
    metadata_list = result.scalars().all()
    
    return [
        {
            "cycle": m.cycle,
            "data_type": m.data_type,
            "download_date": m.download_date.isoformat() if m.download_date else None,
            "file_path": m.file_path,
            "file_size": m.file_size,
            "record_count": m.record_count,
        }
        for m in metadata_list
    ]


async def get_job(session: AsyncSession, job_id: str) -> Optional[BulkImportJob]:
    """Get bulk import job by ID"""
    query = select(BulkImportJob).where(BulkImportJob.id == job_id)
    result = await session.execute(query)
    return result.scalar_one_or_none()


async def get_saved_search(session: AsyncSession, search_id: int) -> Optional[SavedSearch]:
    """Get saved search by ID"""
    query = select(SavedSearch).where(SavedSearch.id == search_id)
    result = await session.execute(query)
    return result.scalar_one_or_none()


async def verify_contribution_data(
    session: AsyncSession,
    contribution_id: str,
    expected_data: Dict[str, Any]
) -> bool:
    """Verify contribution data matches expected values"""
    query = select(Contribution).where(Contribution.contribution_id == contribution_id)
    result = await session.execute(query)
    contrib = result.scalar_one_or_none()
    
    if not contrib:
        return False
    
    for key, expected_value in expected_data.items():
        if hasattr(contrib, key):
            actual_value = getattr(contrib, key)
            if actual_value != expected_value:
                return False
    
    return True

