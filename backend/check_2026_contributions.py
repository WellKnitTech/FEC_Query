#!/usr/bin/env python3
"""Check 2026 cycle contributions"""
import asyncio
import sys
from sqlalchemy import select, func, and_, or_

sys.path.insert(0, '.')

from app.db.database import AsyncSessionLocal, Contribution

async def check_2026_contributions():
    """Check 2026 cycle contributions"""
    print("Checking 2026 cycle contributions")
    print("="*80)
    
    async with AsyncSessionLocal() as session:
        # Total contributions in 2026 cycle
        result = await session.execute(
            select(
                func.count(Contribution.id).label('count'),
                func.sum(Contribution.contribution_amount).label('total')
            )
            .where(
                and_(
                    Contribution.contribution_date >= '2025-01-01',
                    Contribution.contribution_date <= '2026-12-31'
                )
            )
        )
        stats = result.first()
        print(f"2026 Cycle contributions (by date):")
        print(f"  Count: {stats.count or 0:,}")
        print(f"  Total: ${stats.total or 0:,.2f}")
        
        # Contributions without dates
        result = await session.execute(
            select(func.count(Contribution.id))
            .where(Contribution.contribution_date.is_(None))
        )
        no_date = result.scalar() or 0
        print(f"\nContributions without dates: {no_date:,}")
        
        # Check contributions with candidate_id H6TX21301
        result = await session.execute(
            select(
                func.count(Contribution.id).label('count'),
                func.sum(Contribution.contribution_amount).label('total')
            )
            .where(Contribution.candidate_id == 'H6TX21301')
        )
        candidate_stats = result.first()
        print(f"\nContributions with candidate_id=H6TX21301:")
        print(f"  Count: {candidate_stats.count or 0:,}")
        print(f"  Total: ${candidate_stats.total or 0:,.2f}")
        
        # Sample these contributions
        if candidate_stats.count and candidate_stats.count > 0:
            result = await session.execute(
                select(Contribution)
                .where(Contribution.candidate_id == 'H6TX21301')
                .limit(10)
            )
            samples = result.scalars().all()
            print(f"\nSample contributions for H6TX21301:")
            for c in samples:
                print(f"  ID: {c.contribution_id}, Amount: ${c.contribution_amount or 0:,.2f}, "
                      f"Date: {c.contribution_date}, Committee: {c.committee_id or 'None'}")
        
        # Check unique committee_ids in 2026 contributions
        result = await session.execute(
            select(
                func.count(func.distinct(Contribution.committee_id)).label('unique_committees')
            )
            .where(
                and_(
                    Contribution.contribution_date >= '2025-01-01',
                    Contribution.contribution_date <= '2026-12-31'
                )
            )
        )
        unique_committees = result.scalar() or 0
        print(f"\nUnique committee_ids in 2026 contributions: {unique_committees:,}")
        
        # Check if C00917658 appears anywhere (case variations, etc.)
        result = await session.execute(
            select(
                Contribution.committee_id,
                func.count(Contribution.id).label('count')
            )
            .where(
                or_(
                    Contribution.committee_id == 'C00917658',
                    Contribution.committee_id.like('%C00917658%'),
                    Contribution.committee_id.like('%C0091765%')
                )
            )
            .group_by(Contribution.committee_id)
        )
        similar = result.all()
        if similar:
            print(f"\nContributions with similar committee_id to C00917658:")
            for row in similar:
                print(f"  {row.committee_id}: {row.count:,} contributions")
        else:
            print(f"\nNo contributions found for committee C00917658 (or similar)")
        
        # Check most common committee_ids in 2026
        result = await session.execute(
            select(
                Contribution.committee_id,
                func.count(Contribution.id).label('count')
            )
            .where(
                and_(
                    Contribution.contribution_date >= '2025-01-01',
                    Contribution.contribution_date <= '2026-12-31'
                )
            )
            .group_by(Contribution.committee_id)
            .order_by(func.count(Contribution.id).desc())
            .limit(10)
        )
        top_committees = result.all()
        print(f"\nTop 10 committees by contribution count in 2026:")
        for row in top_committees:
            print(f"  {row.committee_id}: {row.count:,} contributions")

if __name__ == "__main__":
    asyncio.run(check_2026_contributions())

