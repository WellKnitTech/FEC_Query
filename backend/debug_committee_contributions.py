#!/usr/bin/env python3
"""Debug script to check contributions for a specific committee"""
import asyncio
import sys
from sqlalchemy import select, func, and_, or_

sys.path.insert(0, '.')

from app.db.database import AsyncSessionLocal, Contribution, Committee

async def check_committee_contributions(committee_id: str = "C00917658"):
    """Check contributions for a specific committee"""
    print(f"Checking contributions for committee: {committee_id}")
    print("="*80)
    
    async with AsyncSessionLocal() as session:
        # Check if committee exists
        result = await session.execute(
            select(Committee)
            .where(Committee.committee_id == committee_id)
        )
        committee = result.scalar_one_or_none()
        if committee:
            print(f"✓ Committee found: {committee.name}")
            print(f"  candidate_ids: {committee.candidate_ids}")
        else:
            print(f"✗ Committee NOT found")
            return
        
        # Check contributions with this committee_id
        result = await session.execute(
            select(
                func.count(Contribution.id).label('count'),
                func.sum(Contribution.contribution_amount).label('total')
            )
            .where(Contribution.committee_id == committee_id)
        )
        stats = result.first()
        print(f"\nContributions with committee_id={committee_id}:")
        print(f"  Count: {stats.count or 0:,}")
        print(f"  Total: ${stats.total or 0:,.2f}")
        
        # Check by cycle (using dates)
        if stats.count and stats.count > 0:
            # Check 2026 cycle (Jan 1, 2025 - Dec 31, 2026)
            result = await session.execute(
                select(
                    func.count(Contribution.id).label('count'),
                    func.sum(Contribution.contribution_amount).label('total')
                )
                .where(
                    and_(
                        Contribution.committee_id == committee_id,
                        Contribution.contribution_date >= '2025-01-01',
                        Contribution.contribution_date <= '2026-12-31'
                    )
                )
            )
            cycle_stats = result.first()
            print(f"\n2026 Cycle contributions:")
            print(f"  Count: {cycle_stats.count or 0:,}")
            print(f"  Total: ${cycle_stats.total or 0:,.2f}")
            
            # Sample contributions
            result = await session.execute(
                select(Contribution)
                .where(Contribution.committee_id == committee_id)
                .limit(10)
            )
            samples = result.scalars().all()
            print(f"\nSample contributions:")
            for c in samples:
                print(f"  ID: {c.contribution_id}, Amount: ${c.contribution_amount or 0:,.2f}, "
                      f"Date: {c.contribution_date}, Candidate: {c.candidate_id or 'None'}")
        
        # Check if there are contributions with similar committee_id (typos?)
        result = await session.execute(
            select(
                Contribution.committee_id,
                func.count(Contribution.id).label('count')
            )
            .where(Contribution.committee_id.like(f"{committee_id}%"))
            .group_by(Contribution.committee_id)
            .limit(10)
        )
        similar = result.all()
        if similar:
            print(f"\nSimilar committee_ids found:")
            for row in similar:
                print(f"  {row.committee_id}: {row.count:,} contributions")
        
        # Check all unique committee_ids in contributions (to see if there's a pattern)
        result = await session.execute(
            select(
                func.count(func.distinct(Contribution.committee_id)).label('unique_committees')
            )
        )
        unique_count = result.scalar() or 0
        print(f"\nTotal unique committee_ids in contributions: {unique_count:,}")

if __name__ == "__main__":
    asyncio.run(check_committee_contributions())

