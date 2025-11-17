#!/usr/bin/env python3
"""
Diagnostic script to check data quality for a specific candidate.
Checks for missing candidate_id, committee linkages, and date ranges.
"""
import asyncio
import sys
from sqlalchemy import select, func, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession

sys.path.insert(0, '.')

from app.db.database import AsyncSessionLocal, Contribution, Committee

async def diagnose_candidate(candidate_id: str):
    """Diagnose data issues for a specific candidate"""
    print("=" * 80)
    print(f"DATA DIAGNOSTIC FOR CANDIDATE: {candidate_id}")
    print("=" * 80)
    print()
    
    async with AsyncSessionLocal() as session:
        # 1. Check all committees for this candidate
        print("1. COMMITTEES LINKED TO CANDIDATE")
        print("-" * 80)
        result = await session.execute(
            select(Committee).where(Committee.candidate_ids.contains(candidate_id))
        )
        committees = result.scalars().all()
        
        if not committees:
            print(f"  ⚠️  No committees found for candidate {candidate_id}")
        else:
            for comm in committees:
                print(f"  Committee: {comm.committee_id} - {comm.name}")
                print(f"    Candidate IDs: {comm.candidate_ids}")
        
        print()
        
        # 2. Check contributions with candidate_id set
        print("2. CONTRIBUTIONS WITH candidate_id SET")
        print("-" * 80)
        result = await session.execute(
            select(
                func.count(Contribution.id).label('count'),
                func.sum(Contribution.contribution_amount).label('total'),
                func.min(Contribution.contribution_date).label('min_date'),
                func.max(Contribution.contribution_date).label('max_date')
            ).where(Contribution.candidate_id == candidate_id)
        )
        stats = result.first()
        print(f"  Total: {stats.count or 0:,} contributions")
        print(f"  Total Amount: ${stats.total or 0:,.2f}")
        print(f"  Date Range: {stats.min_date} to {stats.max_date}")
        print()
        
        # 3. Check contributions by cycle
        print("3. CONTRIBUTIONS BY CYCLE")
        print("-" * 80)
        
        # Cycle 2024 (2023-01-01 to 2024-12-31)
        result = await session.execute(
            select(
                func.count(Contribution.id).label('count'),
                func.sum(Contribution.contribution_amount).label('total')
            ).where(
                and_(
                    Contribution.candidate_id == candidate_id,
                    Contribution.contribution_date >= '2023-01-01',
                    Contribution.contribution_date <= '2024-12-31'
                )
            )
        )
        stats_2024 = result.first()
        print(f"  Cycle 2024 (2023-2024): {stats_2024.count or 0:,} contributions, ${stats_2024.total or 0:,.2f}")
        
        # Cycle 2026 (2025-01-01 to 2026-12-31)
        result = await session.execute(
            select(
                func.count(Contribution.id).label('count'),
                func.sum(Contribution.contribution_amount).label('total')
            ).where(
                and_(
                    Contribution.candidate_id == candidate_id,
                    Contribution.contribution_date >= '2025-01-01',
                    Contribution.contribution_date <= '2026-12-31'
                )
            )
        )
        stats_2026 = result.first()
        print(f"  Cycle 2026 (2025-2026): {stats_2026.count or 0:,} contributions, ${stats_2026.total or 0:,.2f}")
        print()
        
        # 4. Check contributions for candidate's committees (even if candidate_id is missing)
        print("4. CONTRIBUTIONS FOR CANDIDATE'S COMMITTEES (regardless of candidate_id)")
        print("-" * 80)
        
        if committees:
            committee_ids = [comm.committee_id for comm in committees]
            
            # All contributions for these committees
            result = await session.execute(
                select(
                    func.count(Contribution.id).label('count'),
                    func.sum(Contribution.contribution_amount).label('total'),
                    func.min(Contribution.contribution_date).label('min_date'),
                    func.max(Contribution.contribution_date).label('max_date')
                ).where(Contribution.committee_id.in_(committee_ids))
            )
            stats_comm = result.first()
            print(f"  Total: {stats_comm.count or 0:,} contributions")
            print(f"  Total Amount: ${stats_comm.total or 0:,.2f}")
            print(f"  Date Range: {stats_comm.min_date} to {stats_comm.max_date}")
            print()
            
            # Contributions for these committees in cycle 2026
            result = await session.execute(
                select(
                    func.count(Contribution.id).label('count'),
                    func.sum(Contribution.contribution_amount).label('total')
                ).where(
                    and_(
                        Contribution.committee_id.in_(committee_ids),
                        Contribution.contribution_date >= '2025-01-01',
                        Contribution.contribution_date <= '2026-12-31'
                    )
                )
            )
            stats_comm_2026 = result.first()
            print(f"  Cycle 2026 (2025-2026): {stats_comm_2026.count or 0:,} contributions, ${stats_comm_2026.total or 0:,.2f}")
            
            # Check if any are missing candidate_id
            result = await session.execute(
                select(func.count(Contribution.id)).where(
                    and_(
                        Contribution.committee_id.in_(committee_ids),
                        or_(
                            Contribution.candidate_id.is_(None),
                            Contribution.candidate_id == ''
                        )
                    )
                )
            )
            missing_count = result.scalar() or 0
            if missing_count > 0:
                print(f"  ⚠️  {missing_count:,} contributions missing candidate_id (can be backfilled)")
            print()
        
        # 5. Check contributions in 2026 cycle that might belong to this candidate
        print("5. ALL 2026 CYCLE CONTRIBUTIONS (to check if data exists but isn't linked)")
        print("-" * 80)
        result = await session.execute(
            select(
                func.count(Contribution.id).label('count'),
                func.sum(Contribution.contribution_amount).label('total')
            ).where(
                and_(
                    Contribution.contribution_date >= '2025-01-01',
                    Contribution.contribution_date <= '2026-12-31'
                )
            )
        )
        stats_all_2026 = result.first()
        print(f"  Total 2026 cycle contributions in database: {stats_all_2026.count or 0:,}")
        print(f"  Total Amount: ${stats_all_2026.total or 0:,.2f}")
        print()
        
        # 6. Recommendations
        print("6. RECOMMENDATIONS")
        print("-" * 80)
        
        if stats_2026.count == 0:
            if committees:
                if stats_comm_2026.count == 0:
                    print("  ❌ No 2026 cycle contributions found for this candidate's committees.")
                    print("     → The 2026 cycle data may not have been imported, or")
                    print("     → This candidate's committee may not have received contributions in 2026 cycle.")
                    print("     → Consider re-importing 2026 cycle data.")
                else:
                    print(f"  ✅ Found {stats_comm_2026.count:,} contributions for candidate's committees in 2026 cycle")
                    print("     → But candidate_id is not set. Run backfill_candidate_ids script.")
            else:
                print("  ⚠️  No committees found for this candidate.")
                print("     → Committee linkage data may be missing.")
        else:
            print(f"  ✅ Found {stats_2026.count:,} contributions for this candidate in 2026 cycle")
        
        if missing_count > 0:
            print(f"  → Run backfill script to set candidate_id on {missing_count:,} contributions")
        
        print()

if __name__ == "__main__":
    candidate_id = sys.argv[1] if len(sys.argv) > 1 else "H6TX21301"
    asyncio.run(diagnose_candidate(candidate_id))

