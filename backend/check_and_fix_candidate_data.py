#!/usr/bin/env python3
"""
Script to check and fix data issues for a candidate.
1. Checks if 2026 cycle data exists for candidate's committees
2. Runs backfill to set candidate_id if missing
3. Provides recommendations for re-import if needed
"""
import asyncio
import sys
from sqlalchemy import select, func, and_, or_

sys.path.insert(0, '.')

from app.db.database import AsyncSessionLocal, Contribution, Committee
from app.services.backfill_candidate_ids import backfill_candidate_ids_from_committees

async def check_and_fix(candidate_id: str, run_backfill: bool = False):
    """Check data and optionally run backfill"""
    print("=" * 80)
    print(f"CHECKING AND FIXING DATA FOR CANDIDATE: {candidate_id}")
    print("=" * 80)
    print()
    
    async with AsyncSessionLocal() as session:
        # Get committees
        result = await session.execute(
            select(Committee).where(Committee.candidate_ids.contains(candidate_id))
        )
        committees = result.scalars().all()
        committee_ids = [comm.committee_id for comm in committees]
        
        if not committees:
            print("❌ No committees found for this candidate.")
            return
        
        print(f"Found {len(committees)} committee(s): {', '.join(committee_ids)}")
        print()
        
        # Check contributions for these committees in 2026 cycle
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
        stats = result.first()
        
        print(f"2026 Cycle Contributions for candidate's committees:")
        print(f"  Count: {stats.count or 0:,}")
        print(f"  Total: ${stats.total or 0:,.2f}")
        print()
        
        # Check contributions missing candidate_id for these committees
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
            print(f"⚠️  Found {missing_count:,} contributions missing candidate_id")
            if run_backfill:
                print("Running backfill...")
                result = await backfill_candidate_ids_from_committees(limit=missing_count)
                print(f"✅ Backfill complete: {result.get('contributions_updated', 0):,} contributions updated")
            else:
                print("  → Run with --backfill flag to fix this")
        else:
            print("✅ All contributions have candidate_id set")
        print()
        
        # Final check after backfill
        if run_backfill:
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
            final_stats = result.first()
            print(f"Final 2026 Cycle Contributions for {candidate_id}:")
            print(f"  Count: {final_stats.count or 0:,}")
            print(f"  Total: ${final_stats.total or 0:,.2f}")
            print()
        
        # Recommendations
        print("RECOMMENDATIONS:")
        print("-" * 80)
        
        if stats.count == 0:
            print("❌ No 2026 cycle contributions found for this candidate's committees.")
            print()
            print("Possible causes:")
            print("  1. The 2026 cycle bulk import may have missed this committee")
            print("  2. The committee may not have received contributions in 2026 cycle")
            print("  3. The committee_id in the source data may not match")
            print()
            print("Actions to take:")
            print("  1. Verify the committee_id is correct in the FEC database")
            print("  2. Check if the committee received contributions in 2026 cycle via FEC API")
            print("  3. Consider re-importing 2026 cycle Schedule A (pas2) data")
            print("  4. The Financial Summary shows $213.9K from FEC API, so data exists")
            print("     → The issue is that it's not in the local database")
        else:
            print(f"✅ Found {stats.count:,} contributions in 2026 cycle")
            if missing_count > 0 and not run_backfill:
                print("  → Run backfill to set candidate_id on missing contributions")
        
        print()

if __name__ == "__main__":
    candidate_id = sys.argv[1] if len(sys.argv) > 1 else "H6TX21301"
    run_backfill = "--backfill" in sys.argv
    
    asyncio.run(check_and_fix(candidate_id, run_backfill))

