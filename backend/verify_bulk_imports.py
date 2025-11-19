#!/usr/bin/env python3
"""
Comprehensive verification script for bulk data imports.

Checks that all imported data types are being parsed and stored correctly.
"""
import asyncio
import sys
from datetime import datetime
from sqlalchemy import select, func, and_, or_, distinct
from sqlalchemy.ext.asyncio import AsyncSession

sys.path.insert(0, '.')

from app.db.database import (
    AsyncSessionLocal,
    Contribution,
    Committee,
    Candidate,
    BulkDataMetadata,
    BulkDataImportStatus,
    IndependentExpenditure,
    OperatingExpenditure,
    CandidateSummary,
    CommitteeSummary,
    ElectioneeringComm,
    CommunicationCost
)
from app.services.bulk_data_config import DataType, DATA_TYPE_CONFIGS

# Import verification modules
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from verify_bulk_file_counts import BulkFileCounter
from verify_database_counts import DatabaseCounter

async def check_individual_contributions(cycle: int = 2026):
    """Verify individual contributions import"""
    print("\n" + "="*80)
    print("INDIVIDUAL CONTRIBUTIONS (Schedule A)")
    print("="*80)
    
    async with AsyncSessionLocal() as session:
        # Total count
        result = await session.execute(select(func.count(Contribution.id)))
        total = result.scalar() or 0
        print(f"Total contributions in database: {total:,}")
        
        # Count by cycle (using contribution_date)
        if total > 0:
            result = await session.execute(
                select(
                    func.count(Contribution.id).label('count'),
                    func.sum(Contribution.contribution_amount).label('total')
                )
            )
            stats = result.first()
            print(f"  Total amount: ${stats.total or 0:,.2f}")
        
        # Check for contributions with candidate_id
        result = await session.execute(
            select(func.count(Contribution.id))
            .where(
                and_(
                    Contribution.candidate_id.isnot(None),
                    Contribution.candidate_id != ''
                )
            )
        )
        with_candidate_id = result.scalar() or 0
        print(f"Contributions with candidate_id: {with_candidate_id:,} ({with_candidate_id/total*100:.1f}%)" if total > 0 else "Contributions with candidate_id: 0")
        
        # Check for contributions with committee_id
        result = await session.execute(
            select(func.count(Contribution.id))
            .where(
                and_(
                    Contribution.committee_id.isnot(None),
                    Contribution.committee_id != ''
                )
            )
        )
        with_committee_id = result.scalar() or 0
        print(f"Contributions with committee_id: {with_committee_id:,} ({with_committee_id/total*100:.1f}%)" if total > 0 else "Contributions with committee_id: 0")
        
        # Check for contributions missing both
        result = await session.execute(
            select(func.count(Contribution.id))
            .where(
                or_(
                    Contribution.candidate_id.is_(None),
                    Contribution.candidate_id == '',
                    Contribution.committee_id.is_(None),
                    Contribution.committee_id == ''
                )
            )
        )
        missing_both = result.scalar() or 0
        print(f"Contributions missing candidate_id or committee_id: {missing_both:,}")
        
        # Sample a few contributions to check data quality
        if total > 0:
            result = await session.execute(
                select(Contribution)
                .limit(5)
            )
            samples = result.scalars().all()
            print(f"\nSample contributions:")
            for c in samples:
                print(f"  ID: {c.contribution_id}, Amount: ${c.contribution_amount or 0:,.2f}, "
                      f"Candidate: {c.candidate_id or 'None'}, Committee: {c.committee_id or 'None'}, "
                      f"Date: {c.contribution_date or 'None'}")
        
        # Check metadata
        result = await session.execute(
            select(BulkDataMetadata)
            .where(BulkDataMetadata.data_type == DataType.INDIVIDUAL_CONTRIBUTIONS.value)
            .order_by(BulkDataMetadata.cycle.desc())
        )
        metadata = result.scalars().all()
        if metadata:
            print(f"\nImport metadata:")
            for m in metadata[:5]:  # Show last 5 cycles
                print(f"  Cycle {m.cycle}: {m.record_count:,} records, "
                      f"Downloaded: {m.download_date}, Imported: {m.imported}")

async def check_committees():
    """Verify committee master import"""
    print("\n" + "="*80)
    print("COMMITTEE MASTER")
    print("="*80)
    
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(func.count(Committee.id)))
        total = result.scalar() or 0
        print(f"Total committees in database: {total:,}")
        
        # Check committees with candidate_ids
        # SQLite JSON: check if candidate_ids is not null and not empty
        result = await session.execute(
            select(Committee.committee_id, Committee.candidate_ids)
        )
        all_committees = result.all()
        with_candidate_ids = 0
        for comm in all_committees:
            if comm.candidate_ids and isinstance(comm.candidate_ids, list) and len(comm.candidate_ids) > 0:
                with_candidate_ids += 1
        print(f"Committees with candidate_ids: {with_candidate_ids:,} ({with_candidate_ids/total*100:.1f}%)" if total > 0 else "Committees with candidate_ids: 0")
        
        # Check for specific candidate
        test_candidate = "H6TX21301"
        result = await session.execute(
            select(Committee.committee_id, Committee.name, Committee.candidate_ids)
            .where(Committee.candidate_ids.contains([test_candidate]))
            .limit(10)
        )
        committees = result.all()
        print(f"\nCommittees linked to candidate {test_candidate}: {len(committees)}")
        for comm in committees:
            print(f"  {comm.committee_id}: {comm.name} - candidate_ids: {comm.candidate_ids}")
        
        # Sample committees
        if total > 0:
            result = await session.execute(
                select(Committee)
                .limit(5)
            )
            samples = result.scalars().all()
            print(f"\nSample committees:")
            for c in samples:
                print(f"  {c.committee_id}: {c.name}, candidate_ids: {c.candidate_ids or 'None'}")

async def check_candidates():
    """Verify candidate master import"""
    print("\n" + "="*80)
    print("CANDIDATE MASTER")
    print("="*80)
    
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(func.count(Candidate.id)))
        total = result.scalar() or 0
        print(f"Total candidates in database: {total:,}")
        
        # Check for specific candidate
        test_candidate = "H6TX21301"
        result = await session.execute(
            select(Candidate)
            .where(Candidate.candidate_id == test_candidate)
        )
        candidate = result.scalar_one_or_none()
        if candidate:
            print(f"\nCandidate {test_candidate} found:")
            print(f"  Name: {candidate.name}")
            print(f"  Office: {candidate.office}")
            print(f"  State: {candidate.state}")
        else:
            print(f"\nCandidate {test_candidate} NOT found in database")

async def check_candidate_committee_linkage():
    """Verify candidate-committee linkage import"""
    print("\n" + "="*80)
    print("CANDIDATE-COMMITTEE LINKAGE")
    print("="*80)
    
    async with AsyncSessionLocal() as session:
        # Check if committees have candidate_ids populated
        test_candidate = "H6TX21301"
        result = await session.execute(select(func.count(Committee.id)))
        total = result.scalar() or 0
        
        # Count committees with candidate_ids
        result = await session.execute(
            select(Committee.committee_id, Committee.candidate_ids)
        )
        all_committees = result.all()
        with_ids = sum(1 for c in all_committees if c.candidate_ids and isinstance(c.candidate_ids, list) and len(c.candidate_ids) > 0)
        
        print(f"Total committees: {total:,}")
        print(f"Committees with candidate_ids: {with_ids:,}")
        
        # Check specific candidate
        result = await session.execute(
            select(Committee.committee_id, Committee.name, Committee.candidate_ids)
            .where(Committee.candidate_ids.contains([test_candidate]))
        )
        linked_committees = result.all()
        print(f"\nCommittees linked to {test_candidate}: {len(linked_committees)}")
        for comm in linked_committees[:10]:
            print(f"  {comm.committee_id}: {comm.name}")
        
        # Check contributions for these committees
        if linked_committees:
            committee_ids = [c.committee_id for c in linked_committees]
            result = await session.execute(
                select(
                    func.count(Contribution.id).label('count'),
                    func.sum(Contribution.contribution_amount).label('total')
                )
                .where(Contribution.committee_id.in_(committee_ids))
            )
            contrib_stats = result.first()
            print(f"\nContributions for {test_candidate}'s committees:")
            print(f"  Count: {contrib_stats.count or 0:,}")
            print(f"  Total: ${contrib_stats.total or 0:,.2f}")

async def check_other_data_types():
    """Verify other data type imports"""
    print("\n" + "="*80)
    print("OTHER DATA TYPES")
    print("="*80)
    
    data_types = [
        (DataType.INDEPENDENT_EXPENDITURES, IndependentExpenditure, "Independent Expenditures"),
        (DataType.OPERATING_EXPENDITURES, OperatingExpenditure, "Operating Expenditures"),
        (DataType.CANDIDATE_SUMMARY, CandidateSummary, "Candidate Summary"),
        (DataType.COMMITTEE_SUMMARY, CommitteeSummary, "Committee Summary"),
        (DataType.ELECTIONEERING_COMM, ElectioneeringComm, "Electioneering Comm"),
        (DataType.COMMUNICATION_COSTS, CommunicationCost, "Communication Costs"),
    ]
    
    async with AsyncSessionLocal() as session:
        for data_type, model, name in data_types:
            try:
                result = await session.execute(select(func.count(model.id)))
                count = result.scalar() or 0
                print(f"{name}: {count:,} records")
            except Exception as e:
                print(f"{name}: Error checking - {e}")

async def check_import_status(cycle: int = 2026):
    """Check import status for all data types"""
    print("\n" + "="*80)
    print(f"IMPORT STATUS FOR CYCLE {cycle}")
    print("="*80)
    
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(BulkDataImportStatus)
            .where(BulkDataImportStatus.cycle == cycle)
        )
        statuses = result.scalars().all()
        
        if not statuses:
            print("No import status records found for this cycle")
            return
        
        print(f"{'Data Type':<30} {'Status':<15} {'Records':<15} {'Last Imported':<20}")
        print("-" * 80)
        for status in statuses:
            imported_date = status.last_imported_at.strftime("%Y-%m-%d %H:%M") if status.last_imported_at else "Never"
            print(f"{status.data_type:<30} {status.status:<15} {status.record_count or 0:>15,} {imported_date:<20}")
            if status.error_message:
                print(f"  Error: {status.error_message}")

async def check_metadata(cycle: int = 2026):
    """Check metadata for all data types"""
    print("\n" + "="*80)
    print(f"METADATA FOR CYCLE {cycle}")
    print("="*80)
    
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(BulkDataMetadata)
            .where(BulkDataMetadata.cycle == cycle)
        )
        metadata_list = result.scalars().all()
        
        if not metadata_list:
            print("No metadata records found for this cycle")
            return
        
        print(f"{'Data Type':<30} {'Records':<15} {'File Size':<15} {'Downloaded':<20} {'Imported':<10}")
        print("-" * 100)
        for m in metadata_list:
            file_size_mb = (m.file_size / (1024*1024)) if m.file_size else 0
            downloaded = m.download_date.strftime("%Y-%m-%d") if m.download_date else "Never"
            print(f"{m.data_type:<30} {m.record_count or 0:>15,} {file_size_mb:>14.1f} MB {downloaded:<20} {str(m.imported):<10}")

async def compare_file_and_database_counts(cycle: int = 2026):
    """Compare source file counts with database counts"""
    print("\n" + "="*80)
    print(f"FILE vs DATABASE COUNT COMPARISON FOR CYCLE {cycle}")
    print("="*80)
    
    # Count records in source files
    print("\nCounting records in source files...")
    file_counter = BulkFileCounter()
    file_counts = await file_counter.count_all_files(cycle)
    
    # Count records in database
    print("\nCounting records in database...")
    db_counter = DatabaseCounter()
    db_counts = await db_counter.count_all_tables(cycle)
    metadata_counts = await db_counter.get_all_metadata_counts(cycle)
    status_counts = await db_counter.get_all_status_counts(cycle)
    
    # Print comparison table
    print(f"\n{'Data Type':<40} {'File Count':<15} {'DB Count':<15} {'Metadata':<15} {'Status':<15} {'Diff':<15} {'Status':<10}")
    print("-" * 120)
    
    total_passed = 0
    total_failed = 0
    total_warnings = 0
    
    for data_type in DataType:
        data_type_str = data_type.value
        file_count = file_counts.get(data_type_str, 0)
        db_count = db_counts.get(data_type_str, 0)
        metadata_count = metadata_counts.get(data_type_str)
        status_count = status_counts.get(data_type_str)
        
        # Use database count, fall back to metadata/status
        expected_count = db_count
        if expected_count == 0 and metadata_count is not None:
            expected_count = metadata_count
        if expected_count == 0 and status_count is not None:
            expected_count = status_count
        
        difference = file_count - expected_count
        percent_diff = (difference / file_count * 100) if file_count > 0 else 0
        
        # Determine status
        tolerance = max(int(file_count * 0.001), 100)  # 0.1% or 100, whichever is larger
        
        if abs(difference) <= tolerance:
            status = '✓ PASS'
            total_passed += 1
        elif file_count == 0 and expected_count == 0:
            status = '⚠ ZERO'
            total_warnings += 1
        elif file_count > 0 and expected_count == 0:
            status = '✗ FAIL'
            total_failed += 1
        else:
            status = '⚠ WARN'
            total_warnings += 1
        
        metadata_str = f"{metadata_count:,}" if metadata_count is not None else "N/A"
        status_str = f"{status_count:,}" if status_count is not None else "N/A"
        
        print(f"{data_type_str:<40} {file_count:>15,} {expected_count:>15,} {metadata_str:>15} {status_str:>15} {difference:>15,} {status:<10}")
    
    print("\n" + "-" * 120)
    print(f"Summary: {total_passed} passed, {total_failed} failed, {total_warnings} warnings")
    
    if total_failed > 0:
        print("\n⚠️  WARNING: Some imports have significant discrepancies!")
        print("   Review the failed data types above and check import logs.")
    
    return {
        'file_counts': file_counts,
        'database_counts': db_counts,
        'metadata_counts': metadata_counts,
        'status_counts': status_counts,
        'passed': total_passed,
        'failed': total_failed,
        'warnings': total_warnings
    }

async def diagnose_candidate_contributions(candidate_id: str = "H6TX21301"):
    """Detailed diagnosis for a specific candidate"""
    print("\n" + "="*80)
    print(f"DETAILED DIAGNOSIS FOR CANDIDATE: {candidate_id}")
    print("="*80)
    
    async with AsyncSessionLocal() as session:
        # 1. Check if candidate exists
        result = await session.execute(
            select(Candidate)
            .where(Candidate.candidate_id == candidate_id)
        )
        candidate = result.scalar_one_or_none()
        if candidate:
            print(f"✓ Candidate found: {candidate.name} ({candidate.office}, {candidate.state})")
        else:
            print(f"✗ Candidate NOT found in database")
            return
        
        # 2. Check committees linked to candidate
        result = await session.execute(
            select(Committee.committee_id, Committee.name, Committee.candidate_ids)
            .where(Committee.candidate_ids.contains([candidate_id]))
        )
        committees = result.all()
        print(f"\nCommittees linked to candidate: {len(committees)}")
        committee_ids = []
        for comm in committees:
            print(f"  {comm.committee_id}: {comm.name}")
            committee_ids.append(comm.committee_id)
        
        if not committee_ids:
            print("  ⚠️  WARNING: No committees found linked to this candidate!")
            print("  This means contributions won't be found via committee lookup.")
            print("  Solution: Import candidate-committee linkage data for cycle 2026")
        
        # 3. Check contributions with direct candidate_id
        result = await session.execute(
            select(
                func.count(Contribution.id).label('count'),
                func.sum(Contribution.contribution_amount).label('total')
            )
            .where(Contribution.candidate_id == candidate_id)
        )
        direct_stats = result.first()
        print(f"\nContributions with candidate_id={candidate_id}:")
        print(f"  Count: {direct_stats.count or 0:,}")
        print(f"  Total: ${direct_stats.total or 0:,.2f}")
        
        # 4. Check contributions via committees
        if committee_ids:
            result = await session.execute(
                select(
                    func.count(Contribution.id).label('count'),
                    func.sum(Contribution.contribution_amount).label('total')
                )
                .where(Contribution.committee_id.in_(committee_ids))
            )
            committee_stats = result.first()
            print(f"\nContributions via {len(committee_ids)} committees:")
            print(f"  Count: {committee_stats.count or 0:,}")
            print(f"  Total: ${committee_stats.total or 0:,.2f}")
            
            # Check if these contributions have candidate_id set
            # Count total
            result = await session.execute(
                select(func.count(Contribution.id))
                .where(Contribution.committee_id.in_(committee_ids))
            )
            total = result.scalar() or 0
            
            # Count with candidate_id
            result = await session.execute(
                select(func.count(Contribution.id))
                .where(
                    and_(
                        Contribution.committee_id.in_(committee_ids),
                        Contribution.candidate_id.isnot(None),
                        Contribution.candidate_id != ''
                    )
                )
            )
            with_candidate_id = result.scalar() or 0
            missing_candidate_id = total - with_candidate_id
            print(f"  With candidate_id set: {with_candidate_id:,}")
            print(f"  Missing candidate_id: {missing_candidate_id:,}")
            if missing_candidate_id > 0:
                print(f"  ⚠️  WARNING: {missing_candidate_id:,} contributions need candidate_id backfill")
        else:
            print(f"\n⚠️  Cannot check contributions via committees (no committees linked)")
        
        # 5. Combined query (what the analysis should find)
        from app.services.shared.query_builders import build_candidate_condition
        try:
            # Try without FEC client first (faster)
            candidate_condition = await build_candidate_condition(candidate_id, fec_client=None)
            result = await session.execute(
                select(
                    func.count(Contribution.id).label('count'),
                    func.sum(Contribution.contribution_amount).label('total')
                )
                .where(candidate_condition)
            )
            combined_stats = result.first()
            print(f"\nCombined query (candidate_id OR committee_id):")
            print(f"  Count: {combined_stats.count or 0:,}")
            print(f"  Total: ${combined_stats.total or 0:,.2f}")
        except Exception as e:
            print(f"\nError running combined query: {e}")

async def main():
    """Run all verification checks"""
    print("="*80)
    print("BULK DATA IMPORT VERIFICATION")
    print("="*80)
    print(f"Timestamp: {datetime.now().isoformat()}")
    
    cycle = 2026
    
    # Run all checks
    await check_import_status(cycle)
    await check_metadata(cycle)
    await compare_file_and_database_counts(cycle)  # New: Compare file vs database counts
    await check_individual_contributions(cycle)
    await check_committees()
    await check_candidates()
    await check_candidate_committee_linkage()
    await check_other_data_types()
    await diagnose_candidate_contributions("H6TX21301")
    
    print("\n" + "="*80)
    print("VERIFICATION COMPLETE")
    print("="*80)

if __name__ == "__main__":
    asyncio.run(main())

