#!/usr/bin/env python3
"""
Investigate import issues for candidate_summary and operating_expenditures.

This script:
1. Checks what cycles are actually in the database
2. Verifies cycle-specific counts
3. Checks import status and metadata
4. Examines actual import logs/errors
"""
import asyncio
import sys
from pathlib import Path
from sqlalchemy import select, func, distinct
from sqlalchemy.ext.asyncio import AsyncSession

sys.path.insert(0, str(Path(__file__).parent))

from app.db.database import (
    AsyncSessionLocal,
    CandidateSummary,
    OperatingExpenditure,
    BulkDataMetadata,
    BulkDataImportStatus,
    BulkImportJob,
)
from app.services.bulk_data_config import DataType


async def check_cycles_in_database():
    """Check what cycles are actually stored in the database"""
    print("=" * 100)
    print("CYCLES IN DATABASE")
    print("=" * 100)
    
    async with AsyncSessionLocal() as session:
        # Check candidate_summary cycles
        result = await session.execute(
            select(distinct(CandidateSummary.cycle))
            .order_by(CandidateSummary.cycle)
        )
        cand_summary_cycles = [row[0] for row in result.all()]
        print(f"\nCandidate Summary cycles: {cand_summary_cycles}")
        
        if cand_summary_cycles:
            for cycle in cand_summary_cycles:
                count_result = await session.execute(
                    select(func.count(CandidateSummary.id))
                    .where(CandidateSummary.cycle == cycle)
                )
                count = count_result.scalar() or 0
                print(f"  Cycle {cycle}: {count:,} records")
        
        # Check operating_expenditures cycles
        result = await session.execute(
            select(distinct(OperatingExpenditure.cycle))
            .order_by(OperatingExpenditure.cycle)
        )
        op_exp_cycles = [row[0] for row in result.all()]
        print(f"\nOperating Expenditures cycles: {op_exp_cycles}")
        
        if op_exp_cycles:
            for cycle in op_exp_cycles:
                count_result = await session.execute(
                    select(func.count(OperatingExpenditure.id))
                    .where(OperatingExpenditure.cycle == cycle)
                )
                count = count_result.scalar() or 0
                print(f"  Cycle {cycle}: {count:,} records")


async def check_import_status(cycle: int = 2026):
    """Check import status and metadata for the cycle"""
    print("\n" + "=" * 100)
    print(f"IMPORT STATUS FOR CYCLE {cycle}")
    print("=" * 100)
    
    async with AsyncSessionLocal() as session:
        # Check BulkDataImportStatus
        print("\nBulkDataImportStatus:")
        result = await session.execute(
            select(BulkDataImportStatus)
            .where(BulkDataImportStatus.cycle == cycle)
            .where(BulkDataImportStatus.data_type.in_([
                DataType.CANDIDATE_SUMMARY.value,
                DataType.OPERATING_EXPENDITURES.value
            ]))
        )
        statuses = result.scalars().all()
        
        if statuses:
            for status in statuses:
                print(f"\n  {status.data_type}:")
                print(f"    Status: {status.status}")
                print(f"    Record Count: {status.record_count or 0:,}")
                print(f"    Last Imported: {status.last_imported_at}")
                if status.error_message:
                    print(f"    Error: {status.error_message}")
        else:
            print("  No import status records found for these data types")
        
        # Check BulkDataMetadata
        print("\nBulkDataMetadata:")
        result = await session.execute(
            select(BulkDataMetadata)
            .where(BulkDataMetadata.cycle == cycle)
            .where(BulkDataMetadata.data_type.in_([
                DataType.CANDIDATE_SUMMARY.value,
                DataType.OPERATING_EXPENDITURES.value
            ]))
        )
        metadata_list = result.scalars().all()
        
        if metadata_list:
            for metadata in metadata_list:
                print(f"\n  {metadata.data_type}:")
                print(f"    Record Count: {metadata.record_count or 0:,}")
                print(f"    Imported: {metadata.imported}")
                print(f"    File Path: {metadata.file_path}")
                print(f"    File Size: {metadata.file_size:,} bytes" if metadata.file_size else "    File Size: N/A")
                print(f"    Download Date: {metadata.download_date}")
        else:
            print("  No metadata records found for these data types")
        
        # Check BulkImportJob for errors
        print("\nBulkImportJob (recent jobs with errors):")
        result = await session.execute(
            select(BulkImportJob)
            .where(
                BulkImportJob.data_type.in_([
                    DataType.CANDIDATE_SUMMARY.value,
                    DataType.OPERATING_EXPENDITURES.value
                ])
            )
            .where(BulkImportJob.status.in_(['failed', 'completed']))
            .order_by(BulkImportJob.started_at.desc())
            .limit(10)
        )
        jobs = result.scalars().all()
        
        if jobs:
            for job in jobs:
                print(f"\n  Job {job.id[:8]}... ({job.data_type}):")
                print(f"    Status: {job.status}")
                print(f"    Cycle: {job.cycle}")
                print(f"    Imported Records: {job.imported_records or 0:,}")
                print(f"    Skipped Records: {job.skipped_records or 0:,}")
                if job.error_message:
                    print(f"    Error: {job.error_message}")
                print(f"    Started: {job.started_at}")
                print(f"    Completed: {job.completed_at}")
        else:
            print("  No recent import jobs found")


async def check_cycle_specific_counts(cycle: int = 2026):
    """Check cycle-specific counts in database"""
    print("\n" + "=" * 100)
    print(f"CYCLE-SPECIFIC COUNTS FOR CYCLE {cycle}")
    print("=" * 100)
    
    async with AsyncSessionLocal() as session:
        # Candidate Summary
        result = await session.execute(
            select(func.count(CandidateSummary.id))
            .where(CandidateSummary.cycle == cycle)
        )
        cand_summary_count = result.scalar() or 0
        print(f"\nCandidate Summary (cycle {cycle}): {cand_summary_count:,} records")
        
        # Operating Expenditures
        result = await session.execute(
            select(func.count(OperatingExpenditure.id))
            .where(OperatingExpenditure.cycle == cycle)
        )
        op_exp_count = result.scalar() or 0
        print(f"Operating Expenditures (cycle {cycle}): {op_exp_count:,} records")
        
        # Total counts (all cycles)
        result = await session.execute(select(func.count(CandidateSummary.id)))
        total_cand_summary = result.scalar() or 0
        print(f"\nCandidate Summary (all cycles): {total_cand_summary:,} records")
        
        result = await session.execute(select(func.count(OperatingExpenditure.id)))
        total_op_exp = result.scalar() or 0
        print(f"Operating Expenditures (all cycles): {total_op_exp:,} records")


async def check_file_exists(cycle: int = 2026):
    """Check if source files exist"""
    print("\n" + "=" * 100)
    print(f"SOURCE FILES FOR CYCLE {cycle}")
    print("=" * 100)
    
    import os
    from app.services.bulk_data_config import get_config
    
    bulk_data_dir = Path(os.getenv("BULK_DATA_DIR", "./data/bulk"))
    
    for data_type in [DataType.CANDIDATE_SUMMARY, DataType.OPERATING_EXPENDITURES]:
        config = get_config(data_type)
        if not config:
            continue
        
        # Check for CSV file
        csv_path = bulk_data_dir / f"{data_type.value}_{cycle}.csv"
        # Check for extracted TXT file
        txt_path = bulk_data_dir / f"{data_type.value}_{cycle}.txt"
        # Check for ZIP file
        zip_path = bulk_data_dir / f"{data_type.value}_{cycle}.zip"
        
        print(f"\n{data_type.value}:")
        print(f"  CSV: {csv_path.exists()} - {csv_path}")
        if csv_path.exists():
            size = csv_path.stat().st_size
            print(f"    Size: {size:,} bytes ({size / (1024*1024):.2f} MB)")
        
        print(f"  TXT: {txt_path.exists()} - {txt_path}")
        if txt_path.exists():
            size = txt_path.stat().st_size
            print(f"    Size: {size:,} bytes ({size / (1024*1024):.2f} MB)")
        
        print(f"  ZIP: {zip_path.exists()} - {zip_path}")
        if zip_path.exists():
            size = zip_path.stat().st_size
            print(f"    Size: {size:,} bytes ({size / (1024*1024):.2f} MB)")


async def sample_candidate_summary_file(cycle: int = 2026):
    """Sample records from candidate_summary file to see what's there"""
    print("\n" + "=" * 100)
    print(f"SAMPLING CANDIDATE SUMMARY FILE FOR CYCLE {cycle}")
    print("=" * 100)
    
    import os
    import pandas as pd
    from app.services.bulk_data_config import get_config
    
    bulk_data_dir = Path(os.getenv("BULK_DATA_DIR", "./data/bulk"))
    data_type = DataType.CANDIDATE_SUMMARY
    
    # Find the file
    csv_path = bulk_data_dir / f"{data_type.value}_{cycle}.csv"
    
    if not csv_path.exists():
        print(f"File not found: {csv_path}")
        return
    
    try:
        # Read first few rows
        df = pd.read_csv(csv_path, nrows=10)
        print(f"\nFile columns: {list(df.columns)}")
        print(f"\nFirst 5 records:")
        print(df.head().to_string())
        
        # Count total records
        total_count = sum(1 for _ in open(csv_path)) - 1  # Subtract header
        print(f"\nTotal records in file (estimated): {total_count:,}")
        
    except Exception as e:
        print(f"Error reading file: {e}")


async def sample_operating_expenditures_file(cycle: int = 2026):
    """Sample records from operating_expenditures file"""
    print("\n" + "=" * 100)
    print(f"SAMPLING OPERATING EXPENDITURES FILE FOR CYCLE {cycle}")
    print("=" * 100)
    
    import os
    import zipfile
    import pandas as pd
    from app.services.bulk_data_config import get_config
    
    bulk_data_dir = Path(os.getenv("BULK_DATA_DIR", "./data/bulk"))
    data_type = DataType.OPERATING_EXPENDITURES
    config = get_config(data_type)
    
    # Check for extracted file first
    txt_path = bulk_data_dir / f"{data_type.value}_{cycle}.txt"
    zip_path = bulk_data_dir / f"{data_type.value}_{cycle}.zip"
    
    file_path = None
    if txt_path.exists():
        file_path = txt_path
        print(f"Using extracted file: {txt_path}")
    elif zip_path.exists():
        print(f"Extracting from ZIP: {zip_path}")
        # Extract to temp location
        import tempfile
        with tempfile.TemporaryDirectory() as temp_dir:
            with zipfile.ZipFile(zip_path, 'r') as zip_file:
                if config and config.zip_internal_file:
                    zip_file.extract(config.zip_internal_file, temp_dir)
                    file_path = Path(temp_dir) / config.zip_internal_file
                else:
                    print("No zip_internal_file configured")
                    return
    
    if not file_path or not file_path.exists():
        print("File not found")
        return
    
    try:
        # Read first few rows (pipe-delimited, no header)
        # Get column names from header file
        header_path = bulk_data_dir / "headers" / "oppexp_header_file.csv"
        columns = None
        if header_path.exists():
            header_df = pd.read_csv(header_path, nrows=0)
            columns = list(header_df.columns)
            print(f"Using columns from header file: {len(columns)} columns")
        else:
            print("Header file not found, using default columns")
            # Default oppexp columns
            columns = [
                'CMTE_ID', 'AMNDT_IND', 'RPT_YR', 'RPT_TP', 'IMAGE_NUM', 'LINE_NUM',
                'FORM_TP_CD', 'SCHED_TP_CD', 'TRAN_ID', 'FILE_NUM', 'ENTITY_TP',
                'SUB_ID', 'TRANSACTION_DT', 'TRANSACTION_AMT', 'CATEGORY', 'CATEGORY_DESC',
                'MEMO_CD', 'MEMO_TEXT', 'BACK_REF_TRAN_ID'
            ]
        
        df = pd.read_csv(
            file_path,
            sep='|',
            header=None,
            names=columns,
            nrows=10,
            dtype=str
        )
        print(f"\nFile columns: {list(df.columns)}")
        print(f"\nFirst 5 records:")
        print(df[['CMTE_ID', 'SUB_ID', 'TRANSACTION_DT', 'TRANSACTION_AMT']].head().to_string())
        
        # Count total records (quick estimate)
        print(f"\nCounting total records...")
        chunk_count = 0
        total_records = 0
        for chunk in pd.read_csv(file_path, sep='|', header=None, names=columns, chunksize=100000, dtype=str):
            total_records += len(chunk)
            chunk_count += 1
            if chunk_count % 10 == 0:
                print(f"  Processed {chunk_count} chunks: {total_records:,} records so far...")
        print(f"\nTotal records in file: {total_records:,}")
        
    except Exception as e:
        print(f"Error reading file: {e}")
        import traceback
        traceback.print_exc()


async def main():
    """Run all investigations"""
    cycle = int(sys.argv[1]) if len(sys.argv) > 1 else 2026
    
    print("=" * 100)
    print("IMPORT ISSUE INVESTIGATION")
    print("=" * 100)
    print(f"Cycle: {cycle}")
    print()
    
    await check_cycles_in_database()
    await check_cycle_specific_counts(cycle)
    await check_import_status(cycle)
    await check_file_exists(cycle)
    await sample_candidate_summary_file(cycle)
    await sample_operating_expenditures_file(cycle)
    
    print("\n" + "=" * 100)
    print("INVESTIGATION COMPLETE")
    print("=" * 100)


if __name__ == "__main__":
    asyncio.run(main())

