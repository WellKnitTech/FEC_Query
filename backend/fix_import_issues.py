#!/usr/bin/env python3
"""
Fix import issues identified in investigation.

Issues found:
1. candidate_summary: Column name mismatch - file has 'Cand_Id' but parser looks for 'candidate_id' or 'CAND_ID'
2. operating_expenditures: Need to investigate why only 11,883 out of 418,105 records imported
"""
import asyncio
import sys
from pathlib import Path
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))

from app.services.bulk_data_config import DataType


async def test_candidate_summary_parsing():
    """Test candidate summary parsing with actual column names"""
    print("=" * 100)
    print("TESTING CANDIDATE SUMMARY PARSING")
    print("=" * 100)
    
    import os
    bulk_data_dir = Path(os.getenv("BULK_DATA_DIR", "./data/bulk"))
    file_path = bulk_data_dir / "candidate_summary_2026.csv"
    
    if not file_path.exists():
        print(f"File not found: {file_path}")
        return
    
    # Read first few rows to see actual column names
    df = pd.read_csv(file_path, nrows=5)
    print(f"\nActual column names in file:")
    for col in df.columns:
        print(f"  - {col}")
    
    # Check what the parser is looking for
    print(f"\nParser looks for: 'candidate_id' or 'CAND_ID'")
    print(f"File has: 'Cand_Id'")
    print(f"\nISSUE: Column name mismatch!")
    
    # Test if we can find the column
    cand_id_col = None
    for name in ['candidate_id', 'CAND_ID', 'Cand_Id', 'Cand_ID']:
        if name in df.columns:
            cand_id_col = name
            break
    
    if cand_id_col:
        print(f"\nFound candidate ID column: '{cand_id_col}'")
        print(f"Sample values: {df[cand_id_col].head().tolist()}")
    else:
        print("\nERROR: Could not find candidate ID column!")


async def check_operating_expenditures_filtering():
    """Check why operating expenditures are being filtered out"""
    print("\n" + "=" * 100)
    print("CHECKING OPERATING EXPENDITURES FILTERING")
    print("=" * 100)
    
    import os
    import zipfile
    from app.services.bulk_data_config import get_config
    
    bulk_data_dir = Path(os.getenv("BULK_DATA_DIR", "./data/bulk"))
    data_type = DataType.OPERATING_EXPENDITURES
    cycle = 2026
    config = get_config(data_type)
    
    # Check for extracted file
    txt_path = bulk_data_dir / f"{data_type.value}_{cycle}.txt"
    
    if not txt_path.exists():
        print(f"File not found: {txt_path}")
        return
    
    # Get column names
    header_path = bulk_data_dir / "headers" / "oppexp_header_file.csv"
    columns = None
    if header_path.exists():
        header_df = pd.read_csv(header_path, nrows=0)
        columns = list(header_df.columns)
        print(f"Using {len(columns)} columns from header file")
    else:
        print("Header file not found")
        return
    
    # Read a sample
    df = pd.read_csv(
        txt_path,
        sep='|',
        header=None,
        names=columns,
        nrows=1000,
        dtype=str
    )
    
    print(f"\nTotal rows in sample: {len(df)}")
    
    # Check SUB_ID column (this is what the parser filters on)
    if 'SUB_ID' in df.columns:
        print(f"\nSUB_ID column analysis:")
        print(f"  Total rows: {len(df)}")
        print(f"  Non-null SUB_ID: {df['SUB_ID'].notna().sum()}")
        print(f"  Empty SUB_ID: {(df['SUB_ID'].astype(str).str.strip() == '').sum()}")
        print(f"  Valid SUB_ID: {(df['SUB_ID'].notna() & (df['SUB_ID'].astype(str).str.strip() != '')).sum()}")
        
        # Show sample SUB_ID values
        print(f"\nSample SUB_ID values:")
        print(df['SUB_ID'].head(20).tolist())
        
        # Check for unique values
        unique_sub_ids = df['SUB_ID'].nunique()
        print(f"\nUnique SUB_ID values in sample: {unique_sub_ids}")
        
        # Check if SUB_ID might be in a different column
        print(f"\nAll columns: {list(df.columns)}")
    else:
        print("ERROR: SUB_ID column not found!")
        print(f"Available columns: {list(df.columns)}")


async def check_cycle_filtering():
    """Check if operating expenditures are being filtered by cycle incorrectly"""
    print("\n" + "=" * 100)
    print("CHECKING CYCLE FILTERING IN OPERATING EXPENDITURES")
    print("=" * 100)
    
    from sqlalchemy import select, func, distinct
    from app.db.database import AsyncSessionLocal, OperatingExpenditure
    
    async with AsyncSessionLocal() as session:
        # Check what cycles are in the data
        result = await session.execute(
            select(distinct(OperatingExpenditure.cycle))
            .order_by(OperatingExpenditure.cycle)
        )
        cycles = [row[0] for row in result.all()]
        print(f"Cycles in database: {cycles}")
        
        # Check counts by cycle
        for cycle in cycles:
            result = await session.execute(
                select(func.count(OperatingExpenditure.id))
                .where(OperatingExpenditure.cycle == cycle)
            )
            count = result.scalar() or 0
            print(f"  Cycle {cycle}: {count:,} records")
        
        # Check if there are records without cycle set
        result = await session.execute(
            select(func.count(OperatingExpenditure.id))
            .where(OperatingExpenditure.cycle.is_(None))
        )
        null_cycle_count = result.scalar() or 0
        print(f"  NULL cycle: {null_cycle_count:,} records")
        
        # Sample some records to see their cycle values
        result = await session.execute(
            select(OperatingExpenditure.expenditure_id, OperatingExpenditure.cycle, OperatingExpenditure.committee_id)
            .limit(10)
        )
        samples = result.all()
        print(f"\nSample records:")
        for sample in samples:
            print(f"  {sample.expenditure_id}: cycle={sample.cycle}, committee={sample.committee_id}")


async def main():
    """Run all checks"""
    await test_candidate_summary_parsing()
    await check_operating_expenditures_filtering()
    await check_cycle_filtering()
    
    print("\n" + "=" * 100)
    print("ANALYSIS COMPLETE")
    print("=" * 100)


if __name__ == "__main__":
    asyncio.run(main())

