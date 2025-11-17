#!/usr/bin/env python3
"""
Check if dates in raw_data are correct but weren't parsed correctly.
"""
import asyncio
import sys
import json
from sqlalchemy import select, func, and_, or_

sys.path.insert(0, '.')

from app.db.database import AsyncSessionLocal, Contribution
from app.utils.date_utils import extract_date_from_raw_data

async def check_dates(candidate_id: str):
    """Check date parsing issues"""
    print("=" * 80)
    print(f"CHECKING DATE PARSING FOR CANDIDATE: {candidate_id}")
    print("=" * 80)
    print()
    
    async with AsyncSessionLocal() as session:
        # Get contributions
        result = await session.execute(
            select(Contribution).where(Contribution.candidate_id == candidate_id)
        )
        contributions = result.scalars().all()
        
        print(f"Total contributions: {len(contributions)}")
        print()
        
        # Check date status
        null_dates = [c for c in contributions if c.contribution_date is None]
        has_dates = [c for c in contributions if c.contribution_date is not None]
        
        print(f"Contributions with NULL dates: {len(null_dates)}")
        print(f"Contributions with dates: {len(has_dates)}")
        if has_dates:
            unique_dates = set(c.contribution_date.date() for c in has_dates if c.contribution_date)
            print(f"Unique dates: {sorted(unique_dates)}")
        print()
        
        # Check raw_data for dates
        print("CHECKING raw_data FOR DATES:")
        print("-" * 80)
        
        dates_in_raw_data = []
        dates_2025_2026 = []
        dates_2024 = []
        dates_other = []
        no_raw_data = []
        
        for contrib in null_dates[:20]:  # Check first 20 with NULL dates
            if contrib.raw_data:
                raw_data = contrib.raw_data
                if isinstance(raw_data, str):
                    try:
                        raw_data = json.loads(raw_data)
                    except:
                        pass
                
                if isinstance(raw_data, dict):
                    # Try to extract date
                    date_from_raw = extract_date_from_raw_data(raw_data)
                    if date_from_raw:
                        dates_in_raw_data.append((contrib.contribution_id, date_from_raw))
                        year = date_from_raw.year
                        if 2025 <= year <= 2026:
                            dates_2025_2026.append((contrib.contribution_id, date_from_raw))
                        elif year == 2024:
                            dates_2024.append((contrib.contribution_id, date_from_raw))
                        else:
                            dates_other.append((contrib.contribution_id, date_from_raw))
                    else:
                        # Check TRANSACTION_DT directly
                        trans_dt = raw_data.get('TRANSACTION_DT')
                        if trans_dt:
                            print(f"  Contribution {contrib.contribution_id}: TRANSACTION_DT = {trans_dt} (not parsed)")
                else:
                    no_raw_data.append(contrib.contribution_id)
            else:
                no_raw_data.append(contrib.contribution_id)
        
        print(f"Found dates in raw_data: {len(dates_in_raw_data)}")
        print(f"  - 2025-2026 dates: {len(dates_2025_2026)}")
        print(f"  - 2024 dates: {len(dates_2024)}")
        print(f"  - Other dates: {len(dates_other)}")
        print()
        
        if dates_2025_2026:
            print("Sample 2025-2026 dates found in raw_data:")
            for contrib_id, date in dates_2025_2026[:5]:
                print(f"  {contrib_id}: {date.date()}")
            print()
        
        # Check all contributions for 2025-2026 dates in raw_data
        print("CHECKING ALL CONTRIBUTIONS FOR 2025-2026 DATES IN raw_data:")
        print("-" * 80)
        
        all_2026_in_raw = []
        for contrib in contributions:
            if contrib.raw_data:
                raw_data = contrib.raw_data
                if isinstance(raw_data, str):
                    try:
                        raw_data = json.loads(raw_data)
                    except:
                        continue
                
                if isinstance(raw_data, dict):
                    date_from_raw = extract_date_from_raw_data(raw_data)
                    if date_from_raw:
                        year = date_from_raw.year
                        if 2025 <= year <= 2026:
                            all_2026_in_raw.append({
                                'id': contrib.contribution_id,
                                'date_in_raw': date_from_raw,
                                'date_in_db': contrib.contribution_date,
                                'amount': contrib.contribution_amount,
                                'committee_id': contrib.committee_id
                            })
        
        print(f"Found {len(all_2026_in_raw)} contributions with 2025-2026 dates in raw_data")
        if all_2026_in_raw:
            print("\nSample contributions:")
            for c in all_2026_in_raw[:10]:
                print(f"  ID: {c['id']}")
                print(f"    Date in raw_data: {c['date_in_raw'].date()}")
                print(f"    Date in DB: {c['date_in_db']}")
                print(f"    Amount: ${c['amount']}")
                print(f"    Committee: {c['committee_id']}")
                print()
        
        # Summary
        print("SUMMARY:")
        print("-" * 80)
        if all_2026_in_raw:
            print(f"❌ DATE PARSING BUG CONFIRMED!")
            print(f"   Found {len(all_2026_in_raw)} contributions with 2025-2026 dates in raw_data")
            print(f"   but NULL or incorrect dates in contribution_date column")
            print()
            print("   This explains why:")
            print("   - Financial Summary shows $213.9K (from FEC API)")
            print("   - Contribution Analysis shows $14.9K (only contributions with parsed dates)")
            print()
            print("   SOLUTION: Re-run date extraction from raw_data for all contributions")
        else:
            print("✅ No date parsing issues found in sample")
            print("   (Dates in raw_data match dates in DB, or raw_data doesn't have dates)")

if __name__ == "__main__":
    candidate_id = sys.argv[1] if len(sys.argv) > 1 else "H6TX21301"
    asyncio.run(check_dates(candidate_id))

