#!/usr/bin/env python3
"""
Script to fix contribution dates by extracting them from raw_data.
This fixes contributions that were imported with NULL dates.
"""
import asyncio
import sys
import json
import logging
from datetime import datetime
from sqlalchemy import select, update, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession

sys.path.insert(0, '.')

from app.db.database import AsyncSessionLocal, Contribution
from app.utils.date_utils import extract_date_from_raw_data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def fix_contribution_dates(
    candidate_id: str = None,
    committee_id: str = None,
    batch_size: int = 1000,
    dry_run: bool = False
):
    """
    Fix contribution dates by extracting from raw_data
    
    Args:
        candidate_id: Optional candidate ID to filter contributions
        committee_id: Optional committee ID to filter contributions
        batch_size: Number of contributions to process per batch
        dry_run: If True, only report what would be fixed without making changes
    """
    print("=" * 80)
    print("FIXING CONTRIBUTION DATES FROM raw_data")
    print("=" * 80)
    if dry_run:
        print("DRY RUN MODE - No changes will be made")
    print()
    
    async with AsyncSessionLocal() as session:
        # Build query for contributions with NULL dates - use load_only to avoid schema issues
        from sqlalchemy.orm import load_only
        query = select(Contribution).options(
            load_only(
                Contribution.id,
                Contribution.contribution_id,
                Contribution.contribution_date,
                Contribution.raw_data
            )
        ).where(
            Contribution.contribution_date.is_(None)
        )
        
        if candidate_id:
            query = query.where(Contribution.candidate_id == candidate_id)
            print(f"Filtering by candidate_id: {candidate_id}")
        
        if committee_id:
            query = query.where(Contribution.committee_id == committee_id)
            print(f"Filtering by committee_id: {committee_id}")
        
        # Get total count
        count_query = select(Contribution.id).where(
            Contribution.contribution_date.is_(None)
        )
        if candidate_id:
            count_query = count_query.where(Contribution.candidate_id == candidate_id)
        if committee_id:
            count_query = count_query.where(Contribution.committee_id == committee_id)
        
        result = await session.execute(count_query)
        total_count = len(result.scalars().all())
        
        print(f"Found {total_count:,} contributions with NULL dates")
        print()
        
        if total_count == 0:
            print("No contributions to fix!")
            return
        
        # Process in batches
        fixed_count = 0
        failed_count = 0
        no_raw_data_count = 0
        no_date_in_raw_count = 0
        
        offset = 0
        while offset < total_count:
            # Get batch
            batch_query = query.limit(batch_size).offset(offset)
            result = await session.execute(batch_query)
            contributions = result.scalars().all()
            
            if not contributions:
                break
            
            print(f"Processing batch {offset // batch_size + 1} ({len(contributions)} contributions)...")
            
            updates = []
            for contrib in contributions:
                if not contrib.raw_data:
                    no_raw_data_count += 1
                    continue
                
                # Parse raw_data if it's a string
                raw_data = contrib.raw_data
                if isinstance(raw_data, str):
                    try:
                        raw_data = json.loads(raw_data)
                    except (json.JSONDecodeError, TypeError):
                        logger.warning(f"Failed to parse raw_data JSON for {contrib.contribution_id}")
                        failed_count += 1
                        continue
                
                if not isinstance(raw_data, dict):
                    logger.warning(f"raw_data is not a dict for {contrib.contribution_id}")
                    failed_count += 1
                    continue
                
                # Extract date from raw_data
                extracted_date = extract_date_from_raw_data(raw_data)
                
                if extracted_date:
                    if isinstance(extracted_date, datetime):
                        updates.append({
                            'id': contrib.id,
                            'contribution_id': contrib.contribution_id,
                            'date': extracted_date
                        })
                        fixed_count += 1
                    else:
                        logger.warning(f"Extracted date is not datetime for {contrib.contribution_id}: {extracted_date}")
                        no_date_in_raw_count += 1
                else:
                    no_date_in_raw_count += 1
                    # Log if we have raw_data but no date
                    if 'TRANSACTION_DT' in raw_data or 'contribution_receipt_date' in raw_data:
                        logger.debug(f"No date extracted from raw_data for {contrib.contribution_id}, raw_data keys: {list(raw_data.keys())[:10]}")
            
            # Apply updates
            if updates and not dry_run:
                for update_item in updates:
                    await session.execute(
                        update(Contribution)
                        .where(Contribution.id == update_item['id'])
                        .values(contribution_date=update_item['date'])
                    )
                
                await session.commit()
                print(f"  ✅ Fixed {len(updates)} dates in this batch")
            elif updates:
                print(f"  [DRY RUN] Would fix {len(updates)} dates in this batch")
            
            offset += batch_size
        
        print()
        print("=" * 80)
        print("SUMMARY")
        print("=" * 80)
        print(f"Total contributions processed: {total_count:,}")
        print(f"  ✅ Fixed: {fixed_count:,}")
        print(f"  ❌ Failed to parse raw_data: {failed_count:,}")
        print(f"  ⚠️  No raw_data: {no_raw_data_count:,}")
        print(f"  ⚠️  No date in raw_data: {no_date_in_raw_count:,}")
        print()
        
        if dry_run:
            print("DRY RUN - No changes were made. Run without --dry-run to apply fixes.")
        else:
            print("✅ Date fixes applied!")

async def check_and_fix_candidate(candidate_id: str, dry_run: bool = False):
    """Check and fix dates for a specific candidate"""
    print(f"Checking candidate: {candidate_id}")
    print()
    
    async with AsyncSessionLocal() as session:
        # Check current state - use load_only to avoid schema issues
        from sqlalchemy.orm import load_only
        result = await session.execute(
            select(Contribution).options(
                load_only(
                    Contribution.id,
                    Contribution.contribution_id,
                    Contribution.contribution_date,
                    Contribution.raw_data
                )
            ).where(Contribution.candidate_id == candidate_id)
        )
        contributions = result.scalars().all()
        
        null_dates = [c for c in contributions if c.contribution_date is None]
        has_dates = [c for c in contributions if c.contribution_date is not None]
        
        print(f"Current state:")
        print(f"  Total contributions: {len(contributions)}")
        print(f"  With dates: {len(has_dates)}")
        print(f"  NULL dates: {len(null_dates)}")
        print()
        
        if null_dates:
            # Check if dates exist in raw_data
            dates_in_raw = 0
            for contrib in null_dates[:10]:  # Sample check
                if contrib.raw_data:
                    raw_data = contrib.raw_data
                    if isinstance(raw_data, str):
                        try:
                            raw_data = json.loads(raw_data)
                        except:
                            pass
                    if isinstance(raw_data, dict):
                        date = extract_date_from_raw_data(raw_data)
                        if date:
                            dates_in_raw += 1
            
            if dates_in_raw > 0:
                print(f"✅ Found dates in raw_data for {dates_in_raw}/10 sampled contributions")
                print("   Running fix...")
                print()
                await fix_contribution_dates(candidate_id=candidate_id, dry_run=dry_run)
            else:
                print("❌ No dates found in raw_data for sampled contributions")
                print("   These contributions may need to be re-imported")
        else:
            print("✅ All contributions already have dates!")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Fix contribution dates from raw_data')
    parser.add_argument('--candidate-id', help='Filter by candidate ID')
    parser.add_argument('--committee-id', help='Filter by committee ID')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size for processing')
    parser.add_argument('--dry-run', action='store_true', help='Dry run mode (no changes)')
    parser.add_argument('--all', action='store_true', help='Fix all contributions (not just one candidate)')
    
    args = parser.parse_args()
    
    if args.all:
        asyncio.run(fix_contribution_dates(
            batch_size=args.batch_size,
            dry_run=args.dry_run
        ))
    elif args.candidate_id:
        asyncio.run(check_and_fix_candidate(args.candidate_id, dry_run=args.dry_run))
    else:
        # Default: fix for H6TX21301
        asyncio.run(check_and_fix_candidate("H6TX21301", dry_run=args.dry_run))

