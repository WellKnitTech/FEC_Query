#!/usr/bin/env python3
"""
Script to fetch missing contribution dates from FEC API.
This fixes contributions that were imported without dates.
"""
import asyncio
import sys
import logging
from sqlalchemy import select, update
from sqlalchemy.orm import load_only

sys.path.insert(0, '.')

from app.db.database import AsyncSessionLocal, Contribution
from app.services.fec_client import FECClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def fetch_dates_from_api(
    candidate_id: str = None,
    batch_size: int = 100,
    dry_run: bool = False
):
    """
    Fetch missing dates from FEC API
    
    Args:
        candidate_id: Optional candidate ID to filter contributions
        batch_size: Number of contributions to process per batch
        dry_run: If True, only report what would be fetched without making changes
    """
    print("=" * 80)
    print("FETCHING MISSING DATES FROM FEC API")
    print("=" * 80)
    if dry_run:
        print("DRY RUN MODE - No changes will be made")
    print()
    
    fec_client = FECClient()
    
    async with AsyncSessionLocal() as session:
        # Build query for contributions with NULL dates
        query = select(Contribution).options(
            load_only(
                Contribution.id,
                Contribution.contribution_id,
                Contribution.contribution_date,
                Contribution.committee_id,
                Contribution.candidate_id
            )
        ).where(
            Contribution.contribution_date.is_(None)
        )
        
        if candidate_id:
            query = query.where(Contribution.candidate_id == candidate_id)
            print(f"Filtering by candidate_id: {candidate_id}")
        
        # Get total count
        count_query = select(Contribution.id).where(
            Contribution.contribution_date.is_(None)
        )
        if candidate_id:
            count_query = count_query.where(Contribution.candidate_id == candidate_id)
        
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
        not_found_count = 0
        
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
                try:
                    # Fetch directly from API using the internal method
                    # This bypasses the background task scheduling
                    # API requires committee_id or two_year_transaction_period
                    params = {}
                    if contrib.committee_id:
                        params['committee_id'] = contrib.committee_id
                    elif contrib.candidate_id:
                        # Try to get cycle from candidate or use 2026 as default
                        params['two_year_transaction_period'] = 2026
                    
                    api_response = await fec_client._make_request(
                        f"schedules/schedule_a/{contrib.contribution_id}",
                        params
                    )
                    
                    if api_response and 'results' in api_response and len(api_response['results']) > 0:
                        result = api_response['results'][0]
                        date_str = result.get('contribution_receipt_date')
                        
                        if date_str:
                            from datetime import datetime
                            try:
                                # Parse date string (format: YYYY-MM-DD)
                                date = datetime.strptime(date_str, '%Y-%m-%d')
                                updates.append({
                                    'id': contrib.id,
                                    'contribution_id': contrib.contribution_id,
                                    'date': date
                                })
                                fixed_count += 1
                            except ValueError:
                                failed_count += 1
                                logger.warning(f"Failed to parse date '{date_str}' for {contrib.contribution_id}")
                        else:
                            not_found_count += 1
                            logger.debug(f"No date in API response for {contrib.contribution_id}")
                    else:
                        not_found_count += 1
                        logger.debug(f"No API response for {contrib.contribution_id}")
                
                except Exception as e:
                    failed_count += 1
                    logger.warning(f"Failed to fetch date for {contrib.contribution_id}: {e}")
            
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
            
            # Add a small delay to avoid rate limiting
            if not dry_run:
                await asyncio.sleep(0.5)
        
        print()
        print("=" * 80)
        print("SUMMARY")
        print("=" * 80)
        print(f"Total contributions processed: {total_count:,}")
        print(f"  ✅ Fixed: {fixed_count:,}")
        print(f"  ❌ Failed: {failed_count:,}")
        print(f"  ⚠️  Not found in API: {not_found_count:,}")
        print()
        
        if dry_run:
            print("DRY RUN - No changes were made. Run without --dry-run to apply fixes.")
        else:
            print("✅ Date fixes applied!")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Fetch missing contribution dates from FEC API')
    parser.add_argument('--candidate-id', help='Filter by candidate ID', default='H6TX21301')
    parser.add_argument('--batch-size', type=int, default=100, help='Batch size for processing')
    parser.add_argument('--dry-run', action='store_true', help='Dry run mode (no changes)')
    
    args = parser.parse_args()
    
    asyncio.run(fetch_dates_from_api(
        candidate_id=args.candidate_id,
        batch_size=args.batch_size,
        dry_run=args.dry_run
    ))

