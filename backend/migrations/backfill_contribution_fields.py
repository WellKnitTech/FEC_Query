"""
Backfill script to populate new Contribution fields from raw_data
Extracts AMNDT_IND, RPT_TP, TRAN_ID, ENTITY_TP, OTHER_ID, FILE_NUM, MEMO_CD, MEMO_TEXT
from raw_data JSON and populates the new structured columns.
"""
import asyncio
import json
import sys
from pathlib import Path
from sqlalchemy import select, update, text

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.db.database import AsyncSessionLocal, Contribution


async def backfill_contributions():
    """Backfill new Contribution fields from raw_data"""
    print("=" * 80)
    print("BACKFILLING CONTRIBUTION FIELDS FROM RAW_DATA")
    print("=" * 80)
    print()
    
    async with AsyncSessionLocal() as session:
        # Count records that need backfilling
        result = await session.execute(
            select(Contribution.id, Contribution.raw_data, Contribution.amendment_indicator)
            .where(Contribution.raw_data.isnot(None))
        )
        all_records = result.all()
        
        total_records = len(all_records)
        print(f"Found {total_records:,} contributions with raw_data")
        
        if total_records == 0:
            print("No records to backfill.")
            return
        
        # Count how many already have the new fields populated
        result = await session.execute(
            select(Contribution.id)
            .where(
                Contribution.raw_data.isnot(None),
                Contribution.amendment_indicator.isnot(None)
            )
        )
        already_populated = len(result.all())
        
        if already_populated > 0:
            print(f"{already_populated:,} records already have new fields populated")
            print(f"Will backfill {total_records - already_populated:,} records")
        
        print()
        print("Starting backfill...")
        
        # Process in batches
        batch_size = 1000
        updated_count = 0
        error_count = 0
        
        for i in range(0, total_records, batch_size):
            batch = all_records[i:i + batch_size]
            
            updates = []
            for record_id, raw_data, amendment_indicator in batch:
                # Skip if already populated
                if amendment_indicator is not None:
                    continue
                
                # Parse raw_data
                if isinstance(raw_data, str):
                    try:
                        raw_data_dict = json.loads(raw_data)
                    except:
                        error_count += 1
                        continue
                elif isinstance(raw_data, dict):
                    raw_data_dict = raw_data
                else:
                    error_count += 1
                    continue
                
                # Extract fields from raw_data
                update_data = {
                    'amendment_indicator': raw_data_dict.get('AMNDT_IND') or None,
                    'report_type': raw_data_dict.get('RPT_TP') or None,
                    'transaction_id': raw_data_dict.get('TRAN_ID') or None,
                    'entity_type': raw_data_dict.get('ENTITY_TP') or None,
                    'other_id': raw_data_dict.get('OTHER_ID') or None,
                    'file_number': raw_data_dict.get('FILE_NUM') or None,
                    'memo_code': raw_data_dict.get('MEMO_CD') or None,
                    'memo_text': raw_data_dict.get('MEMO_TEXT') or None,
                }
                
                # Only update if we have at least one field to set
                if any(v is not None for v in update_data.values()):
                    updates.append((record_id, update_data))
            
            # Bulk update this batch using executemany for better performance
            if updates:
                try:
                    # Use raw SQL for bulk update (more efficient than individual updates)
                    update_stmt = """
                        UPDATE contributions 
                        SET amendment_indicator = :amendment_indicator,
                            report_type = :report_type,
                            transaction_id = :transaction_id,
                            entity_type = :entity_type,
                            other_id = :other_id,
                            file_number = :file_number,
                            memo_code = :memo_code,
                            memo_text = :memo_text
                        WHERE id = :id
                    """
                    
                    # Prepare data for bulk update
                    bulk_data = [
                        {
                            'id': record_id,
                            **update_data
                        }
                        for record_id, update_data in updates
                    ]
                    
                    await session.execute(
                        text(update_stmt),
                        bulk_data
                    )
                    await session.commit()
                    updated_count += len(updates)
                except Exception as e:
                    await session.rollback()
                    error_count += len(updates)
                    print(f"Error updating batch: {e}")
                    # Fallback to individual updates
                    for record_id, update_data in updates:
                        try:
                            await session.execute(
                                update(Contribution)
                                .where(Contribution.id == record_id)
                                .values(**update_data)
                            )
                            await session.commit()
                            updated_count += 1
                            error_count -= 1
                        except Exception as e2:
                            print(f"Error updating record {record_id}: {e2}")
            
            # Progress update
            if (i + batch_size) % 10000 == 0 or (i + batch_size) >= total_records:
                print(f"Processed {min(i + batch_size, total_records):,} / {total_records:,} records "
                      f"({updated_count:,} updated, {error_count:,} errors)")
        
        print()
        print("=" * 80)
        print(f"Backfill complete!")
        print(f"  Total records processed: {total_records:,}")
        print(f"  Records updated: {updated_count:,}")
        print(f"  Errors: {error_count:,}")
        print(f"  Already populated: {already_populated:,}")
        print("=" * 80)


if __name__ == "__main__":
    asyncio.run(backfill_contributions())

