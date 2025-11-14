"""
Service to backfill candidate_id in contributions using committee linkages.

This fixes contributions that were imported without candidate_id by looking up
the candidate_id from the Committee table's candidate_ids field, or by querying
the FEC API if committee linkages aren't in the database.
"""

from app.db.database import AsyncSessionLocal, Contribution, Committee
from sqlalchemy import select, update
from app.services.fec_client import FECClient
import logging
from typing import Optional, Dict

logger = logging.getLogger(__name__)


async def backfill_candidate_ids_from_committees(
    batch_size: int = 10000,
    limit: Optional[int] = None
) -> dict:
    """
    Backfill candidate_id in contributions using committee linkages.
    
    Args:
        batch_size: Number of contributions to update per batch
        limit: Optional limit on total number of contributions to update
    
    Returns:
        Dictionary with update statistics
    """
    async with AsyncSessionLocal() as session:
        # Get all unique committee_ids that have contributions without candidate_id
        result = await session.execute(
            select(Contribution.committee_id)
            .where(
                ((Contribution.candidate_id.is_(None)) | (Contribution.candidate_id == '')),
                Contribution.committee_id.isnot(None),
                Contribution.committee_id != ''
            )
            .distinct()
        )
        committee_ids = [row[0] for row in result]
        
        logger.info(f"Found {len(committee_ids)} committees with contributions missing candidate_id")
        
        if not committee_ids:
            return {
                "committees_processed": 0,
                "contributions_updated": 0,
                "committees_with_linkages": 0
            }
        
        # Get committee-to-candidate mappings from database
        result = await session.execute(
            select(Committee.committee_id, Committee.candidate_ids)
            .where(Committee.committee_id.in_(committee_ids))
        )
        committee_to_candidates = {}
        committees_needing_api_lookup = []
        
        for row in result:
            if row.candidate_ids and len(row.candidate_ids) > 0:
                # Use first candidate_id (most committees have one primary candidate)
                committee_to_candidates[row.committee_id] = row.candidate_ids[0]
            else:
                # Need to fetch from API
                committees_needing_api_lookup.append(row.committee_id)
        
        # If we have committees without linkages, try multiple approaches to find candidate_id
        if committees_needing_api_lookup:
            logger.info(f"Finding candidate linkages for {len(committees_needing_api_lookup)} committees")
            fec_client = FECClient()
            
            for comm_id in committees_needing_api_lookup:
                candidate_id_found = None
                
                # Approach 1: Check if any contributions for this committee already have candidate_id
                try:
                    contrib_result = await session.execute(
                        select(Contribution.candidate_id)
                        .where(
                            Contribution.committee_id == comm_id,
                            Contribution.candidate_id.isnot(None),
                            Contribution.candidate_id != ''
                        )
                        .limit(1)
                    )
                    contrib_with_candidate = contrib_result.scalar_one_or_none()
                    if contrib_with_candidate:
                        candidate_id_found = contrib_with_candidate
                        logger.debug(f"Found candidate_id {candidate_id_found} from existing contributions for committee {comm_id}")
                except Exception as e:
                    logger.debug(f"Error checking contributions for committee {comm_id}: {e}")
                
                # Approach 2: Query FEC API for committee details
                if not candidate_id_found:
                    try:
                        committees = await fec_client.get_committees(committee_id=comm_id, limit=1)
                        if committees and len(committees) > 0:
                            committee_data = committees[0]
                            # Try multiple ways to get candidate_id
                            candidate_ids = committee_data.get('candidate_ids', [])
                            if not candidate_ids and committee_data.get('raw_data'):
                                # Try extracting from raw_data
                                raw_data = committee_data.get('raw_data', {})
                                if isinstance(raw_data, dict):
                                    # FEC API might return candidate_id directly
                                    if raw_data.get('candidate_ids'):
                                        candidate_ids = raw_data['candidate_ids']
                                    elif raw_data.get('candidate_id'):
                                        candidate_ids = [raw_data['candidate_id']]
                            
                            if candidate_ids and len(candidate_ids) > 0:
                                candidate_id_found = candidate_ids[0]
                                
                                # Update the Committee record in database for future use
                                await session.execute(
                                    update(Committee)
                                    .where(Committee.committee_id == comm_id)
                                    .values(candidate_ids=candidate_ids)
                                )
                                await session.commit()
                                logger.debug(f"Updated committee {comm_id} with candidate_ids: {candidate_ids} from API")
                    except Exception as e:
                        logger.debug(f"Error fetching committee {comm_id} from API: {e}")
                
                # Approach 3: Query contributions from API for this committee to find candidate_id
                if not candidate_id_found:
                    try:
                        # Query a few contributions from API to see if they have candidate_id
                        contributions = await fec_client.get_contributions(committee_id=comm_id, limit=10)
                        for contrib in contributions:
                            cand_id = contrib.get('candidate_id')
                            if cand_id:
                                candidate_id_found = cand_id
                                logger.debug(f"Found candidate_id {candidate_id_found} from API contributions for committee {comm_id}")
                                break
                    except Exception as e:
                        logger.debug(f"Error querying contributions for committee {comm_id}: {e}")
                
                if candidate_id_found:
                    committee_to_candidates[comm_id] = candidate_id_found
        
        db_linkages = len(committee_to_candidates) - len([c for c in committee_to_candidates.keys() if c in committees_needing_api_lookup])
        api_linkages = len([c for c in committee_to_candidates.keys() if c in committees_needing_api_lookup])
        logger.info(f"Found {len(committee_to_candidates)} committees with candidate linkages ({db_linkages} from DB, {api_linkages} from API)")
        
        if not committee_to_candidates:
            logger.warning("No committee linkages found in database or API. Contributions may not be linked to candidates.")
            return {
                "committees_processed": len(committee_ids),
                "contributions_updated": 0,
                "committees_with_linkages": 0
            }
        
        # Update contributions in batches
        total_updated = 0
        committees_processed = 0
        
        for committee_id, candidate_id in committee_to_candidates.items():
            if limit and total_updated >= limit:
                break
                
            # Count how many contributions need updating for this committee
            count_result = await session.execute(
                select(Contribution.id)
                .where(
                    Contribution.committee_id == committee_id,
                    ((Contribution.candidate_id.is_(None)) | (Contribution.candidate_id == ''))
                )
                .limit(batch_size + 1)  # Check if there are more
            )
            contributions_to_update = count_result.scalars().all()
            
            if not contributions_to_update:
                continue
            
            # Update in batches
            batch_limit = min(batch_size, len(contributions_to_update))
            if limit:
                batch_limit = min(batch_limit, limit - total_updated)
            
            result = await session.execute(
                update(Contribution)
                .where(
                    Contribution.committee_id == committee_id,
                    ((Contribution.candidate_id.is_(None)) | (Contribution.candidate_id == ''))
                )
                .values(candidate_id=candidate_id)
                .execution_options(synchronize_session=False)
            )
            
            updated_count = result.rowcount
            total_updated += updated_count
            committees_processed += 1
            
            if updated_count > 0:
                logger.info(f"Updated {updated_count} contributions for committee {committee_id} with candidate_id {candidate_id}")
            
            # Commit after each committee to avoid long transactions
            await session.commit()
            
            if limit and total_updated >= limit:
                break
        
        logger.info(f"Backfill complete: Updated {total_updated} contributions across {committees_processed} committees")
        
        return {
            "committees_processed": committees_processed,
            "contributions_updated": total_updated,
            "committees_with_linkages": len(committee_to_candidates)
        }


async def get_backfill_stats() -> dict:
    """Get statistics about contributions missing candidate_id"""
    async with AsyncSessionLocal() as session:
        # Count contributions without candidate_id
        result = await session.execute(
            select(Contribution.id)
            .where(
                ((Contribution.candidate_id.is_(None)) | (Contribution.candidate_id == '')),
                Contribution.committee_id.isnot(None),
                Contribution.committee_id != ''
            )
        )
        missing_count = len(result.scalars().all())
        
        # Count unique committees with missing candidate_id
        result = await session.execute(
            select(Contribution.committee_id)
            .where(
                ((Contribution.candidate_id.is_(None)) | (Contribution.candidate_id == '')),
                Contribution.committee_id.isnot(None),
                Contribution.committee_id != ''
            )
            .distinct()
        )
        committees_affected = len(result.scalars().all())
        
        return {
            "contributions_missing_candidate_id": missing_count,
            "committees_affected": committees_affected
        }

