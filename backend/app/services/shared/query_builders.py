"""Query builders for common database query patterns"""
import logging
from typing import Optional, List
from datetime import datetime
from sqlalchemy import and_, or_, select
from sqlalchemy.sql import Select

from app.db.database import Contribution, Committee, AsyncSessionLocal
from app.services.shared.cycle_utils import convert_cycle_to_date_range, should_convert_cycle

logger = logging.getLogger(__name__)


async def build_candidate_condition(candidate_id: str, fec_client=None):
    """
    Build a SQLAlchemy condition that matches contributions for a candidate.
    
    This includes:
    1. Contributions with candidate_id set to the given value
    2. Contributions with committee_id from committees linked to this candidate
    
    This ensures bulk-imported contributions (which may only have committee_id)
    are found when querying by candidate_id.
    
    Args:
        candidate_id: The candidate ID to query for
        fec_client: Optional FECClient to fetch committees from API if not in DB
        
    Returns:
        SQLAlchemy condition (OR clause) that matches contributions for this candidate
    """
    # Start with direct candidate_id match
    candidate_condition = Contribution.candidate_id == candidate_id
    
    # Get committees linked to this candidate from database
    committee_ids = []
    try:
        async with AsyncSessionLocal() as session:
            # SQLite JSON contains works with JSON arrays
            # Try multiple approaches for compatibility
            result = await session.execute(
                select(Committee.committee_id)
                .where(Committee.candidate_ids.contains([candidate_id]))
            )
            committee_ids = [row[0] for row in result]
            if committee_ids:
                logger.info(f"Found {len(committee_ids)} committees in DB for candidate {candidate_id}: {committee_ids[:5]}{'...' if len(committee_ids) > 5 else ''}")
            else:
                logger.debug(f"No committees found in DB for candidate {candidate_id} (candidate_ids field may be empty or null)")
    except Exception as e:
        logger.warning(f"Error fetching committees from DB for candidate {candidate_id}: {e}", exc_info=True)
    
    # If no committees found in DB and FEC client provided, try API
    if not committee_ids and fec_client:
        try:
            committees = await fec_client.get_committees(candidate_id=candidate_id, limit=100)
            if committees:
                committee_ids = [c.get('committee_id') for c in committees if c.get('committee_id')]
                logger.debug(f"Found {len(committee_ids)} committees for candidate {candidate_id} via FEC API")
        except Exception as e:
            logger.debug(f"Error fetching committees from API for candidate {candidate_id}: {e}")
    
    if committee_ids:
        # Add OR condition for committee IDs
        committee_condition = Contribution.committee_id.in_(committee_ids)
        logger.info(f"Building query for candidate {candidate_id}: will match contributions with candidate_id={candidate_id} OR committee_id in {len(committee_ids)} committees")
        return or_(candidate_condition, committee_condition)
    else:
        logger.info(f"No committees found for candidate {candidate_id}, using direct candidate_id match only")
        return candidate_condition


class ContributionQueryBuilder:
    """Builder for contribution queries with common filtering patterns"""
    
    def __init__(self):
        self.conditions: List = []
        self.date_conditions: List = []
        self._cycle: Optional[int] = None
        self._min_date: Optional[str] = None
        self._max_date: Optional[str] = None
        self._candidate_id: Optional[str] = None
        self._committee_ids: Optional[List[str]] = None
        self._candidate_condition_added: bool = False
    
    def with_candidate(self, candidate_id: Optional[str]) -> 'ContributionQueryBuilder':
        """
        Add candidate_id filter.
        
        When candidate_id is provided, this will match contributions that:
        1. Have candidate_id set to the given value, OR
        2. Have committee_id that belongs to a committee linked to this candidate
        
        This ensures bulk-imported contributions (which may only have committee_id)
        are still found when querying by candidate_id.
        """
        if candidate_id:
            self._candidate_id = candidate_id
            # Don't add condition here - we'll add it in build_where_clause after fetching committees
            # This allows us to build the OR condition properly
            self._candidate_condition_added = False
        return self
    
    async def _get_committee_ids_for_candidate(self, candidate_id: str) -> List[str]:
        """Get committee IDs linked to a candidate"""
        try:
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    select(Committee.committee_id)
                    .where(Committee.candidate_ids.contains([candidate_id]))
                )
                committee_ids = [row[0] for row in result]
                if committee_ids:
                    logger.debug(f"Query builder found {len(committee_ids)} committees for candidate {candidate_id}")
                return committee_ids
        except Exception as e:
            logger.warning(f"Error fetching committees for candidate {candidate_id}: {e}", exc_info=True)
            return []
    
    def with_committee(self, committee_id: Optional[str]) -> 'ContributionQueryBuilder':
        """Add committee_id filter"""
        if committee_id:
            self.conditions.append(Contribution.committee_id == committee_id)
        return self
    
    def with_dates(
        self,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None,
        cycle: Optional[int] = None
    ) -> 'ContributionQueryBuilder':
        """
        Add date filters with cycle handling.
        
        If cycle is specified, also include contributions without dates (they belong to this cycle).
        """
        self._cycle = cycle
        self._min_date = min_date
        self._max_date = max_date
        
        # Convert cycle to date range if provided and no explicit dates given
        if should_convert_cycle(cycle, min_date, max_date):
            min_date, max_date = convert_cycle_to_date_range(cycle)
            self._min_date = min_date
            self._max_date = max_date
        
        # Build date conditions
        if min_date and max_date:
            try:
                min_date_obj = datetime.strptime(min_date, "%Y-%m-%d")
                max_date_obj = datetime.strptime(max_date, "%Y-%m-%d")
                # If cycle is specified, include contributions without dates OR within date range
                if cycle:
                    self.date_conditions.append(
                        or_(
                            and_(
                                Contribution.contribution_date >= min_date_obj,
                                Contribution.contribution_date <= max_date_obj
                            ),
                            Contribution.contribution_date.is_(None)
                        )
                    )
                else:
                    # No cycle specified, only include contributions with dates in range
                    self.date_conditions.append(Contribution.contribution_date >= min_date_obj)
                    self.date_conditions.append(Contribution.contribution_date <= max_date_obj)
            except ValueError:
                pass
        elif min_date:
            try:
                min_date_obj = datetime.strptime(min_date, "%Y-%m-%d")
                if cycle:
                    self.date_conditions.append(
                        or_(
                            Contribution.contribution_date >= min_date_obj,
                            Contribution.contribution_date.is_(None)
                        )
                    )
                else:
                    self.date_conditions.append(Contribution.contribution_date >= min_date_obj)
            except ValueError:
                pass
        elif max_date:
            try:
                max_date_obj = datetime.strptime(max_date, "%Y-%m-%d")
                if cycle:
                    self.date_conditions.append(
                        or_(
                            Contribution.contribution_date <= max_date_obj,
                            Contribution.contribution_date.is_(None)
                        )
                    )
                else:
                    self.date_conditions.append(Contribution.contribution_date <= max_date_obj)
            except ValueError:
                pass
        
        return self
    
    async def build_where_clause(self):
        """
        Build the final WHERE clause combining all conditions.
        
        If candidate_id was provided, also includes contributions from committees
        linked to that candidate (to handle bulk-imported data that may only have committee_id).
        """
        # If we have a candidate_id, also fetch committee IDs and add them to the query
        if self._candidate_id and not self._candidate_condition_added:
            self._committee_ids = await self._get_committee_ids_for_candidate(self._candidate_id)
            candidate_condition = Contribution.candidate_id == self._candidate_id
            
            if self._committee_ids:
                # Add OR condition: candidate_id matches OR committee_id is in the list
                committee_condition = Contribution.committee_id.in_(self._committee_ids)
                # Add the combined condition
                self.conditions.append(or_(candidate_condition, committee_condition))
                logger.debug(f"Added {len(self._committee_ids)} committees to candidate query for {self._candidate_id}")
            else:
                # No committees found, just use candidate_id condition
                self.conditions.append(candidate_condition)
                logger.debug(f"No committees found for candidate {self._candidate_id}, using direct candidate_id match only")
            
            self._candidate_condition_added = True
        
        all_conditions = self.conditions + self.date_conditions
        if all_conditions:
            return and_(*all_conditions)
        elif self.conditions:
            return and_(*self.conditions)
        else:
            return True
    
    def build_where_clause_sync(self):
        """
        Build WHERE clause synchronously (for cases where async is not available).
        This version only uses direct candidate_id matching, not committee lookups.
        """
        all_conditions = self.conditions + self.date_conditions
        if all_conditions:
            return and_(*all_conditions)
        elif self.conditions:
            return and_(*self.conditions)
        else:
            return True
    
    def reset(self) -> 'ContributionQueryBuilder':
        """Reset all conditions"""
        self.conditions = []
        self.date_conditions = []
        self._cycle = None
        self._min_date = None
        self._max_date = None
        self._candidate_id = None
        self._committee_ids = None
        self._candidate_condition_added = False
        return self

