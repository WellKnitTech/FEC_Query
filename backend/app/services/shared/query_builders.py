"""Query builders for common database query patterns"""
import logging
from typing import Optional, List
from datetime import datetime
from sqlalchemy import and_, or_
from sqlalchemy.sql import Select

from app.db.database import Contribution
from app.services.shared.cycle_utils import convert_cycle_to_date_range, should_convert_cycle

logger = logging.getLogger(__name__)


class ContributionQueryBuilder:
    """Builder for contribution queries with common filtering patterns"""
    
    def __init__(self):
        self.conditions: List = []
        self.date_conditions: List = []
        self._cycle: Optional[int] = None
        self._min_date: Optional[str] = None
        self._max_date: Optional[str] = None
    
    def with_candidate(self, candidate_id: Optional[str]) -> 'ContributionQueryBuilder':
        """Add candidate_id filter"""
        if candidate_id:
            self.conditions.append(Contribution.candidate_id == candidate_id)
        return self
    
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
    
    def build_where_clause(self):
        """Build the final WHERE clause combining all conditions"""
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
        return self

