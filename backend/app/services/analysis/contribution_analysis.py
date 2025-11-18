"""Contribution analysis service"""
import pandas as pd
import re
import logging
from typing import Optional
from datetime import datetime
from sqlalchemy import select, func, and_

from app.db.database import AsyncSessionLocal, Contribution
from app.services.fec_client import FECClient
from app.models.schemas import (
    ContributionAnalysis, EmployerAnalysis, ContributionVelocity, CumulativeTotals
)
from app.services.shared.query_builders import ContributionQueryBuilder
from app.services.shared.cycle_utils import convert_cycle_to_date_range, should_convert_cycle
from app.services.shared.aggregation_helpers import calculate_distribution_bins

logger = logging.getLogger(__name__)


class ContributionAnalysisService:
    """Service for contribution analysis"""
    
    def __init__(self, fec_client: FECClient):
        self.fec_client = fec_client
    
    def _normalize_employer_name(self, employer: str) -> str:
        """Normalize employer name for better aggregation"""
        if not employer or pd.isna(employer):
            return 'Unknown Employer'
        
        # Convert to string and strip whitespace
        normalized = str(employer).strip()
        
        # Convert to uppercase for consistent comparison
        normalized = normalized.upper()
        
        # Remove common business suffixes and variations (with optional comma before)
        # This handles cases like "COMPANY, INC.", "COMPANY INC", "COMPANY, INC", etc.
        normalized = re.sub(r',?\s*(INC|LLC|CORP|LTD|CO|CORPORATION|COMPANY|INCORPORATED)\.?$', '', normalized, flags=re.IGNORECASE)
        
        # Remove all punctuation (commas, periods, etc.)
        normalized = re.sub(r'[^\w\s]', '', normalized)
        
        # Normalize whitespace (multiple spaces to single space)
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        
        return normalized if normalized else 'Unknown Employer'
    
    async def analyze_contributions(
        self,
        candidate_id: Optional[str] = None,
        committee_id: Optional[str] = None,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None,
        cycle: Optional[int] = None
    ) -> ContributionAnalysis:
        """Analyze contributions with aggregations using efficient SQL queries"""
        try:
            async with AsyncSessionLocal() as session:
                # Convert cycle to date range if provided and no explicit dates given
                if should_convert_cycle(cycle, min_date, max_date):
                    min_date, max_date = convert_cycle_to_date_range(cycle)
                    logger.debug(f"analyze_contributions: Converted cycle {cycle} to date range: {min_date} to {max_date}")
                
                # Build query using ContributionQueryBuilder
                query_builder = ContributionQueryBuilder()
                query_builder.with_candidate(candidate_id).with_committee(committee_id).with_dates(min_date, max_date, cycle)
                where_clause = query_builder.build_where_clause()
                
                # Check if we have any contributions for this candidate (for debugging)
                if candidate_id:
                    total_check_query = select(func.count(Contribution.id)).where(
                        Contribution.candidate_id == candidate_id
                    )
                    total_check_result = await session.execute(total_check_query)
                    total_count = total_check_result.scalar() or 0
                    logger.debug(f"analyze_contributions: Found {total_count} total contributions for candidate {candidate_id}")
                    
                    # Check contributions with dates
                    dated_check_query = select(func.count(Contribution.id)).where(
                        and_(
                            Contribution.candidate_id == candidate_id,
                            Contribution.contribution_date.isnot(None)
                        )
                    )
                    dated_check_result = await session.execute(dated_check_query)
                    dated_count = dated_check_result.scalar() or 0
                    logger.debug(f"analyze_contributions: Found {dated_count} contributions with dates for candidate {candidate_id}")
                
                # Get total contributions and count using aggregation
                total_query = select(
                    func.sum(Contribution.contribution_amount).label('total'),
                    func.count(Contribution.id).label('count'),
                    func.count(func.distinct(Contribution.contributor_name)).label('unique_donors')
                ).where(
                    and_(
                        where_clause,
                        Contribution.contribution_amount.isnot(None)
                    )
                )
                
                total_result = await session.execute(total_query)
                total_row = total_result.first()
                
                total_contributions = float(total_row.total) if total_row.total else 0.0
                total_count = int(total_row.count) if total_row.count else 0
                total_contributors = int(total_row.unique_donors) if total_row.unique_donors else 0
                average_contribution = total_contributions / total_count if total_count > 0 else 0.0
                
                # Contributions by date (aggregated)
                date_query = select(
                    func.date(Contribution.contribution_date).label('date'),
                    func.sum(Contribution.contribution_amount).label('amount')
                ).where(
                    and_(
                        where_clause,
                        Contribution.contribution_date.isnot(None),
                        Contribution.contribution_amount.isnot(None)
                    )
                ).group_by(func.date(Contribution.contribution_date))
                
                date_result = await session.execute(date_query)
                contributions_by_date = {
                    str(row.date): float(row.amount)
                    for row in date_result
                    if row.date
                }
                
                # Contributions by state (aggregated)
                state_query = select(
                    Contribution.contributor_state.label('state'),
                    func.sum(Contribution.contribution_amount).label('amount')
                ).where(
                    and_(
                        where_clause,
                        Contribution.contributor_state.isnot(None),
                        Contribution.contribution_amount.isnot(None)
                    )
                ).group_by(Contribution.contributor_state)
                
                state_result = await session.execute(state_query)
                contributions_by_state = {
                    row.state: float(row.amount)
                    for row in state_result
                    if row.state
                }
                
                # Top donors (aggregated)
                top_donors_query = select(
                    Contribution.contributor_name.label('name'),
                    func.sum(Contribution.contribution_amount).label('total'),
                    func.count(Contribution.id).label('count')
                ).where(
                    and_(
                        where_clause,
                        Contribution.contributor_name.isnot(None),
                        Contribution.contribution_amount.isnot(None)
                    )
                ).group_by(Contribution.contributor_name).order_by(
                    func.sum(Contribution.contribution_amount).desc()
                ).limit(20)
                
                top_donors_result = await session.execute(top_donors_query)
                top_donors = [
                    {
                        'name': row.name,
                        'total': float(row.total),
                        'count': int(row.count)
                    }
                    for row in top_donors_result
                    if row.name
                ]
                
                # Contribution distribution (bins) - need to fetch amounts for binning
                amount_query = select(Contribution.contribution_amount).where(
                    and_(
                        where_clause,
                        Contribution.contribution_amount.isnot(None),
                        Contribution.contribution_amount > 0
                    )
                ).limit(10000)  # Limit for binning calculation
                
                amount_result = await session.execute(amount_query)
                amounts = [float(row.contribution_amount) for row in amount_result if row.contribution_amount]
                
                # Calculate distribution bins
                contribution_distribution = calculate_distribution_bins(amounts) if amounts else {}
                
                return ContributionAnalysis(
                    total_contributions=total_contributions,
                    total_contributors=total_contributors,
                    average_contribution=average_contribution,
                    contributions_by_date=contributions_by_date,
                    contributions_by_state=contributions_by_state,
                    top_donors=top_donors,
                    contribution_distribution=contribution_distribution
                )
        except Exception as e:
            logger.warning(f"Error in optimized analyze_contributions, falling back to pandas method: {e}", exc_info=True)
            # Fallback to original pandas-based method
            contributions = await self.fec_client.get_contributions(
                candidate_id=candidate_id,
                committee_id=committee_id,
                min_date=min_date,
                max_date=max_date,
                limit=10000,
                two_year_transaction_period=cycle
            )
            
            if not contributions:
                return ContributionAnalysis(
                    total_contributions=0.0,
                    total_contributors=0,
                    average_contribution=0.0,
                    contributions_by_date={},
                    contributions_by_state={},
                    top_donors=[],
                    contribution_distribution={}
                )
            
            df = pd.DataFrame(contributions)
            
            # Ensure required columns exist with defaults
            if 'contribution_amount' not in df.columns:
                df['contribution_amount'] = 0.0
            if 'contributor_name' not in df.columns:
                df['contributor_name'] = 'Unknown'
            if 'contribution_date' not in df.columns:
                df['contribution_date'] = None
            if 'contributor_state' not in df.columns:
                df['contributor_state'] = None
            
            # Convert contribution_amount to float, handling None, strings, and other types
            df['contribution_amount'] = pd.to_numeric(df['contribution_amount'], errors='coerce').fillna(0.0)
            
            # Calculate totals
            total_contributions = df['contribution_amount'].sum()
            total_contributors = df['contributor_name'].fillna('Unknown').nunique()
            average_contribution = total_contributions / len(df) if len(df) > 0 else 0.0
            
            # Contributions by date
            df['date'] = pd.to_datetime(df['contribution_date'], errors='coerce')
            date_grouped = df[df['date'].notna()].groupby(df['date'].dt.date)['contribution_amount'].sum()
            contributions_by_date = {str(k): float(v) for k, v in date_grouped.items() if k is not None}
            
            # Contributions by state
            state_grouped = df[df['contributor_state'].notna()].groupby('contributor_state')['contribution_amount'].sum()
            contributions_by_state = {k: float(v) for k, v in state_grouped.items()}
            
            # Top donors
            if len(df) > 0:
                top_donors_df = df.groupby('contributor_name').agg({
                    'contribution_amount': ['sum', 'count']
                }).reset_index()
                top_donors_df.columns = ['name', 'total', 'count']
                top_donors_df = top_donors_df.sort_values('total', ascending=False).head(20)
                top_donors = top_donors_df.to_dict('records')
            else:
                top_donors = []
            
            # Contribution distribution (bins)
            amounts = df['contribution_amount'].fillna(0).tolist()
            contribution_distribution = calculate_distribution_bins(amounts) if amounts else {}
            
            return ContributionAnalysis(
                total_contributions=float(total_contributions),
                total_contributors=int(total_contributors),
                average_contribution=float(average_contribution),
                contributions_by_date=contributions_by_date,
                contributions_by_state=contributions_by_state,
                top_donors=top_donors,
                contribution_distribution=contribution_distribution
            )
    
    async def analyze_by_employer(
        self,
        candidate_id: Optional[str] = None,
        committee_id: Optional[str] = None,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None,
        cycle: Optional[int] = None
    ) -> EmployerAnalysis:
        """Analyze contributions by employer with name normalization using efficient SQL queries"""
        try:
            # Convert cycle to date range if provided
            if should_convert_cycle(cycle, min_date, max_date):
                min_date, max_date = convert_cycle_to_date_range(cycle)
            
            async with AsyncSessionLocal() as session:
                # Build base query conditions
                conditions = []
                if candidate_id:
                    conditions.append(Contribution.candidate_id == candidate_id)
                if committee_id:
                    conditions.append(Contribution.committee_id == committee_id)
                if min_date:
                    try:
                        min_date_obj = datetime.strptime(min_date, "%Y-%m-%d")
                        conditions.append(Contribution.contribution_date >= min_date_obj)
                    except ValueError:
                        pass
                if max_date:
                    try:
                        max_date_obj = datetime.strptime(max_date, "%Y-%m-%d")
                        conditions.append(Contribution.contribution_date <= max_date_obj)
                    except ValueError:
                        pass
                
                where_clause = and_(*conditions) if conditions else True
                
                # Get total contributions (for total_contributions field)
                total_query = select(
                    func.sum(Contribution.contribution_amount).label('total')
                ).where(
                    and_(
                        where_clause,
                        Contribution.contribution_amount.isnot(None)
                    )
                )
                total_result = await session.execute(total_query)
                total_row = total_result.first()
                total_contributions = float(total_row.total) if total_row.total else 0.0
                
                # Get employer breakdown using SQL aggregation
                employer_query = select(
                    Contribution.contributor_employer.label('employer'),
                    func.sum(Contribution.contribution_amount).label('total'),
                    func.count(Contribution.id).label('count')
                ).where(
                    and_(
                        where_clause,
                        Contribution.contributor_employer.isnot(None),
                        Contribution.contribution_amount.isnot(None)
                    )
                ).group_by(Contribution.contributor_employer)
                
                employer_result = await session.execute(employer_query)
                employer_rows = employer_result.all()
                
                if not employer_rows:
                    return EmployerAnalysis(
                        total_by_employer={},
                        top_employers=[],
                        employer_count=0,
                        total_contributions=total_contributions
                    )
                
                # Convert to DataFrame for normalization
                employer_data = [
                    {
                        'employer': row.employer,
                        'total': float(row.total),
                        'count': int(row.count)
                    }
                    for row in employer_rows
                    if row.employer
                ]
                
                df = pd.DataFrame(employer_data)
                
                # Normalize employer names for better aggregation
                df['normalized_employer'] = df['employer'].apply(self._normalize_employer_name)
                
                # Group by normalized employer name and aggregate
                employer_grouped = df.groupby('normalized_employer').agg({
                    'total': 'sum',
                    'count': 'sum',
                    'employer': 'first'  # Keep first original name for display
                }).reset_index()
                employer_grouped.columns = ['normalized_employer', 'total', 'count', 'display_name']
                employer_grouped = employer_grouped.sort_values('total', ascending=False)
                
                # Use display name (original) for the output, but grouping was done on normalized name
                total_by_employer = {row['display_name']: float(row['total']) for _, row in employer_grouped.iterrows()}
                top_employers = [
                    {
                        'employer': row['display_name'],
                        'total': float(row['total']),
                        'count': int(row['count'])
                    }
                    for _, row in employer_grouped.head(50).iterrows()
                ]
                
                return EmployerAnalysis(
                    total_by_employer=total_by_employer,
                    top_employers=top_employers,
                    employer_count=int(employer_grouped['normalized_employer'].nunique()),
                    total_contributions=total_contributions
                )
        except Exception as e:
            logger.warning(f"Error in optimized analyze_by_employer, falling back to pandas method: {e}", exc_info=True)
            # Fallback to original pandas-based method
            contributions = await self.fec_client.get_contributions(
                candidate_id=candidate_id,
                committee_id=committee_id,
                min_date=min_date,
                max_date=max_date,
                limit=10000
            )
            
            if not contributions:
                return EmployerAnalysis(
                    total_by_employer={},
                    top_employers=[],
                    employer_count=0,
                    total_contributions=0.0
                )
            
            df = pd.DataFrame(contributions)
            
            # Ensure required columns exist
            if 'contribution_amount' not in df.columns:
                df['contribution_amount'] = 0.0
            if 'contributor_employer' not in df.columns:
                df['contributor_employer'] = None
            
            # Convert contribution_amount to float, handling None, strings, and other types
            df['contribution_amount'] = pd.to_numeric(df['contribution_amount'], errors='coerce').fillna(0.0)
            
            # Filter out null employers
            df_with_employer = df[df['contributor_employer'].notna()].copy()
            
            if len(df_with_employer) == 0:
                return EmployerAnalysis(
                    total_by_employer={},
                    top_employers=[],
                    employer_count=0,
                    total_contributions=float(df['contribution_amount'].sum())
                )
            
            # Normalize employer names for better aggregation
            df_with_employer['normalized_employer'] = df_with_employer['contributor_employer'].apply(self._normalize_employer_name)
            
            # Group by normalized employer name
            employer_grouped = df_with_employer.groupby('normalized_employer').agg({
                'contribution_amount': ['sum', 'count'],
                'contributor_employer': 'first'  # Keep original name for display
            }).reset_index()
            employer_grouped.columns = ['employer', 'total', 'count', 'display_name']
            employer_grouped = employer_grouped.sort_values('total', ascending=False)
            
            # Use display name (original) for the output, but grouping was done on normalized name
            total_by_employer = {row['display_name']: float(row['total']) for _, row in employer_grouped.iterrows()}
            top_employers = [
                {
                    'employer': row['display_name'],
                    'total': float(row['total']),
                    'count': int(row['count'])
                }
                for _, row in employer_grouped.head(50).iterrows()
            ]
            
            return EmployerAnalysis(
                total_by_employer=total_by_employer,
                top_employers=top_employers,
                employer_count=int(employer_grouped['employer'].nunique()),
                total_contributions=float(df['contribution_amount'].sum())
            )
    
    async def analyze_velocity(
        self,
        candidate_id: Optional[str] = None,
        committee_id: Optional[str] = None,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None,
        cycle: Optional[int] = None
    ) -> ContributionVelocity:
        """Calculate contribution velocity (contributions per day/week) using efficient SQL queries"""
        try:
            # Convert cycle to date range if provided
            if should_convert_cycle(cycle, min_date, max_date):
                min_date, max_date = convert_cycle_to_date_range(cycle)
            
            async with AsyncSessionLocal() as session:
                # Build query using ContributionQueryBuilder
                query_builder = ContributionQueryBuilder()
                query_builder.with_candidate(candidate_id).with_committee(committee_id).with_dates(min_date, max_date, cycle)
                where_clause = query_builder.build_where_clause()
                
                # Get velocity by date using SQL aggregation
                # Note: For velocity, we only use contributions with dates (can't calculate velocity without dates)
                date_query = select(
                    func.date(Contribution.contribution_date).label('date'),
                    func.sum(Contribution.contribution_amount).label('amount'),
                    func.count(Contribution.id).label('count')
                ).where(
                    and_(
                        where_clause,
                        Contribution.contribution_date.isnot(None),
                        Contribution.contribution_amount.isnot(None)
                    )
                ).group_by(func.date(Contribution.contribution_date))
                
                date_result = await session.execute(date_query)
                date_rows = date_result.all()
                
                if not date_rows:
                    return ContributionVelocity(
                        velocity_by_date={},
                        velocity_by_week={},
                        peak_days=[],
                        average_daily_velocity=0.0
                    )
                
                # Convert to DataFrame for week grouping and peak day calculation
                velocity_data = [
                    {
                        'date': row.date,
                        'amount': float(row.amount),
                        'count': int(row.count)
                    }
                    for row in date_rows
                    if row.date
                ]
                
                df = pd.DataFrame(velocity_data)
                df['date'] = pd.to_datetime(df['date'])
                
                # Velocity by date
                velocity_by_date = {
                    str(row['date'].date()): float(row['amount'])
                    for _, row in df.iterrows()
                }
                
                # Velocity by week (group by week)
                df['week'] = df['date'].dt.to_period('W').astype(str)
                week_grouped = df.groupby('week')['amount'].sum()
                velocity_by_week = {str(k): float(v) for k, v in week_grouped.items()}
                
                # Peak days (top 10 by amount)
                peak_days_df = df.nlargest(10, 'amount')[['date', 'amount', 'count']]
                peak_days = [
                    {
                        'date': str(row['date'].date()),
                        'amount': float(row['amount']),
                        'count': int(row['count'])
                    }
                    for _, row in peak_days_df.iterrows()
                ]
                
                # Average daily velocity
                average_daily_velocity = float(df['amount'].mean()) if len(df) > 0 else 0.0
                
                return ContributionVelocity(
                    velocity_by_date=velocity_by_date,
                    velocity_by_week=velocity_by_week,
                    peak_days=peak_days,
                    average_daily_velocity=average_daily_velocity
                )
        except Exception as e:
            logger.warning(f"Error in optimized analyze_velocity, falling back to pandas method: {e}", exc_info=True)
            # Fallback to original pandas-based method
            contributions = await self.fec_client.get_contributions(
                candidate_id=candidate_id,
                committee_id=committee_id,
                min_date=min_date,
                max_date=max_date,
                limit=10000
            )
            
            if not contributions:
                return ContributionVelocity(
                    velocity_by_date={},
                    velocity_by_week={},
                    peak_days=[],
                    average_daily_velocity=0.0
                )
            
            df = pd.DataFrame(contributions)
            
            # Ensure required columns exist
            if 'contribution_date' not in df.columns:
                df['contribution_date'] = None
            if 'contribution_amount' not in df.columns:
                df['contribution_amount'] = 0.0
            
            # Convert contribution_amount to float, handling None, strings, and other types
            df['contribution_amount'] = pd.to_numeric(df['contribution_amount'], errors='coerce').fillna(0.0)
            
            df['date'] = pd.to_datetime(df['contribution_date'], errors='coerce')
            df = df[df['date'].notna()].copy()
            
            if len(df) == 0:
                return ContributionVelocity(
                    velocity_by_date={},
                    velocity_by_week={},
                    peak_days=[],
                    average_daily_velocity=0.0
                )
            
            # Daily velocity (amount per day)
            daily_amounts = df.groupby(df['date'].dt.date)['contribution_amount'].sum()
            velocity_by_date = {str(k): float(v) for k, v in daily_amounts.items()}
            
            # Weekly velocity
            df['week'] = df['date'].dt.to_period('W').astype(str)
            weekly_amounts = df.groupby('week')['contribution_amount'].sum()
            velocity_by_week = {k: float(v) for k, v in weekly_amounts.items()}
            
            # Peak days (top 10 by amount)
            peak_days_df = daily_amounts.sort_values(ascending=False).head(10).reset_index()
            daily_counts = df.groupby(df['date'].dt.date).size()
            peak_days = [
                {
                    'date': str(row['date']),
                    'amount': float(row['contribution_amount']),
                    'count': int(daily_counts.get(row['date'], 0))
                }
                for _, row in peak_days_df.iterrows()
            ]
            
            # Average daily velocity
            if len(velocity_by_date) > 0:
                average_daily_velocity = sum(velocity_by_date.values()) / len(velocity_by_date)
            else:
                average_daily_velocity = 0.0
            
            return ContributionVelocity(
                velocity_by_date=velocity_by_date,
                velocity_by_week=velocity_by_week,
                peak_days=peak_days,
                average_daily_velocity=float(average_daily_velocity)
            )
    
    async def get_cumulative_totals(
        self,
        candidate_id: Optional[str] = None,
        committee_id: Optional[str] = None,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None,
        cycle: Optional[int] = None
    ) -> CumulativeTotals:
        """Get cumulative contribution totals aggregated by date using efficient SQL queries"""
        try:
            async with AsyncSessionLocal() as session:
                # Convert cycle to date range if provided and no explicit dates given
                if should_convert_cycle(cycle, min_date, max_date):
                    min_date, max_date = convert_cycle_to_date_range(cycle)
                
                # Build query using ContributionQueryBuilder
                query_builder = ContributionQueryBuilder()
                query_builder.with_candidate(candidate_id).with_committee(committee_id).with_dates(min_date, max_date, cycle)
                where_clause = query_builder.build_where_clause()
                
                # Query for contributions with dates (for the timeline)
                query = select(
                    func.date(Contribution.contribution_date).label('date'),
                    func.sum(Contribution.contribution_amount).label('daily_total')
                ).where(
                    and_(
                        where_clause,
                        Contribution.contribution_date.isnot(None),
                        Contribution.contribution_amount.isnot(None)
                    )
                )
                
                # Group by date and order by date ascending
                query = query.group_by(func.date(Contribution.contribution_date)).order_by(
                    func.date(Contribution.contribution_date).asc()
                )
                
                result = await session.execute(query)
                rows = result.all()
                
                if not rows:
                    return CumulativeTotals(
                        totals_by_date={},
                        total_amount=0.0,
                        first_date=None,
                        last_date=None
                    )
                
                # Calculate cumulative totals from contributions with dates
                cumulative_total = 0.0
                totals_by_date = {}
                first_date = None
                last_date = None
                
                for row in rows:
                    date_str = row.date.strftime("%Y-%m-%d") if row.date else None
                    daily_total = float(row.daily_total) if row.daily_total else 0.0
                    
                    if date_str:
                        cumulative_total += daily_total
                        totals_by_date[date_str] = cumulative_total
                        
                        if first_date is None:
                            first_date = date_str
                        last_date = date_str
                
                # If cycle is specified, also include contributions without dates in the total
                if cycle:
                    undated_query = select(
                        func.sum(Contribution.contribution_amount).label('total')
                    ).where(
                        and_(
                            where_clause,
                            Contribution.contribution_date.is_(None),
                            Contribution.contribution_amount.isnot(None)
                        )
                    )
                    undated_result = await session.execute(undated_query)
                    undated_row = undated_result.first()
                    undated_total = float(undated_row.total) if undated_row and undated_row.total else 0.0
                    if undated_total > 0:
                        # Add undated contributions to the final total
                        cumulative_total += undated_total
                        logger.debug(f"get_cumulative_totals: Added ${undated_total:,.2f} from {cycle} contributions without dates")
                
                return CumulativeTotals(
                    totals_by_date=totals_by_date,
                    total_amount=cumulative_total,
                    first_date=first_date,
                    last_date=last_date
                )
        except Exception as e:
            logger.error(f"Error getting cumulative totals: {e}", exc_info=True)
            # Fallback to empty result
            return CumulativeTotals(
                totals_by_date={},
                total_amount=0.0,
                first_date=None,
                last_date=None
            )

