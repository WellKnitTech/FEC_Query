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
from app.utils.thread_pool import async_to_numeric, async_dataframe_operation, async_aggregation
from app.config import config

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
                where_clause = await query_builder.build_where_clause()
                
                # Check if we have any contributions for this candidate (for debugging)
                if candidate_id:
                    from app.services.shared.query_builders import build_candidate_condition
                    candidate_condition = await build_candidate_condition(candidate_id, fec_client=self.fec_client)
                    
                    # Check contributions with direct candidate_id
                    direct_query = select(func.count(Contribution.id)).where(
                        Contribution.candidate_id == candidate_id
                    )
                    direct_result = await session.execute(direct_query)
                    direct_count = direct_result.scalar() or 0
                    
                    # Check contributions via committees
                    from app.db.database import Committee
                    committee_result = await session.execute(
                        select(Committee.committee_id)
                        .where(Committee.candidate_ids.contains([candidate_id]))
                    )
                    committee_ids = [row[0] for row in committee_result]
                    committee_count = 0
                    if committee_ids:
                        committee_query = select(func.count(Contribution.id)).where(
                            Contribution.committee_id.in_(committee_ids)
                        )
                        committee_result = await session.execute(committee_query)
                        committee_count = committee_result.scalar() or 0
                    
                    # Total with combined condition
                    total_check_query = select(func.count(Contribution.id)).where(candidate_condition)
                    total_check_result = await session.execute(total_check_query)
                    total_count = total_check_result.scalar() or 0
                    
                    logger.info(
                        f"analyze_contributions for {candidate_id}: "
                        f"direct={direct_count}, via_committees={committee_count} (from {len(committee_ids)} committees), "
                        f"total={total_count}"
                    )
                    
                    # Check contributions with dates
                    dated_check_query = select(func.count(Contribution.id)).where(
                        and_(
                            candidate_condition,
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
                # Use streaming for large datasets to avoid memory issues
                # Sample up to 10k records for distribution calculation (sufficient for accurate bins)
                from app.services.shared.chunked_processor import ChunkedProcessor
                
                amount_query = select(Contribution.contribution_amount).where(
                    and_(
                        where_clause,
                        Contribution.contribution_amount.isnot(None),
                        Contribution.contribution_amount > 0
                    )
                )
                
                # Stream amounts in chunks to avoid loading all into memory
                processor = ChunkedProcessor(chunk_size=10000)
                amounts = []
                async for contribution in processor.stream_contributions(session, amount_query, max_records=10000):
                    if contribution.contribution_amount:
                        amounts.append(float(contribution.contribution_amount))
                
                # Calculate distribution bins
                contribution_distribution = calculate_distribution_bins(amounts) if amounts else {}
                
                # Get FEC API totals for comparison and data completeness calculation
                data_completeness = None
                total_from_api = None
                warning_message = None
                using_financial_totals_fallback = False
                
                if candidate_id:
                    try:
                        api_totals = await self.fec_client.get_candidate_totals(candidate_id, cycle=cycle)
                        if api_totals and len(api_totals) > 0:
                            # Find matching cycle or use first result
                            matching_total = None
                            for total in api_totals:
                                total_cycle = total.get('cycle') or total.get('two_year_transaction_period') or total.get('election_year')
                                if cycle is None or total_cycle == cycle:
                                    matching_total = total
                                    break
                            
                            if not matching_total and api_totals:
                                # Use first result if no cycle match
                                matching_total = api_totals[0]
                            
                            if matching_total:
                                # Use individual_contributions for comparison (most accurate for Schedule A data)
                                total_from_api = float(matching_total.get('individual_contributions', 0) or matching_total.get('contributions', 0) or 0)
                                
                                # Check for discrepancy: financial totals show contributions but database has none
                                if total_from_api > 0 and total_contributions == 0:
                                    warning_message = (
                                        f"Financial totals show ${total_from_api:,.2f} in individual contributions, "
                                        f"but detailed records are not yet available in the database. "
                                        f"This is normal - individual contribution records are published with a delay "
                                        f"after campaign finance filings. The financial totals are accurate, but detailed "
                                        f"donor information will appear once the FEC processes and publishes the records."
                                    )
                                    logger.info(f"Warning: Financial totals show ${total_from_api:,.2f} but database has $0.00 for candidate {candidate_id}")
                                    
                                    # Optionally use financial totals as a fallback estimate
                                    # This provides users with approximate data while waiting for detailed records
                                    using_financial_totals_fallback = True
                                    total_contributions = total_from_api
                                    # Estimate contributors based on average contribution size
                                    # Use a conservative estimate of $200 average contribution
                                    estimated_avg = 200.0
                                    total_contributors = max(1, int(total_from_api / estimated_avg))
                                    average_contribution = estimated_avg
                                    data_completeness = 0.0  # 0% because we're using estimates
                                    logger.info(f"Using financial totals as fallback estimate for candidate {candidate_id}")
                                
                                elif total_from_api > 0:
                                    # Calculate data completeness percentage
                                    data_completeness = min(100.0, (total_contributions / total_from_api) * 100.0)
                                    logger.debug(f"Data completeness: {data_completeness:.1f}% (Local: ${total_contributions:,.2f}, API: ${total_from_api:,.2f})")
                                    
                                    # Warn if completeness is very low
                                    if data_completeness < 10.0 and total_from_api > 1000:
                                        warning_message = (
                                            f"This analysis is based on {data_completeness:.1f}% of total contributions. "
                                            f"Financial totals show ${total_from_api:,.2f}, but only ${total_contributions:,.2f} "
                                            f"is available in the local database. Consider importing additional bulk data "
                                            f"or waiting for the FEC to publish detailed records."
                                        )
                                else:
                                    logger.debug(f"API total is 0, cannot calculate data completeness")
                    except Exception as e:
                        logger.warning(f"Error getting FEC API totals for data completeness: {e}")
                
                return ContributionAnalysis(
                    total_contributions=total_contributions,
                    total_contributors=total_contributors,
                    average_contribution=average_contribution,
                    contributions_by_date=contributions_by_date,
                    contributions_by_state=contributions_by_state,
                    top_donors=top_donors,
                    contribution_distribution=contribution_distribution,
                    data_completeness=data_completeness,
                    total_from_api=total_from_api,
                    warning_message=warning_message,
                    using_financial_totals_fallback=using_financial_totals_fallback
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
                # Check financial totals for fallback
                warning_message = None
                using_financial_totals_fallback = False
                total_contributions = 0.0
                total_from_api = None
                
                if candidate_id:
                    try:
                        api_totals = await self.fec_client.get_candidate_totals(candidate_id, cycle=cycle)
                        if api_totals and len(api_totals) > 0:
                            matching_total = api_totals[0]
                            total_from_api = float(matching_total.get('individual_contributions', 0) or matching_total.get('contributions', 0) or 0)
                            if total_from_api > 0:
                                warning_message = (
                                    f"Financial totals show ${total_from_api:,.2f} in individual contributions, "
                                    f"but detailed records are not yet available. This is normal - individual "
                                    f"contribution records are published with a delay after campaign finance filings."
                                )
                                using_financial_totals_fallback = True
                                total_contributions = total_from_api
                    except Exception:
                        pass
                
                return ContributionAnalysis(
                    total_contributions=total_contributions,
                    total_contributors=0,
                    average_contribution=0.0,
                    contributions_by_date={},
                    contributions_by_state={},
                    top_donors=[],
                    contribution_distribution={},
                    data_completeness=0.0 if total_from_api and total_from_api > 0 else None,
                    total_from_api=total_from_api,
                    warning_message=warning_message,
                    using_financial_totals_fallback=using_financial_totals_fallback
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
            df['contribution_amount'] = await async_to_numeric(df['contribution_amount'], errors='coerce')
            df['contribution_amount'] = df['contribution_amount'].fillna(0.0)
            
            # Calculate totals (offload to thread pool)
            total_contributions = await async_dataframe_operation(df, lambda d: d['contribution_amount'].sum())
            total_contributors = await async_dataframe_operation(df, lambda d: d['contributor_name'].fillna('Unknown').nunique())
            average_contribution = total_contributions / len(df) if len(df) > 0 else 0.0
            
            # Contributions by date
            df['date'] = pd.to_datetime(df['contribution_date'], errors='coerce')
            df_dated = df[df['date'].notna()].copy()
            date_grouped = await async_dataframe_operation(
                df_dated,
                lambda d: d.groupby(d['date'].dt.date)['contribution_amount'].sum()
            )
            contributions_by_date = {str(k): float(v) for k, v in date_grouped.items() if k is not None}
            
            # Contributions by state
            df_stated = df[df['contributor_state'].notna()].copy()
            state_grouped = await async_dataframe_operation(
                df_stated,
                lambda d: d.groupby('contributor_state')['contribution_amount'].sum()
            )
            contributions_by_state = {k: float(v) for k, v in state_grouped.items()}
            
            # Top donors
            if len(df) > 0:
                top_donors_df = await async_dataframe_operation(
                    df,
                    lambda d: d.groupby('contributor_name').agg({
                        'contribution_amount': ['sum', 'count']
                    }).reset_index()
                )
                top_donors_df.columns = ['name', 'total', 'count']
                top_donors_df = await async_dataframe_operation(
                    top_donors_df,
                    lambda d: d.sort_values('total', ascending=False).head(20)
                )
                top_donors = top_donors_df.to_dict('records')
            else:
                top_donors = []
            
            # Contribution distribution (bins)
            amounts = df['contribution_amount'].fillna(0).tolist()
            contribution_distribution = calculate_distribution_bins(amounts) if amounts else {}
            
            # Get FEC API totals for comparison and data completeness calculation
            data_completeness = None
            total_from_api = None
            warning_message = None
            using_financial_totals_fallback = False
            
            if candidate_id:
                try:
                    api_totals = await self.fec_client.get_candidate_totals(candidate_id, cycle=cycle)
                    if api_totals and len(api_totals) > 0:
                        # Find matching cycle or use first result
                        matching_total = None
                        for total in api_totals:
                            total_cycle = total.get('cycle') or total.get('two_year_transaction_period') or total.get('election_year')
                            if cycle is None or total_cycle == cycle:
                                matching_total = total
                                break
                        
                        if not matching_total and api_totals:
                            # Use first result if no cycle match
                            matching_total = api_totals[0]
                        
                        if matching_total:
                            # Use individual_contributions for comparison (most accurate for Schedule A data)
                            total_from_api = float(matching_total.get('individual_contributions', 0) or matching_total.get('contributions', 0) or 0)
                            if total_from_api > 0:
                                data_completeness = min(100.0, (total_contributions / total_from_api) * 100.0)
                                logger.debug(f"Data completeness (fallback): {data_completeness:.1f}% (Local: ${total_contributions:,.2f}, API: ${total_from_api:,.2f})")
                                
                                # Warn if completeness is very low
                                if data_completeness < 10.0 and total_from_api > 1000:
                                    warning_message = (
                                        f"This analysis is based on {data_completeness:.1f}% of total contributions. "
                                        f"Financial totals show ${total_from_api:,.2f}, but only ${total_contributions:,.2f} "
                                        f"is available in the local database. Consider importing additional bulk data "
                                        f"or waiting for the FEC to publish detailed records."
                                    )
                except Exception as e:
                    logger.warning(f"Error getting FEC API totals for data completeness (fallback): {e}")
            
            return ContributionAnalysis(
                total_contributions=float(total_contributions),
                total_contributors=int(total_contributors),
                average_contribution=float(average_contribution),
                contributions_by_date=contributions_by_date,
                contributions_by_state=contributions_by_state,
                top_donors=top_donors,
                contribution_distribution=contribution_distribution,
                data_completeness=data_completeness,
                total_from_api=total_from_api,
                warning_message=warning_message,
                using_financial_totals_fallback=using_financial_totals_fallback
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
        # Check for pre-computed result first
        if config.ENABLE_PRECOMPUTED_ANALYSIS and not min_date and not max_date:
            try:
                from app.services.analysis.computation import AnalysisComputationService
                computation_service = AnalysisComputationService(self.fec_client)
                
                precomputed = await computation_service.get_precomputed_analysis(
                    analysis_type='employer',
                    candidate_id=candidate_id,
                    cycle=cycle,
                    committee_id=committee_id
                )
                
                if precomputed:
                    logger.debug("Using pre-computed employer analysis")
                    # Convert dict back to Pydantic model
                    return EmployerAnalysis(**precomputed['result_data'])
            except Exception as e:
                logger.debug(f"Could not retrieve pre-computed employer analysis: {e}")
                # Fall through to compute
        
        try:
            # Convert cycle to date range if provided
            if should_convert_cycle(cycle, min_date, max_date):
                min_date, max_date = convert_cycle_to_date_range(cycle)
            
            async with AsyncSessionLocal() as session:
                # Build base query conditions
                conditions = []
                if candidate_id:
                    from app.services.shared.query_builders import build_candidate_condition
                    candidate_condition = await build_candidate_condition(candidate_id, fec_client=self.fec_client)
                    conditions.append(candidate_condition)
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
                
                result = EmployerAnalysis(
                    total_by_employer=total_by_employer,
                    top_employers=top_employers,
                    employer_count=int(employer_grouped['normalized_employer'].nunique()),
                    total_contributions=total_contributions
                )
                
                # Store result for future use if pre-computation is enabled
                if config.ENABLE_PRECOMPUTED_ANALYSIS and not min_date and not max_date:
                    try:
                        from app.services.analysis.computation import AnalysisComputationService
                        computation_service = AnalysisComputationService(self.fec_client)
                        await computation_service._store_analysis(
                            analysis_type='employer',
                            candidate_id=candidate_id,
                            cycle=cycle,
                            committee_id=committee_id,
                            result_data=result.model_dump() if hasattr(result, 'model_dump') else result.dict()
                        )
                    except Exception as e:
                        logger.debug(f"Could not store employer analysis result: {e}")
                
                return result
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
            df['contribution_amount'] = await async_to_numeric(df['contribution_amount'], errors='coerce')
            df['contribution_amount'] = df['contribution_amount'].fillna(0.0)
            
            # Filter out null employers
            df_with_employer = df[df['contributor_employer'].notna()].copy()
            
            if len(df_with_employer) == 0:
                total_contrib = await async_dataframe_operation(df, lambda d: d['contribution_amount'].sum())
                return EmployerAnalysis(
                    total_by_employer={},
                    top_employers=[],
                    employer_count=0,
                    total_contributions=float(total_contrib)
                )
            
            # Normalize employer names for better aggregation (offload to thread pool)
            df_with_employer['normalized_employer'] = await async_dataframe_operation(
                df_with_employer,
                lambda d: d['contributor_employer'].apply(self._normalize_employer_name)
            )
            
            # Group by normalized employer name
            employer_grouped = await async_dataframe_operation(
                df_with_employer,
                lambda d: d.groupby('normalized_employer').agg({
                    'contribution_amount': ['sum', 'count'],
                    'contributor_employer': 'first'  # Keep original name for display
                }).reset_index()
            )
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
        # Check for pre-computed result first
        if config.ENABLE_PRECOMPUTED_ANALYSIS and not min_date and not max_date:
            try:
                from app.services.analysis.computation import AnalysisComputationService
                computation_service = AnalysisComputationService(self.fec_client)
                
                precomputed = await computation_service.get_precomputed_analysis(
                    analysis_type='velocity',
                    candidate_id=candidate_id,
                    cycle=cycle,
                    committee_id=committee_id
                )
                
                if precomputed:
                    logger.debug("Using pre-computed velocity analysis")
                    # Convert dict back to Pydantic model
                    return ContributionVelocity(**precomputed['result_data'])
            except Exception as e:
                logger.debug(f"Could not retrieve pre-computed velocity analysis: {e}")
                # Fall through to compute
        
        try:
            # Convert cycle to date range if provided
            if should_convert_cycle(cycle, min_date, max_date):
                min_date, max_date = convert_cycle_to_date_range(cycle)
            
            async with AsyncSessionLocal() as session:
                # Build query using ContributionQueryBuilder
                query_builder = ContributionQueryBuilder()
                query_builder.with_candidate(candidate_id).with_committee(committee_id).with_dates(min_date, max_date, cycle)
                where_clause = await query_builder.build_where_clause()
                
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
                
                result = ContributionVelocity(
                    velocity_by_date=velocity_by_date,
                    velocity_by_week=velocity_by_week,
                    peak_days=peak_days,
                    average_daily_velocity=average_daily_velocity
                )
                
                # Store result for future use if pre-computation is enabled
                if config.ENABLE_PRECOMPUTED_ANALYSIS and not min_date and not max_date:
                    try:
                        from app.services.analysis.computation import AnalysisComputationService
                        computation_service = AnalysisComputationService(self.fec_client)
                        await computation_service._store_analysis(
                            analysis_type='velocity',
                            candidate_id=candidate_id,
                            cycle=cycle,
                            committee_id=committee_id,
                            result_data=result.model_dump() if hasattr(result, 'model_dump') else result.dict()
                        )
                    except Exception as e:
                        logger.debug(f"Could not store velocity analysis result: {e}")
                
                return result
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
            df['contribution_amount'] = await async_to_numeric(df['contribution_amount'], errors='coerce')
            df['contribution_amount'] = df['contribution_amount'].fillna(0.0)
            
            df['date'] = pd.to_datetime(df['contribution_date'], errors='coerce')
            df = df[df['date'].notna()].copy()
            
            if len(df) == 0:
                return ContributionVelocity(
                    velocity_by_date={},
                    velocity_by_week={},
                    peak_days=[],
                    average_daily_velocity=0.0
                )
            
            # Daily velocity (amount per day) - offload to thread pool
            daily_amounts = await async_dataframe_operation(
                df,
                lambda d: d.groupby(d['date'].dt.date)['contribution_amount'].sum()
            )
            velocity_by_date = {str(k): float(v) for k, v in daily_amounts.items()}
            
            # Weekly velocity
            df['week'] = df['date'].dt.to_period('W').astype(str)
            weekly_amounts = await async_dataframe_operation(
                df,
                lambda d: d.groupby('week')['contribution_amount'].sum()
            )
            velocity_by_week = {k: float(v) for k, v in weekly_amounts.items()}
            
            # Peak days (top 10 by amount) - offload to thread pool
            peak_days_df = await async_dataframe_operation(
                daily_amounts.to_frame().reset_index(),
                lambda d: d.sort_values(0, ascending=False).head(10)
            )
            daily_counts = await async_dataframe_operation(
                df,
                lambda d: d.groupby(d['date'].dt.date).size()
            )
            peak_days = [
                {
                    'date': str(row['date']),
                    'amount': float(row[0]),
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
                where_clause = await query_builder.build_where_clause()
                
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
                    # SQLite's func.date() returns a string, not a datetime object
                    # Handle both string and date objects for compatibility
                    if row.date:
                        if isinstance(row.date, str):
                            date_str = row.date  # Already a string in YYYY-MM-DD format
                        else:
                            date_str = row.date.strftime("%Y-%m-%d")
                    else:
                        date_str = None
                    
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

