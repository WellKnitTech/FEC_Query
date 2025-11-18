"""Donor analysis service"""
import pandas as pd
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime
from sqlalchemy import select, and_, func

from app.db.database import AsyncSessionLocal, Contribution
from app.services.fec_client import FECClient
from app.models.schemas import DonorStateAnalysis
from app.services.shared.cycle_utils import convert_cycle_to_date_range, should_convert_cycle
from app.utils.date_utils import serialize_date, extract_date_from_raw_data

logger = logging.getLogger(__name__)


class DonorAnalysisService:
    """Service for donor analysis"""
    
    def __init__(self, fec_client: FECClient):
        self.fec_client = fec_client
    
    async def analyze_donor_states(
        self,
        candidate_id: str,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None,
        cycle: Optional[int] = None
    ) -> DonorStateAnalysis:
        """Analyze individual donors by state to identify out-of-state funding"""
        # Get candidate info to get their state
        candidate = await self.fec_client.get_candidate(candidate_id)
        candidate_state = candidate.get('state') if candidate else None
        
        # Convert cycle to date range if provided and no explicit dates given
        if should_convert_cycle(cycle, min_date, max_date):
            min_date, max_date = convert_cycle_to_date_range(cycle)
            logger.debug(f"analyze_donor_states: Converted cycle {cycle} to date range: {min_date} to {max_date}")
        
        # Get contributions
        contributions = await self.fec_client.get_contributions(
            candidate_id=candidate_id,
            min_date=min_date,
            max_date=max_date,
            limit=10000,
            two_year_transaction_period=cycle if not min_date and not max_date else None
        )
        
        if not contributions:
            return DonorStateAnalysis(
                donors_by_state={},
                donor_percentages_by_state={},
                amounts_by_state={},
                amount_percentages_by_state={},
                candidate_state=candidate_state,
                in_state_donor_percentage=0.0,
                in_state_amount_percentage=0.0,
                out_of_state_donor_percentage=0.0,
                out_of_state_amount_percentage=0.0,
                total_unique_donors=0,
                total_contributions=0.0,
                is_highly_out_of_state=False
            )
        
        df = pd.DataFrame(contributions)
        
        # Ensure required columns exist
        if 'contribution_amount' not in df.columns:
            df['contribution_amount'] = 0.0
        if 'contributor_name' not in df.columns:
            df['contributor_name'] = None
        if 'contributor_state' not in df.columns:
            df['contributor_state'] = None
        
        # Convert contribution_amount to float, and extract from raw_data if needed
        def extract_amount(row):
            amount = row.get('contribution_amount')
            if amount is not None:
                try:
                    amount_float = float(amount)
                    if amount_float > 0:
                        return amount_float
                except (ValueError, TypeError):
                    pass
            
            raw_data = row.get('raw_data')
            if raw_data and isinstance(raw_data, dict):
                for amt_key in ['TRANSACTION_AMT', 'CONTB_AMT', 'contb_receipt_amt', 'contribution_amount', 'transaction_amt', 'contribution_receipt_amount']:
                    if amt_key in raw_data:
                        try:
                            amt_val = str(raw_data[amt_key]).strip()
                            amt_val = amt_val.replace('$', '').replace(',', '').strip()
                            if amt_val:
                                amount_float = float(amt_val)
                                if amount_float > 0:
                                    return amount_float
                        except (ValueError, TypeError):
                            continue
            
            for amt_key in ['contb_receipt_amt', 'contribution_receipt_amount', 'amount']:
                if amt_key in row:
                    try:
                        amount_float = float(row[amt_key])
                        if amount_float > 0:
                            return amount_float
                    except (ValueError, TypeError):
                        continue
            
            return 0.0
        
        df['contribution_amount'] = df.apply(extract_amount, axis=1)
        
        logger.debug(f"analyze_donor_states: Total contribution amount: ${df['contribution_amount'].sum():,.2f}")
        logger.debug(f"analyze_donor_states: Number of contributions: {len(df)}")
        
        df = df[df['contributor_name'].notna()].copy()
        
        if len(df) == 0:
            return DonorStateAnalysis(
                donors_by_state={},
                donor_percentages_by_state={},
                amounts_by_state={},
                amount_percentages_by_state={},
                candidate_state=candidate_state,
                in_state_donor_percentage=0.0,
                in_state_amount_percentage=0.0,
                out_of_state_donor_percentage=0.0,
                out_of_state_amount_percentage=0.0,
                total_unique_donors=0,
                total_contributions=0.0,
                is_highly_out_of_state=False
            )
        
        df['contributor_state'] = df['contributor_state'].fillna('Unknown')
        
        df['donor_key'] = df.apply(
            lambda row: f"{row['contributor_name']}|{row['contributor_state']}" 
            if pd.notna(row['contributor_state']) and row['contributor_state'] != 'Unknown'
            else row['contributor_name'],
            axis=1
        )
        
        donor_states = {}
        donor_amounts = {}
        
        for donor_key in df['donor_key'].unique():
            donor_df = df[df['donor_key'] == donor_key]
            state_counts = donor_df.groupby('contributor_state')['contribution_amount'].sum()
            primary_state = state_counts.idxmax() if len(state_counts) > 0 else 'Unknown'
            total_amount = donor_df['contribution_amount'].sum()
            
            donor_states[donor_key] = primary_state
            donor_amounts[donor_key] = total_amount
        
        state_donor_counts = {}
        state_amounts = {}
        
        for donor_key, state in donor_states.items():
            if state not in state_donor_counts:
                state_donor_counts[state] = 0
                state_amounts[state] = 0.0
            state_donor_counts[state] += 1
            state_amounts[state] += donor_amounts[donor_key]
        
        total_unique_donors = len(donor_states)
        total_contributions = df['contribution_amount'].sum()
        
        donor_percentages = {}
        amount_percentages = {}
        
        for state, count in state_donor_counts.items():
            donor_percentages[state] = (count / total_unique_donors * 100) if total_unique_donors > 0 else 0.0
        
        for state, amount in state_amounts.items():
            amount_percentages[state] = (amount / total_contributions * 100) if total_contributions > 0 else 0.0
        
        in_state_donor_count = state_donor_counts.get(candidate_state, 0) if candidate_state else 0
        in_state_amount = state_amounts.get(candidate_state, 0.0) if candidate_state else 0.0
        
        in_state_donor_percentage = (in_state_donor_count / total_unique_donors * 100) if total_unique_donors > 0 else 0.0
        in_state_amount_percentage = (in_state_amount / total_contributions * 100) if total_contributions > 0 else 0.0
        
        out_of_state_donor_percentage = 100.0 - in_state_donor_percentage if candidate_state else 0.0
        out_of_state_amount_percentage = 100.0 - in_state_amount_percentage if candidate_state else 0.0
        
        is_highly_out_of_state = False
        if candidate_state:
            is_highly_out_of_state = (
                out_of_state_donor_percentage > 50.0 or 
                out_of_state_amount_percentage > 50.0
            )
        
        return DonorStateAnalysis(
            donors_by_state={k: int(v) for k, v in state_donor_counts.items()},
            donor_percentages_by_state={k: float(v) for k, v in donor_percentages.items()},
            amounts_by_state={k: float(v) for k, v in state_amounts.items()},
            amount_percentages_by_state={k: float(v) for k, v in amount_percentages.items()},
            candidate_state=candidate_state,
            in_state_donor_percentage=float(in_state_donor_percentage),
            in_state_amount_percentage=float(in_state_amount_percentage),
            out_of_state_donor_percentage=float(out_of_state_donor_percentage),
            out_of_state_amount_percentage=float(out_of_state_amount_percentage),
            total_unique_donors=int(total_unique_donors),
            total_contributions=float(total_contributions),
            is_highly_out_of_state=is_highly_out_of_state
        )
    
    async def get_out_of_state_contributions(
        self,
        candidate_id: str,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None,
        cycle: Optional[int] = None,
        limit: int = 10000
    ) -> List[Dict[str, Any]]:
        """Get contributions from out-of-state donors for human analysis"""
        candidate = await self.fec_client.get_candidate(candidate_id)
        candidate_state = candidate.get('state') if candidate else None
        
        if not candidate_state:
            return []
        
        try:
            async with AsyncSessionLocal() as session:
                query = select(Contribution).where(
                    Contribution.candidate_id == candidate_id
                )
                
                query = query.where(
                    and_(
                        Contribution.contributor_state.isnot(None),
                        Contribution.contributor_state != '',
                        Contribution.contributor_state != 'Unknown',
                        func.upper(Contribution.contributor_state) != candidate_state.upper()
                    )
                )
                
                if min_date:
                    try:
                        min_date_obj = datetime.strptime(min_date, "%Y-%m-%d")
                        query = query.where(Contribution.contribution_date >= min_date_obj)
                    except ValueError:
                        pass
                
                if max_date:
                    try:
                        max_date_obj = datetime.strptime(max_date, "%Y-%m-%d")
                        query = query.where(Contribution.contribution_date <= max_date_obj)
                    except ValueError:
                        pass
                
                query = query.order_by(
                    Contribution.contribution_amount.desc().nulls_last(),
                    Contribution.contribution_date.desc().nulls_last()
                ).limit(limit)
                
                result = await session.execute(query)
                contributions = result.scalars().all()
                
                out_of_state_contributions = []
                for c in contributions:
                    amount = float(c.contribution_amount) if c.contribution_amount else 0.0
                    if amount == 0.0 and c.raw_data and isinstance(c.raw_data, dict):
                        for amt_key in ['TRANSACTION_AMT', 'CONTB_AMT', 'contribution_amount', 'transaction_amt', 'contb_receipt_amt']:
                            if amt_key in c.raw_data:
                                try:
                                    amt_val = str(c.raw_data[amt_key]).strip()
                                    amt_val = amt_val.replace('$', '').replace(',', '').strip()
                                    if amt_val:
                                        amount = float(amt_val)
                                        break
                                except (ValueError, TypeError):
                                    continue
                    
                    contrib_date = None
                    if c.contribution_date:
                        contrib_date = c.contribution_date
                    elif c.raw_data:
                        contrib_date = extract_date_from_raw_data(c.raw_data)
                    
                    date_str_formatted = serialize_date(contrib_date)
                    
                    contrib_dict = {
                        "sub_id": c.contribution_id,
                        "contribution_id": c.contribution_id,
                        "candidate_id": c.candidate_id,
                        "committee_id": c.committee_id,
                        "contributor_name": c.contributor_name,
                        "contributor_city": c.contributor_city,
                        "contributor_state": c.contributor_state,
                        "contributor_zip": c.contributor_zip,
                        "contributor_employer": c.contributor_employer,
                        "contributor_occupation": c.contributor_occupation,
                        "contribution_amount": amount,
                        "contribution_receipt_date": date_str_formatted,
                        "contribution_date": date_str_formatted,
                        "contribution_type": c.contribution_type,
                        "receipt_type": None
                    }
                    
                    if c.raw_data and isinstance(c.raw_data, dict):
                        for key, value in c.raw_data.items():
                            if key not in contrib_dict or not contrib_dict[key]:
                                contrib_dict[key] = value
                    
                    out_of_state_contributions.append(contrib_dict)
                
                return out_of_state_contributions
                
        except Exception as e:
            logger.warning(f"Error querying out-of-state contributions from database: {e}")
            contributions = await self.fec_client.get_contributions(
                candidate_id=candidate_id,
                min_date=min_date,
                max_date=max_date,
                limit=limit,
                two_year_transaction_period=cycle
            )
            
            if not contributions:
                return []
            
            out_of_state_contributions = []
            for contrib in contributions:
                contributor_state = contrib.get('contributor_state') or contrib.get('state')
                
                if not contributor_state or contributor_state == 'Unknown':
                    continue
                
                if contributor_state.upper() != candidate_state.upper():
                    date_value = contrib.get('contribution_receipt_date') or contrib.get('contribution_date') or contrib.get('receipt_date')
                    formatted_date = serialize_date(date_value)
                    
                    contrib['contribution_date'] = formatted_date
                    contrib['contribution_receipt_date'] = formatted_date
                    
                    out_of_state_contributions.append(contrib)
            
            out_of_state_contributions.sort(
                key=lambda x: (
                    -float(x.get('contribution_amount') or x.get('contb_receipt_amt') or 0),
                    x.get('contribution_date') or x.get('contribution_receipt_date') or ''
                ),
                reverse=True
            )
            
            return out_of_state_contributions
    
    async def get_aggregated_out_of_state_donors(
        self,
        candidate_id: str,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None,
        cycle: Optional[int] = None,
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """Get aggregated out-of-state donors for human analysis"""
        contributions = await self.get_out_of_state_contributions(
            candidate_id=candidate_id,
            min_date=min_date,
            max_date=max_date,
            cycle=cycle,
            limit=50000
        )
        
        if not contributions:
            return []
        
        from app.services.donor_aggregation import DonorAggregationService
        
        contrib_dicts = []
        for contrib in contributions:
            contrib_dict = {
                'contribution_id': contrib.get('contribution_id') or contrib.get('sub_id'),
                'contributor_name': contrib.get('contributor_name'),
                'contributor_city': contrib.get('contributor_city'),
                'contributor_state': contrib.get('contributor_state'),
                'contributor_zip': contrib.get('contributor_zip'),
                'contributor_employer': contrib.get('contributor_employer'),
                'contributor_occupation': contrib.get('contributor_occupation'),
                'contribution_amount': contrib.get('contribution_amount', 0) or 0,
                'contribution_date': contrib.get('contribution_date') or contrib.get('contribution_receipt_date')
            }
            if contrib_dict.get('contributor_name'):
                contrib_dicts.append(contrib_dict)
        
        if not contrib_dicts:
            return []
        
        aggregation_service = DonorAggregationService()
        aggregated = aggregation_service.aggregate_donors(contrib_dicts)
        
        aggregated.sort(key=lambda x: x.get('total_amount', 0), reverse=True)
        
        return aggregated[:limit]

