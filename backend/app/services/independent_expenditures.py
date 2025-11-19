import pandas as pd
from typing import Optional, Dict, List, Any
from datetime import datetime
from app.services.fec_client import FECClient
from app.db.database import AsyncSessionLocal, IndependentExpenditure
from sqlalchemy import select, and_, or_, func
import logging

logger = logging.getLogger(__name__)


class IndependentExpenditureService:
    """Service for independent expenditure analysis"""
    
    def __init__(self, fec_client: FECClient):
        self.fec_client = fec_client
    
    async def get_independent_expenditures(
        self,
        candidate_id: Optional[str] = None,
        committee_id: Optional[str] = None,
        support_oppose: Optional[str] = None,  # 'S' or 'O'
        min_date: Optional[str] = None,
        max_date: Optional[str] = None,
        min_amount: Optional[float] = None,
        max_amount: Optional[float] = None,
        limit: int = 1000
    ) -> List[Dict]:
        """Get independent expenditures from local DB or API"""
        # Try local database first
        try:
            local_data = await self._query_local_expenditures(
                candidate_id=candidate_id,
                committee_id=committee_id,
                support_oppose=support_oppose,
                min_date=min_date,
                max_date=max_date,
                min_amount=min_amount,
                max_amount=max_amount,
                limit=limit
            )
            if local_data and len(local_data) > 0:
                logger.debug(f"Found {len(local_data)} independent expenditures in local database")
                return local_data
        except Exception as e:
            logger.warning(f"Error querying local independent expenditures, falling back to API: {e}")
        
        # Fall back to API
        logger.debug(f"Querying FEC API for independent expenditures: candidate_id={candidate_id}, committee_id={committee_id}")
        try:
            return await self.fec_client.get_independent_expenditures(
                candidate_id=candidate_id,
                committee_id=committee_id,
                support_oppose=support_oppose,
                min_date=min_date,
                max_date=max_date,
                min_amount=min_amount,
                max_amount=max_amount,
                limit=limit
            )
        except Exception as e:
            logger.error(f"API fallback failed for independent expenditures: {e}")
            return []
    
    async def _query_local_expenditures(
        self,
        candidate_id: Optional[str] = None,
        committee_id: Optional[str] = None,
        support_oppose: Optional[str] = None,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None,
        min_amount: Optional[float] = None,
        max_amount: Optional[float] = None,
        limit: int = 1000
    ) -> Optional[List[Dict]]:
        """Query independent expenditures from local database"""
        try:
            async with AsyncSessionLocal() as session:
                query = select(IndependentExpenditure)
                conditions = []
                
                if candidate_id:
                    conditions.append(IndependentExpenditure.candidate_id == candidate_id)
                if committee_id:
                    conditions.append(IndependentExpenditure.committee_id == committee_id)
                if support_oppose:
                    conditions.append(IndependentExpenditure.support_oppose_indicator == support_oppose)
                if min_amount is not None:
                    conditions.append(IndependentExpenditure.expenditure_amount >= min_amount)
                if max_amount is not None:
                    conditions.append(IndependentExpenditure.expenditure_amount <= max_amount)
                if min_date:
                    try:
                        min_date_obj = datetime.strptime(min_date, "%Y-%m-%d")
                        conditions.append(IndependentExpenditure.expenditure_date >= min_date_obj)
                    except ValueError as e:
                        logger.debug(f"Invalid min_date format '{min_date}': {e}")
                    if max_date:
                        try:
                            max_date_obj = datetime.strptime(max_date, "%Y-%m-%d")
                            conditions.append(IndependentExpenditure.expenditure_date <= max_date_obj)
                        except ValueError as e:
                            logger.debug(f"Invalid max_date format '{max_date}': {e}")
                
                if conditions:
                    query = query.where(and_(*conditions))
                
                query = query.order_by(IndependentExpenditure.expenditure_date.desc().nulls_last())
                query = query.limit(limit)
                
                result = await session.execute(query)
                expenditures = result.scalars().all()
                
                if expenditures:
                    result_list = []
                    for exp in expenditures:
                        # Safely convert expenditure_amount to float, handling malformed values
                        amount = 0.0
                        if exp.expenditure_amount:
                            try:
                                if isinstance(exp.expenditure_amount, str):
                                    # Clean the string and handle malformed values
                                    amount_str = exp.expenditure_amount.replace('$', '').replace(',', '').strip()
                                    # Handle multiple decimal points
                                    if '.' in amount_str and amount_str.count('.') > 1:
                                        first_dot = amount_str.find('.')
                                        second_dot = amount_str.find('.', first_dot + 1)
                                        if second_dot > 0:
                                            amount_str = amount_str[:second_dot]
                                    amount = float(amount_str)
                                else:
                                    amount = float(exp.expenditure_amount)
                            except (ValueError, TypeError) as e:
                                logger.warning(f"Invalid expenditure_amount in database: {exp.expenditure_amount}, using 0.0")
                                amount = 0.0
                        
                        exp_dict = {
                            "expenditure_id": exp.expenditure_id,
                            "cycle": exp.cycle,
                            "committee_id": exp.committee_id,
                            "candidate_id": exp.candidate_id,
                            "candidate_name": exp.candidate_name,
                            "support_oppose_indicator": exp.support_oppose_indicator,
                            "expenditure_amount": amount,
                            "expenditure_date": exp.expenditure_date.strftime("%Y-%m-%d") if exp.expenditure_date else None,
                            "payee_name": exp.payee_name,
                            "expenditure_purpose": exp.expenditure_purpose
                        }
                        if exp.raw_data:
                            exp_dict.update(exp.raw_data)
                        result_list.append(exp_dict)
                    return result_list
                return None
        except Exception as e:
            logger.warning(f"Error querying local independent expenditures: {e}")
            return None
    
    async def analyze_independent_expenditures(
        self,
        candidate_id: Optional[str] = None,
        committee_id: Optional[str] = None,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None
    ) -> Dict[str, Any]:
        """Analyze independent expenditures with aggregations"""
        expenditures = await self.get_independent_expenditures(
            candidate_id=candidate_id,
            committee_id=committee_id,
            min_date=min_date,
            max_date=max_date,
            limit=10000
        )
        
        if not expenditures:
            return {
                "total_expenditures": 0.0,
                "total_support": 0.0,
                "total_oppose": 0.0,
                "total_transactions": 0,
                "expenditures_by_date": {},
                "expenditures_by_committee": {},
                "expenditures_by_candidate": {},
                "top_committees": [],
                "top_candidates": []
            }
        
        df = pd.DataFrame(expenditures)
        
        # Ensure expenditure_amount is numeric, handling malformed values
        from app.utils.thread_pool import async_to_numeric, async_dataframe_operation
        if 'expenditure_amount' in df.columns:
            # Convert to numeric, coercing errors to NaN, then fill NaN with 0
            df['expenditure_amount'] = await async_to_numeric(df['expenditure_amount'], errors='coerce')
            df['expenditure_amount'] = df['expenditure_amount'].fillna(0.0)
        else:
            df['expenditure_amount'] = 0.0
        
        # Calculate totals (offload to thread pool)
        total_expenditures = await async_dataframe_operation(df, lambda d: d['expenditure_amount'].sum()) if 'expenditure_amount' in df.columns else 0.0
        total_transactions = len(df)
        
        # Support vs Oppose
        support_oppose_col = df.get('support_oppose_indicator', pd.Series())
        if 'support_oppose_indicator' in df.columns:
            df_support = df[df['support_oppose_indicator'] == 'S']
            df_oppose = df[df['support_oppose_indicator'] == 'O']
            total_support = await async_dataframe_operation(df_support, lambda d: d['expenditure_amount'].sum())
            total_oppose = await async_dataframe_operation(df_oppose, lambda d: d['expenditure_amount'].sum())
        else:
            total_support = 0.0
            total_oppose = 0.0
        
        # By date
        expenditures_by_date = {}
        if 'expenditure_date' in df.columns:
            date_grouped = await async_dataframe_operation(
                df,
                lambda d: d.groupby('expenditure_date')['expenditure_amount'].sum()
            )
            expenditures_by_date = {str(date): float(amount) for date, amount in date_grouped.items() if pd.notna(amount)}
        
        # By committee
        expenditures_by_committee = {}
        top_committees = []
        if 'committee_id' in df.columns:
            # Use a column that exists for counting, or count rows directly
            count_col = None
            if 'expenditure_id' in df.columns:
                count_col = 'expenditure_id'
            elif 'sub_id' in df.columns:
                count_col = 'sub_id'
            
            if count_col:
                committee_grouped = await async_dataframe_operation(
                    df,
                    lambda d: d.groupby('committee_id').agg({
                        'expenditure_amount': 'sum',
                        count_col: 'count'
                    }).reset_index()
                )
                committee_grouped.columns = ['committee_id', 'total_amount', 'count']
            else:
                # If no ID column exists, count rows directly using size()
                committee_grouped = await async_dataframe_operation(
                    df,
                    lambda d: d.groupby('committee_id').agg({
                        'expenditure_amount': 'sum'
                    }).reset_index()
                )
                count_df = await async_dataframe_operation(
                    df,
                    lambda d: d.groupby('committee_id').size().reset_index(name='count')
                )
                committee_grouped = await async_dataframe_operation(
                    committee_grouped,
                    lambda d: d.merge(count_df, on='committee_id')
                )
            
            expenditures_by_committee = {
                row['committee_id']: float(row['total_amount']) 
                for _, row in committee_grouped.iterrows() 
                if pd.notna(row['total_amount'])
            }
            top_committees = committee_grouped.nlargest(10, 'total_amount').to_dict('records')
        
        # By candidate
        expenditures_by_candidate = {}
        top_candidates = []
        if 'candidate_id' in df.columns:
            # Use a column that exists for counting, or count rows directly
            count_col = None
            if 'expenditure_id' in df.columns:
                count_col = 'expenditure_id'
            elif 'sub_id' in df.columns:
                count_col = 'sub_id'
            
            if count_col:
                candidate_grouped = await async_dataframe_operation(
                    df,
                    lambda d: d.groupby('candidate_id').agg({
                        'expenditure_amount': 'sum',
                        count_col: 'count'
                    }).reset_index()
                )
                candidate_grouped.columns = ['candidate_id', 'total_amount', 'count']
            else:
                # If no ID column exists, count rows directly using size()
                candidate_grouped = await async_dataframe_operation(
                    df,
                    lambda d: d.groupby('candidate_id').agg({
                        'expenditure_amount': 'sum'
                    }).reset_index()
                )
                count_df = await async_dataframe_operation(
                    df,
                    lambda d: d.groupby('candidate_id').size().reset_index(name='count')
                )
                candidate_grouped = await async_dataframe_operation(
                    candidate_grouped,
                    lambda d: d.merge(count_df, on='candidate_id')
                )
            
            expenditures_by_candidate = {
                row['candidate_id']: float(row['total_amount']) 
                for _, row in candidate_grouped.iterrows() 
                if pd.notna(row['total_amount'])
            }
            top_candidates = candidate_grouped.nlargest(10, 'total_amount').to_dict('records')
        
        return {
            "total_expenditures": float(total_expenditures),
            "total_support": float(total_support),
            "total_oppose": float(total_oppose),
            "total_transactions": total_transactions,
            "expenditures_by_date": expenditures_by_date,
            "expenditures_by_committee": expenditures_by_committee,
            "expenditures_by_candidate": expenditures_by_candidate,
            "top_committees": top_committees,
            "top_candidates": top_candidates
        }
    
    async def get_candidate_summary(
        self,
        candidate_id: str,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get independent expenditure summary for a specific candidate"""
        analysis = await self.analyze_independent_expenditures(
            candidate_id=candidate_id,
            min_date=min_date,
            max_date=max_date
        )
        
        expenditures = await self.get_independent_expenditures(
            candidate_id=candidate_id,
            min_date=min_date,
            max_date=max_date,
            limit=100
        )
        
        return {
            "candidate_id": candidate_id,
            "analysis": analysis,
            "recent_expenditures": expenditures[:10]  # Most recent 10
        }

