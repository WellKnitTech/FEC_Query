from typing import Optional, Dict, List, Any
from app.services.fec_client import FECClient
from app.db.database import AsyncSessionLocal, FinancialTotal
from sqlalchemy import select, and_, func
import logging

logger = logging.getLogger(__name__)


class TrendAnalysisService:
    """Service for historical trend analysis"""
    
    def __init__(self, fec_client: FECClient):
        self.fec_client = fec_client
    
    async def get_candidate_trends(
        self,
        candidate_id: str,
        min_cycle: Optional[int] = None,
        max_cycle: Optional[int] = None
    ) -> Dict[str, Any]:
        """Get multi-cycle financial trends for a candidate"""
        # Query from local database
        async with AsyncSessionLocal() as session:
            query = select(FinancialTotal).where(FinancialTotal.candidate_id == candidate_id)
            
            if min_cycle:
                query = query.where(FinancialTotal.cycle >= min_cycle)
            if max_cycle:
                query = query.where(FinancialTotal.cycle <= max_cycle)
            
            query = query.order_by(FinancialTotal.cycle.asc())
            result = await session.execute(query)
            financials = result.scalars().all()
        
        if not financials:
            # Fallback to API if no local data
            totals = await self.fec_client.get_candidate_totals(candidate_id)
            financials = []
            for total in totals:
                cycle_value = total.get("cycle") or total.get("two_year_transaction_period") or 0
                if (min_cycle and cycle_value < min_cycle) or (max_cycle and cycle_value > max_cycle):
                    continue
                # Try multiple field names for cash on hand
                cash_on_hand_value = (
                    total.get("cash_on_hand_end_period") or
                    total.get("cash_on_hand") or
                    total.get("coh_cop") or
                    total.get("cash_on_hand_end") or
                    0
                )
                financials.append({
                    "cycle": cycle_value,
                    "total_receipts": float(total.get("receipts", 0)),
                    "total_disbursements": float(total.get("disbursements", 0)),
                    "cash_on_hand": float(cash_on_hand_value),
                    "total_contributions": float(total.get("contributions", 0)),
                    "individual_contributions": float(total.get("individual_contributions", 0)),
                    "pac_contributions": float(total.get("pac_contributions", 0)),
                })
        else:
            financials = [
                {
                    "cycle": f.cycle,
                    "total_receipts": f.total_receipts,
                    "total_disbursements": f.total_disbursements,
                    "cash_on_hand": f.cash_on_hand,
                    "total_contributions": f.total_contributions,
                    "individual_contributions": f.individual_contributions,
                    "pac_contributions": f.pac_contributions,
                }
                for f in financials
            ]
        
        # Calculate growth rates
        trends = []
        for i, fin in enumerate(financials):
            trend_entry = {
                "cycle": fin["cycle"],
                "total_receipts": fin["total_receipts"],
                "total_disbursements": fin["total_disbursements"],
                "cash_on_hand": fin["cash_on_hand"],
                "total_contributions": fin["total_contributions"],
                "individual_contributions": fin["individual_contributions"],
                "pac_contributions": fin["pac_contributions"],
            }
            
            # Calculate growth from previous cycle
            if i > 0:
                prev = financials[i - 1]
                if prev["total_receipts"] > 0:
                    trend_entry["receipts_growth"] = ((fin["total_receipts"] - prev["total_receipts"]) / prev["total_receipts"]) * 100
                else:
                    trend_entry["receipts_growth"] = 0.0
            else:
                trend_entry["receipts_growth"] = 0.0
            
            trends.append(trend_entry)
        
        return {
            "candidate_id": candidate_id,
            "trends": trends,
            "total_cycles": len(trends)
        }
    
    async def get_race_trends(
        self,
        candidate_ids: List[str],
        min_cycle: Optional[int] = None,
        max_cycle: Optional[int] = None
    ) -> Dict[str, Any]:
        """Compare multiple candidates across cycles"""
        candidate_trends = {}
        
        for candidate_id in candidate_ids:
            trends = await self.get_candidate_trends(candidate_id, min_cycle, max_cycle)
            candidate_trends[candidate_id] = trends
        
        return {
            "candidate_ids": candidate_ids,
            "candidate_trends": candidate_trends
        }
    
    async def get_contribution_trends(
        self,
        candidate_id: str,
        min_cycle: Optional[int] = None,
        max_cycle: Optional[int] = None
    ) -> Dict[str, Any]:
        """Analyze contribution patterns across cycles"""
        # Get financial trends
        trends_data = await self.get_candidate_trends(candidate_id, min_cycle, max_cycle)
        
        # Extract contribution data
        contribution_trends = []
        for trend in trends_data["trends"]:
            contribution_trends.append({
                "cycle": trend["cycle"],
                "total_contributions": trend["total_contributions"],
                "individual_contributions": trend["individual_contributions"],
                "pac_contributions": trend["pac_contributions"],
            })
        
        return {
            "candidate_id": candidate_id,
            "contribution_trends": contribution_trends
        }

