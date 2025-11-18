"""Analysis service facade - maintains backward compatibility"""
from typing import Optional, List, Dict, Any

from app.services.fec_client import FECClient
from app.models.schemas import (
    ContributionAnalysis, MoneyFlowNode, MoneyFlowEdge, MoneyFlowGraph,
    ExpenditureBreakdown, EmployerAnalysis, ContributionVelocity, DonorStateAnalysis, CumulativeTotals
)

from .contribution_analysis import ContributionAnalysisService
from .donor_analysis import DonorAnalysisService
from .expenditure_analysis import ExpenditureAnalysisService
from .money_flow import MoneyFlowService


class AnalysisService:
    """Service for financial analysis - facade that delegates to specialized services"""
    
    def __init__(self, fec_client: FECClient):
        self.fec_client = fec_client
        self._contribution_service = ContributionAnalysisService(fec_client)
        self._donor_service = DonorAnalysisService(fec_client)
        self._expenditure_service = ExpenditureAnalysisService(fec_client)
        self._money_flow_service = MoneyFlowService(fec_client)
    
    async def analyze_contributions(
        self,
        candidate_id: Optional[str] = None,
        committee_id: Optional[str] = None,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None,
        cycle: Optional[int] = None
    ) -> ContributionAnalysis:
        """Analyze contributions with aggregations using efficient SQL queries"""
        return await self._contribution_service.analyze_contributions(
            candidate_id=candidate_id,
            committee_id=committee_id,
            min_date=min_date,
            max_date=max_date,
            cycle=cycle
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
        return await self._contribution_service.analyze_by_employer(
            candidate_id=candidate_id,
            committee_id=committee_id,
            min_date=min_date,
            max_date=max_date,
            cycle=cycle
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
        return await self._contribution_service.analyze_velocity(
            candidate_id=candidate_id,
            committee_id=committee_id,
            min_date=min_date,
            max_date=max_date,
            cycle=cycle
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
        return await self._contribution_service.get_cumulative_totals(
            candidate_id=candidate_id,
            committee_id=committee_id,
            min_date=min_date,
            max_date=max_date,
            cycle=cycle
        )
    
    async def analyze_donor_states(
        self,
        candidate_id: str,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None,
        cycle: Optional[int] = None
    ) -> DonorStateAnalysis:
        """Analyze individual donors by state to identify out-of-state funding"""
        return await self._donor_service.analyze_donor_states(
            candidate_id=candidate_id,
            min_date=min_date,
            max_date=max_date,
            cycle=cycle
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
        return await self._donor_service.get_out_of_state_contributions(
            candidate_id=candidate_id,
            min_date=min_date,
            max_date=max_date,
            cycle=cycle,
            limit=limit
        )
    
    async def get_aggregated_out_of_state_donors(
        self,
        candidate_id: str,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None,
        cycle: Optional[int] = None,
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """Get aggregated out-of-state donors for human analysis"""
        return await self._donor_service.get_aggregated_out_of_state_donors(
            candidate_id=candidate_id,
            min_date=min_date,
            max_date=max_date,
            cycle=cycle,
            limit=limit
        )
    
    async def analyze_expenditures(
        self,
        candidate_id: Optional[str] = None,
        committee_id: Optional[str] = None,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None
    ) -> ExpenditureBreakdown:
        """Analyze expenditures with category aggregation"""
        return await self._expenditure_service.analyze_expenditures(
            candidate_id=candidate_id,
            committee_id=committee_id,
            min_date=min_date,
            max_date=max_date
        )
    
    async def build_money_flow_graph(
        self,
        candidate_id: str,
        max_depth: int = 2,
        min_amount: float = 100.0,
        aggregate_by_employer: bool = True
    ) -> MoneyFlowGraph:
        """Build network graph of money flows"""
        return await self._money_flow_service.build_money_flow_graph(
            candidate_id=candidate_id,
            max_depth=max_depth,
            min_amount=min_amount,
            aggregate_by_employer=aggregate_by_employer
        )

