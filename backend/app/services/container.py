"""
Service container for dependency injection
"""
import asyncio
import logging
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from app.services.fec_client import FECClient
from app.services.bulk_data import BulkDataService
from app.services.analysis import AnalysisService
from app.services.fraud_detection import FraudDetectionService
from app.services.committees import CommitteeService
from app.services.independent_expenditures import IndependentExpenditureService
from app.services.trends import TrendAnalysisService
from app.services.donor_aggregation import DonorAggregationService
from app.services.contribution_limits import ContributionLimitsService
from app.services.saved_searches import SavedSearchService

logger = logging.getLogger(__name__)


class ServiceContainer:
    """Centralized service container for dependency injection"""
    
    def __init__(self):
        self._fec_client: Optional[FECClient] = None
        self._bulk_data_service: Optional[BulkDataService] = None
        self._initialized = False
    
    async def initialize(self):
        """Initialize all services"""
        if self._initialized:
            return
        
        # Services are initialized lazily, so this is mainly for future use
        self._initialized = True
        logger.debug("Service container initialized")
    
    def get_fec_client(self, api_key: Optional[str] = None) -> FECClient:
        """Get or create FEC client instance"""
        if self._fec_client is None:
            self._fec_client = FECClient(api_key=api_key)
        return self._fec_client
    
    def get_bulk_data_service(self) -> BulkDataService:
        """Get or create bulk data service instance"""
        if self._bulk_data_service is None:
            self._bulk_data_service = BulkDataService()
        return self._bulk_data_service
    
    def get_analysis_service(self, fec_client: Optional[FECClient] = None) -> AnalysisService:
        """Get analysis service instance"""
        if fec_client is None:
            fec_client = self.get_fec_client()
        return AnalysisService(fec_client)
    
    async def get_fraud_service(
        self, 
        db: AsyncSession, 
        fec_client: Optional[FECClient] = None
    ) -> FraudDetectionService:
        """Get fraud detection service instance with contribution limits service"""
        if fec_client is None:
            fec_client = self.get_fec_client()
        limits_service = ContributionLimitsService(db)
        return FraudDetectionService(fec_client, limits_service=limits_service)
    
    def get_committee_service(self, fec_client: Optional[FECClient] = None) -> CommitteeService:
        """Get committee service instance"""
        if fec_client is None:
            fec_client = self.get_fec_client()
        return CommitteeService(fec_client)
    
    def get_independent_expenditure_service(
        self, 
        fec_client: Optional[FECClient] = None
    ) -> IndependentExpenditureService:
        """Get independent expenditure service instance"""
        if fec_client is None:
            fec_client = self.get_fec_client()
        return IndependentExpenditureService(fec_client)
    
    def get_trend_service(
        self, 
        fec_client: Optional[FECClient] = None
    ) -> TrendAnalysisService:
        """Get trend analysis service instance"""
        if fec_client is None:
            fec_client = self.get_fec_client()
        return TrendAnalysisService(fec_client)
    
    def get_donor_aggregation_service(self) -> DonorAggregationService:
        """Get donor aggregation service instance"""
        return DonorAggregationService()
    
    def get_contribution_limits_service(self, db: AsyncSession) -> ContributionLimitsService:
        """Get contribution limits service instance"""
        return ContributionLimitsService(db)
    
    def get_saved_search_service(self) -> SavedSearchService:
        """Get saved search service instance"""
        return SavedSearchService()
    
    async def cleanup(self):
        """Clean up all services"""
        if self._bulk_data_service:
            await self._bulk_data_service.cleanup()
        if self._fec_client:
            await self._fec_client.close()
        self._initialized = False


# Global service container instance
_service_container: Optional[ServiceContainer] = None


def get_service_container() -> ServiceContainer:
    """Get the global service container instance"""
    global _service_container
    if _service_container is None:
        _service_container = ServiceContainer()
    return _service_container

