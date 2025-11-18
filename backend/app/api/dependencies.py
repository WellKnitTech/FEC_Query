import logging
from fastapi import Depends, HTTPException
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from app.utils.api_config import get_fec_api_key, get_fec_api_key_with_source, get_fec_api_base_url
from app.db.database import get_db
from app.services.fec_client import FECClient
from app.services.analysis import AnalysisService
from app.services.fraud_detection import FraudDetectionService
from app.services.committees import CommitteeService
from app.services.independent_expenditures import IndependentExpenditureService
from app.services.trends import TrendAnalysisService
from app.services.bulk_data import BulkDataService
from app.services.container import get_service_container

logger = logging.getLogger(__name__)


async def get_fec_client() -> FECClient:
    """Get FEC client instance"""
    try:
        container = get_service_container()
        api_key = await get_fec_api_key()
        return container.get_fec_client(api_key=api_key)
    except ValueError as e:
        logger.error(f"FEC API key not configured: {e}")
        raise HTTPException(
            status_code=500,
            detail="FEC API key not configured. Please set FEC_API_KEY in your .env file."
        )


async def get_analysis_service(
    fec_client: FECClient = Depends(get_fec_client)
) -> AnalysisService:
    """Get analysis service instance"""
    container = get_service_container()
    return container.get_analysis_service(fec_client=fec_client)


async def get_fraud_service(
    db: AsyncSession = Depends(get_db),
    fec_client: FECClient = Depends(get_fec_client)
) -> FraudDetectionService:
    """Get fraud detection service instance with contribution limits service"""
    container = get_service_container()
    return await container.get_fraud_service(db=db, fec_client=fec_client)


async def get_committee_service(
    fec_client: FECClient = Depends(get_fec_client)
) -> CommitteeService:
    """Get committee service instance"""
    container = get_service_container()
    return container.get_committee_service(fec_client=fec_client)


async def get_independent_expenditure_service(
    fec_client: FECClient = Depends(get_fec_client)
) -> IndependentExpenditureService:
    """Get independent expenditure service instance"""
    container = get_service_container()
    return container.get_independent_expenditure_service(fec_client=fec_client)


async def get_trend_service(
    fec_client: FECClient = Depends(get_fec_client)
) -> TrendAnalysisService:
    """Get trend analysis service instance"""
    container = get_service_container()
    return container.get_trend_service(fec_client=fec_client)


def get_bulk_data_service() -> BulkDataService:
    """Get bulk data service instance"""
    container = get_service_container()
    return container.get_bulk_data_service()

