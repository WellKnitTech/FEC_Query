from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Optional
from app.db.database import AsyncSessionLocal, ApiKeySetting
from sqlalchemy import select
from datetime import datetime
from app.utils.logging import get_logger

logger = get_logger(__name__)

router = APIRouter()


class ApiKeyRequest(BaseModel):
    api_key: str = Field(..., min_length=1, max_length=200, description="FEC API key")


class ApiKeyStatus(BaseModel):
    has_key: bool
    key_preview: Optional[str] = None
    source: str  # 'ui' or 'env'


def mask_api_key(api_key: str) -> str:
    """Mask API key showing only first 4 and last 4 characters"""
    if len(api_key) <= 8:
        return "*" * len(api_key)
    return f"{api_key[:4]}...{api_key[-4:]}"


@router.get("/api-key", response_model=ApiKeyStatus)
async def get_api_key_status():
    """Get current API key status (masked)"""
    try:
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(ApiKeySetting).where(
                    ApiKeySetting.is_active == 1,
                    ApiKeySetting.source == "ui"
                ).order_by(ApiKeySetting.updated_at.desc())
            )
            db_key = result.scalar_one_or_none()
            
            if db_key and db_key.api_key:
                return ApiKeyStatus(
                    has_key=True,
                    key_preview=mask_api_key(db_key.api_key),
                    source="ui"
                )
            else:
                # Check if env var exists
                import os
                env_key = os.getenv("FEC_API_KEY")
                if env_key:
                    return ApiKeyStatus(
                        has_key=True,
                        key_preview=mask_api_key(env_key),
                        source="env"
                    )
                else:
                    return ApiKeyStatus(
                        has_key=False,
                        source="env"
                    )
    except Exception as e:
        logger.error(f"Error getting API key status: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get API key status: {str(e)}")


@router.post("/api-key")
async def set_api_key(request: ApiKeyRequest):
    """Set or update API key"""
    try:
        # Validate API key format (basic validation)
        api_key = request.api_key.strip()
        if not api_key or len(api_key) < 10:
            raise HTTPException(
                status_code=400,
                detail="API key must be at least 10 characters long"
            )
        
        async with AsyncSessionLocal() as session:
            # Mark all existing active UI keys as inactive
            result = await session.execute(
                select(ApiKeySetting).where(
                    ApiKeySetting.is_active == 1,
                    ApiKeySetting.source == "ui"
                )
            )
            existing_keys = result.scalars().all()
            for key in existing_keys:
                key.is_active = 0
                key.updated_at = datetime.utcnow()
            
            # Create new active key
            new_key = ApiKeySetting(
                api_key=api_key,
                source="ui",
                is_active=1
            )
            session.add(new_key)
            await session.commit()
            
            logger.info("API key updated via UI")
            return {"message": "API key saved successfully", "key_preview": mask_api_key(api_key)}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error setting API key: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to set API key: {str(e)}")


@router.delete("/api-key")
async def delete_api_key():
    """Remove API key (soft delete)"""
    try:
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(ApiKeySetting).where(
                    ApiKeySetting.is_active == 1,
                    ApiKeySetting.source == "ui"
                ).order_by(ApiKeySetting.updated_at.desc())
            )
            active_key = result.scalar_one_or_none()
            
            if not active_key:
                raise HTTPException(
                    status_code=404,
                    detail="No UI-configured API key found to delete"
                )
            
            # Soft delete
            active_key.is_active = 0
            active_key.updated_at = datetime.utcnow()
            await session.commit()
            
            logger.info("API key removed via UI")
            return {"message": "API key removed successfully"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting API key: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to delete API key: {str(e)}")

