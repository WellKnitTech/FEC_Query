import os
from dotenv import load_dotenv
from app.db.database import AsyncSessionLocal, ApiKeySetting
from sqlalchemy import select

load_dotenv()


async def get_fec_api_key() -> str:
    """
    Get FEC API key - checks database first (UI-provided key), 
    then falls back to environment variable.
    UI-provided keys take precedence over environment variables.
    """
    # Check database for active UI-provided key first
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
                return db_key.api_key
    except Exception as e:
        # If database check fails, fall back to env var
        pass
    
    # Fall back to environment variable
    api_key = os.getenv("FEC_API_KEY")
    if not api_key:
        raise ValueError("FEC_API_KEY environment variable is not set and no UI key configured")
    return api_key


async def get_fec_api_key_with_source() -> tuple[str, str]:
    """
    Get FEC API key with source information.
    Returns: (api_key: str, source: 'ui' | 'env')
    """
    # Check database for active UI-provided key first
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
                return (db_key.api_key, "ui")
    except Exception:
        pass
    
    # Fall back to environment variable
    api_key = os.getenv("FEC_API_KEY")
    if not api_key:
        raise ValueError("FEC_API_KEY environment variable is not set and no UI key configured")
    return (api_key, "env")


def get_fec_api_base_url() -> str:
    """Get FEC API base URL from environment"""
    return os.getenv("FEC_API_BASE_URL", "https://api.open.fec.gov/v1")

