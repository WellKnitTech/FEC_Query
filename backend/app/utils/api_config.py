"""
API configuration utilities
Separated from dependencies to avoid circular imports
"""
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
    import logging
    logger = logging.getLogger(__name__)
    
    # Check environment variable first to avoid database query if possible
    # This prevents hanging on database locks for simple requests
    api_key = os.getenv("FEC_API_KEY")
    if api_key:
        logger.debug("Using API key from environment variable (skipping database check)")
        return api_key
    
    # Only check database if no env var is set
    # Add timeout to prevent hanging on database lock
    import asyncio
    from sqlalchemy.exc import OperationalError
    
    try:
        logger.debug("No env var found, checking database for UI-provided API key...")
        try:
            # Add timeout to database query to prevent hanging (Python 3.11+)
            if hasattr(asyncio, 'timeout'):
                async with asyncio.timeout(5.0):  # 5 second timeout
                    logger.debug("Creating database session...")
                    async with AsyncSessionLocal() as session:
                        logger.debug("Database session obtained, executing query...")
                        result = await session.execute(
                            select(ApiKeySetting).where(
                                ApiKeySetting.is_active == 1,
                                ApiKeySetting.source == "ui"
                            ).order_by(ApiKeySetting.updated_at.desc())
                        )
                        logger.debug("Query executed, fetching result...")
                        db_key = result.scalar_one_or_none()
                        if db_key and db_key.api_key:
                            logger.debug("Found UI-provided API key in database")
                            return db_key.api_key
                        logger.debug("No UI-provided API key found in database")
            else:
                # Fallback for Python < 3.11
                logger.debug("Creating database session...")
                async with AsyncSessionLocal() as session:
                    logger.debug("Database session obtained, executing query...")
                    result = await asyncio.wait_for(
                        session.execute(
                            select(ApiKeySetting).where(
                                ApiKeySetting.is_active == 1,
                                ApiKeySetting.source == "ui"
                            ).order_by(ApiKeySetting.updated_at.desc())
                        ),
                        timeout=5.0
                    )
                    logger.debug("Query executed, fetching result...")
                    db_key = result.scalar_one_or_none()
                    if db_key and db_key.api_key:
                        logger.debug("Found UI-provided API key in database")
                        return db_key.api_key
                    logger.debug("No UI-provided API key found in database")
        except asyncio.TimeoutError:
            logger.warning("Database query for API key timed out after 5 seconds (database may be locked)")
            pass
        except OperationalError as e:
            # Handle database lock errors specifically
            error_msg = str(e).lower()
            if "locked" in error_msg or "database is locked" in error_msg:
                logger.warning("Database is locked, skipping database API key check and using env var")
            else:
                logger.warning(f"Database operational error: {e}")
            pass
    except Exception as e:
        # If database check fails, fall back to env var
        logger.warning(f"Database check for API key failed: {e}")
        pass
    
    # If we get here, no key was found
    logger.error("FEC_API_KEY not found in environment or database")
    raise ValueError("FEC_API_KEY environment variable is not set and no UI key configured")


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

