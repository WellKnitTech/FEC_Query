"""
Centralized configuration management for the application

This module provides a single source of truth for all configuration settings,
loading from environment variables with sensible defaults. All configuration
values can be overridden via environment variables.

The Config class provides:
- Type-safe configuration access
- Sensible defaults for all settings
- Validation methods to check configuration
- Helper methods for database type detection

Example:
    ```python
    from app.config import config
    
    # Access configuration
    pool_size = config.SQLITE_POOL_SIZE
    
    # Validate configuration
    warnings = config.validate()
    
    # Check database type
    if config.is_sqlite():
        # SQLite-specific logic
        pass
    ```
"""
import os
from typing import List, Optional
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class Config:
    """
    Application configuration with environment variable support
    
    This class centralizes all application configuration, loading values from
    environment variables with sensible defaults. Configuration can be validated
    on startup to catch potential issues early.
    
    All configuration values can be overridden by setting corresponding environment
    variables. See env.example for a list of available configuration options.
    """
    
    # API Configuration
    FEC_API_KEY: str = os.getenv("FEC_API_KEY", "")
    FEC_API_BASE_URL: str = os.getenv("FEC_API_BASE_URL", "https://api.open.fec.gov/v1")
    
    # Database Configuration
    DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./fec_data.db")
    
    # SQLite-specific pool settings
    SQLITE_POOL_SIZE: int = int(os.getenv("SQLITE_POOL_SIZE", "10"))
    SQLITE_MAX_OVERFLOW: int = int(os.getenv("SQLITE_MAX_OVERFLOW", "10"))
    SQLITE_MAX_BATCH_SIZE: int = int(os.getenv("SQLITE_MAX_BATCH_SIZE", "90"))
    SQLITE_BULK_BATCH_SIZE: int = int(os.getenv("SQLITE_BULK_BATCH_SIZE", "500"))
    
    # PostgreSQL-specific pool settings (if using PostgreSQL)
    POSTGRES_POOL_SIZE: int = int(os.getenv("POSTGRES_POOL_SIZE", "20"))
    POSTGRES_MAX_OVERFLOW: int = int(os.getenv("POSTGRES_MAX_OVERFLOW", "30"))
    
    # Application Configuration
    DEBUG: bool = os.getenv("DEBUG", "False").lower() in ("true", "1", "yes")
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()
    
    # CORS Configuration
    CORS_ORIGINS: List[str] = [
        origin.strip() 
        for origin in os.getenv("CORS_ORIGINS", "http://localhost:3000,http://localhost:5173").split(",")
        if origin.strip()
    ]
    
    # Performance Configuration
    UVICORN_WORKERS: int = int(os.getenv("UVICORN_WORKERS", "1"))
    THREAD_POOL_WORKERS: int = int(os.getenv("THREAD_POOL_WORKERS", "4"))
    
    # Cache Configuration
    CACHE_TTL_HOURS: int = int(os.getenv("CACHE_TTL_HOURS", "24"))
    CACHE_TTL_CANDIDATES_HOURS: int = int(os.getenv("CACHE_TTL_CANDIDATES_HOURS", "168"))  # 7 days
    CACHE_TTL_COMMITTEES_HOURS: int = int(os.getenv("CACHE_TTL_COMMITTEES_HOURS", "168"))  # 7 days
    CACHE_TTL_FINANCIALS_HOURS: int = int(os.getenv("CACHE_TTL_FINANCIALS_HOURS", "24"))
    CACHE_TTL_CONTRIBUTIONS_HOURS: int = int(os.getenv("CACHE_TTL_CONTRIBUTIONS_HOURS", "24"))
    CACHE_TTL_EXPENDITURES_HOURS: int = int(os.getenv("CACHE_TTL_EXPENDITURES_HOURS", "24"))
    
    # Bulk Data Configuration
    BULK_DATA_ENABLED: bool = os.getenv("BULK_DATA_ENABLED", "true").lower() in ("true", "1", "yes")
    BULK_DATA_DIR: str = os.getenv("BULK_DATA_DIR", "./data/bulk")
    BULK_DATA_UPDATE_INTERVAL_HOURS: int = int(os.getenv("BULK_DATA_UPDATE_INTERVAL_HOURS", "24"))
    
    # Contribution Configuration
    CONTRIBUTION_LOOKBACK_DAYS: int = int(os.getenv("CONTRIBUTION_LOOKBACK_DAYS", "30"))
    DEFAULT_CHUNK_SIZE: int = int(os.getenv("DEFAULT_CHUNK_SIZE", "5000"))
    ANALYSIS_CHUNK_SIZE: int = int(os.getenv("ANALYSIS_CHUNK_SIZE", "5000"))
    
    # Background Task Configuration
    WAL_CHECKPOINT_INTERVAL_SECONDS: int = int(os.getenv("WAL_CHECKPOINT_INTERVAL_SECONDS", "1800"))  # 30 minutes
    INTEGRITY_CHECK_INTERVAL_HOURS: int = int(os.getenv("INTEGRITY_CHECK_INTERVAL_HOURS", "24"))
    
    # Rate Limiting Configuration
    RATE_LIMIT_PER_MINUTE: int = int(os.getenv("RATE_LIMIT_PER_MINUTE", "100"))
    RATE_LIMIT_PER_HOUR: int = int(os.getenv("RATE_LIMIT_PER_HOUR", "1000"))
    
    # Retry Configuration
    DEFAULT_MAX_RETRIES: int = int(os.getenv("DEFAULT_MAX_RETRIES", "3"))
    DEFAULT_RETRY_DELAY: float = float(os.getenv("DEFAULT_RETRY_DELAY", "0.1"))
    
    @classmethod
    def validate(cls) -> List[str]:
        """
        Validate configuration and return list of warnings/errors
        
        Returns:
            List of validation messages (empty if all valid)
        """
        warnings = []
        
        if not cls.FEC_API_KEY:
            warnings.append("FEC_API_KEY is not set - API calls will fail")
        
        if cls.UVICORN_WORKERS > 1 and cls.DATABASE_URL.startswith("sqlite"):
            warnings.append(
                f"Warning: Using {cls.UVICORN_WORKERS} workers with SQLite may cause "
                "database lock contention during bulk imports. Consider using 1-2 workers."
            )
        
        if cls.SQLITE_POOL_SIZE > 20:
            warnings.append(
                f"Warning: SQLite pool size ({cls.SQLITE_POOL_SIZE}) is high. "
                "SQLite works better with smaller pools (10-15 recommended)."
            )
        
        if cls.THREAD_POOL_WORKERS < 1:
            warnings.append("THREAD_POOL_WORKERS must be at least 1")
        
        return warnings
    
    @classmethod
    def is_sqlite(cls) -> bool:
        """Check if using SQLite database"""
        return cls.DATABASE_URL.startswith("sqlite")
    
    @classmethod
    def is_postgres(cls) -> bool:
        """Check if using PostgreSQL database"""
        return cls.DATABASE_URL.startswith("postgresql")
    
    @classmethod
    def get_cache_ttls(cls) -> dict:
        """Get cache TTL configuration as dictionary"""
        return {
            "candidates": cls.CACHE_TTL_CANDIDATES_HOURS,
            "committees": cls.CACHE_TTL_COMMITTEES_HOURS,
            "financials": cls.CACHE_TTL_FINANCIALS_HOURS,
            "contributions": cls.CACHE_TTL_CONTRIBUTIONS_HOURS,
            "expenditures": cls.CACHE_TTL_EXPENDITURES_HOURS,
            "default": cls.CACHE_TTL_HOURS,
        }


# Global config instance
config = Config()

