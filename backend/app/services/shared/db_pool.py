"""
Database connection pool management utilities
"""
import logging
from typing import Optional, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


class DatabasePoolManager:
    """
    Manages database connection pool and provides utilities for connection handling
    """
    
    def __init__(
        self, 
        session_factory: Optional[async_sessionmaker] = None, 
        engine_instance: Optional[Engine] = None
    ) -> None:
        """
        Initialize database pool manager
        
        Args:
            session_factory: Optional custom session factory (defaults to AsyncSessionLocal)
            engine_instance: Optional engine instance (will be imported if not provided)
        """
        if session_factory is None:
            from app.db.database import AsyncSessionLocal
            self.session_factory = AsyncSessionLocal
        else:
            self.session_factory = session_factory
        
        if engine_instance is None:
            from app.db.database import engine
            self._engine = engine
        else:
            self._engine = engine_instance
        
        self._pool = self._engine.pool if hasattr(self._engine, 'pool') else None
    
    def get_pool_size(self) -> Optional[int]:
        """Get current pool size"""
        if self._pool:
            return getattr(self._pool, 'size', None)
        return None
    
    def get_checked_out_connections(self) -> Optional[int]:
        """Get number of connections currently checked out"""
        if self._pool:
            return getattr(self._pool, 'checkedout', None)
        return None
    
    def get_overflow_connections(self) -> Optional[int]:
        """Get number of overflow connections"""
        if self._pool:
            return getattr(self._pool, 'overflow', None)
        return None
    
    async def get_session(self) -> AsyncSession:
        """
        Get a database session from the pool
        
        Returns:
            AsyncSession instance
        """
        return self.session_factory()
    
    async def health_check(self) -> Dict[str, Any]:
        """
        Perform a health check on the database pool
        
        Returns:
            Dictionary with pool status information
        """
        try:
            # Import here to avoid circular dependency
            from app.db.database import AsyncSessionLocal
            async with AsyncSessionLocal() as session:
                # Simple query to verify connection works
                from sqlalchemy import text
                result = await session.execute(text("SELECT 1"))
                result.scalar()
            
            pool_size = self.get_pool_size()
            checked_out = self.get_checked_out_connections()
            overflow = self.get_overflow_connections()
            available = (pool_size - checked_out) if pool_size and checked_out else None
            
            # Log warning if pool is getting exhausted
            if pool_size and checked_out:
                utilization = (checked_out / pool_size) * 100 if pool_size > 0 else 0
                if utilization > 80:
                    logger.warning(
                        f"Database pool utilization high: {utilization:.1f}% "
                        f"({checked_out}/{pool_size} connections in use)"
                    )
                elif utilization > 60:
                    logger.debug(
                        f"Database pool utilization: {utilization:.1f}% "
                        f"({checked_out}/{pool_size} connections in use)"
                    )
            
            return {
                "status": "healthy",
                "pool_size": pool_size,
                "checked_out": checked_out,
                "overflow": overflow,
                "available": available,
                "utilization_percent": (
                    (checked_out / pool_size * 100) if pool_size and checked_out else None
                )
            }
        except Exception as e:
            logger.error(f"Database pool health check failed: {e}")
            return {
                "status": "unhealthy",
                "error": str(e)
            }
    
    def log_pool_stats(self) -> None:
        """Log current pool statistics for monitoring"""
        pool_size = self.get_pool_size()
        checked_out = self.get_checked_out_connections()
        overflow = self.get_overflow_connections()
        
        if pool_size:
            utilization = (checked_out / pool_size * 100) if checked_out else 0
            logger.info(
                f"Database pool stats: size={pool_size}, "
                f"checked_out={checked_out}, overflow={overflow}, "
                f"utilization={utilization:.1f}%"
            )


# Global instance
_db_pool_manager: Optional[DatabasePoolManager] = None


def get_db_pool_manager() -> DatabasePoolManager:
    """Get the global database pool manager instance"""
    global _db_pool_manager
    if _db_pool_manager is None:
        _db_pool_manager = DatabasePoolManager()
    return _db_pool_manager

