from typing import Optional, Dict, List, Any
from app.db.database import AsyncSessionLocal, SavedSearch
from sqlalchemy import select, and_
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class SavedSearchService:
    """Service for managing saved searches"""
    
    async def create_saved_search(
        self,
        name: str,
        search_type: str,
        search_params: Dict[str, Any]
    ) -> SavedSearch:
        """Create a new saved search"""
        async with AsyncSessionLocal() as session:
            saved_search = SavedSearch(
                name=name,
                search_type=search_type,
                search_params=search_params
            )
            session.add(saved_search)
            await session.commit()
            await session.refresh(saved_search)
            return saved_search
    
    async def get_saved_search(self, search_id: int) -> Optional[SavedSearch]:
        """Get a saved search by ID"""
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(SavedSearch).where(SavedSearch.id == search_id)
            )
            return result.scalar_one_or_none()
    
    async def list_saved_searches(
        self,
        search_type: Optional[str] = None
    ) -> List[SavedSearch]:
        """List all saved searches, optionally filtered by type"""
        async with AsyncSessionLocal() as session:
            query = select(SavedSearch)
            if search_type:
                query = query.where(SavedSearch.search_type == search_type)
            query = query.order_by(SavedSearch.created_at.desc())
            result = await session.execute(query)
            return result.scalars().all()
    
    async def update_saved_search(
        self,
        search_id: int,
        name: Optional[str] = None,
        search_params: Optional[Dict[str, Any]] = None
    ) -> Optional[SavedSearch]:
        """Update a saved search"""
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(SavedSearch).where(SavedSearch.id == search_id)
            )
            saved_search = result.scalar_one_or_none()
            if not saved_search:
                return None
            
            if name is not None:
                saved_search.name = name
            if search_params is not None:
                saved_search.search_params = search_params
            saved_search.updated_at = datetime.utcnow()
            
            await session.commit()
            await session.refresh(saved_search)
            return saved_search
    
    async def delete_saved_search(self, search_id: int) -> bool:
        """Delete a saved search"""
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(SavedSearch).where(SavedSearch.id == search_id)
            )
            saved_search = result.scalar_one_or_none()
            if not saved_search:
                return False
            
            await session.delete(saved_search)
            await session.commit()
            return True

