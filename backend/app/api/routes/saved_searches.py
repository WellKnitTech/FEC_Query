from fastapi import APIRouter, HTTPException, Query, Body
from typing import Optional, List
from pydantic import BaseModel
from app.services.saved_searches import SavedSearchService
from app.db.database import SavedSearch
from app.utils.logging import get_logger

logger = get_logger(__name__)

router = APIRouter()


class SavedSearchCreate(BaseModel):
    name: str
    search_type: str  # 'candidate', 'committee', 'race', 'donor', 'independent_expenditure'
    search_params: dict


class SavedSearchUpdate(BaseModel):
    name: Optional[str] = None
    search_params: Optional[dict] = None


def get_saved_search_service():
    """Get saved search service instance"""
    return SavedSearchService()


@router.get("/", response_model=List[dict])
async def list_saved_searches(
    search_type: Optional[str] = Query(None, description="Filter by search type")
):
    """List all saved searches"""
    try:
        service = get_saved_search_service()
        searches = await service.list_saved_searches(search_type=search_type)
        return [
            {
                "id": search.id,
                "name": search.name,
                "search_type": search.search_type,
                "search_params": search.search_params,
                "created_at": search.created_at.isoformat(),
                "updated_at": search.updated_at.isoformat(),
            }
            for search in searches
        ]
    except Exception as e:
        logger.error(f"Error listing saved searches: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to list saved searches: {str(e)}")


@router.post("/", response_model=dict)
async def create_saved_search(request: SavedSearchCreate):
    """Create a new saved search"""
    try:
        service = get_saved_search_service()
        saved_search = await service.create_saved_search(
            name=request.name,
            search_type=request.search_type,
            search_params=request.search_params
        )
        return {
            "id": saved_search.id,
            "name": saved_search.name,
            "search_type": saved_search.search_type,
            "search_params": saved_search.search_params,
            "created_at": saved_search.created_at.isoformat(),
            "updated_at": saved_search.updated_at.isoformat(),
        }
    except Exception as e:
        logger.error(f"Error creating saved search: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to create saved search: {str(e)}")


@router.get("/{search_id}", response_model=dict)
async def get_saved_search(search_id: int):
    """Get a saved search by ID"""
    try:
        service = get_saved_search_service()
        saved_search = await service.get_saved_search(search_id)
        if not saved_search:
            raise HTTPException(status_code=404, detail="Saved search not found")
        return {
            "id": saved_search.id,
            "name": saved_search.name,
            "search_type": saved_search.search_type,
            "search_params": saved_search.search_params,
            "created_at": saved_search.created_at.isoformat(),
            "updated_at": saved_search.updated_at.isoformat(),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting saved search {search_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get saved search: {str(e)}")


@router.put("/{search_id}", response_model=dict)
async def update_saved_search(search_id: int, request: SavedSearchUpdate):
    """Update a saved search"""
    try:
        service = get_saved_search_service()
        saved_search = await service.update_saved_search(
            search_id=search_id,
            name=request.name,
            search_params=request.search_params
        )
        if not saved_search:
            raise HTTPException(status_code=404, detail="Saved search not found")
        return {
            "id": saved_search.id,
            "name": saved_search.name,
            "search_type": saved_search.search_type,
            "search_params": saved_search.search_params,
            "created_at": saved_search.created_at.isoformat(),
            "updated_at": saved_search.updated_at.isoformat(),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating saved search {search_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to update saved search: {str(e)}")


@router.delete("/{search_id}")
async def delete_saved_search(search_id: int):
    """Delete a saved search"""
    try:
        service = get_saved_search_service()
        success = await service.delete_saved_search(search_id)
        if not success:
            raise HTTPException(status_code=404, detail="Saved search not found")
        return {"message": "Saved search deleted successfully"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting saved search {search_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to delete saved search: {str(e)}")

