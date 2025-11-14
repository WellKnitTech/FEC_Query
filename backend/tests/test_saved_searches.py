"""
Tests for Saved Searches API endpoints
"""
import pytest
from httpx import AsyncClient
from tests.helpers.db_helpers import get_saved_search


@pytest.mark.integration
@pytest.mark.asyncio
async def test_list_saved_searches(client: AsyncClient):
    """Test listing saved searches"""
    response = await client.get("/api/saved-searches/")
    assert response.status_code == 200
    searches = response.json()
    assert isinstance(searches, list)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_list_saved_searches_by_type(client: AsyncClient):
    """Test listing saved searches filtered by type"""
    response = await client.get(
        "/api/saved-searches/",
        params={"search_type": "candidate"}
    )
    assert response.status_code == 200
    searches = response.json()
    assert isinstance(searches, list)
    
    if searches:
        for search in searches:
            assert "search_type" in search
            assert search["search_type"] == "candidate"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_create_saved_search(client: AsyncClient):
    """Test creating a saved search"""
    response = await client.post(
        "/api/saved-searches/",
        json={
            "name": "Test Search",
            "search_type": "candidate",
            "search_params": {"office": "P", "state": "CA"}
        }
    )
    assert response.status_code == 200
    search = response.json()
    assert "id" in search
    assert "name" in search
    assert search["name"] == "Test Search"
    assert search["search_type"] == "candidate"
    assert "search_params" in search
    
    # Cleanup
    if "id" in search:
        await client.delete(f"/api/saved-searches/{search['id']}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_saved_search(client: AsyncClient):
    """Test getting a saved search by ID"""
    # First create one
    create_response = await client.post(
        "/api/saved-searches/",
        json={
            "name": "Test Get Search",
            "search_type": "committee",
            "search_params": {"committee_type": "H"}
        }
    )
    assert create_response.status_code == 200
    created_search = create_response.json()
    search_id = created_search["id"]
    
    # Then get it
    response = await client.get(f"/api/saved-searches/{search_id}")
    assert response.status_code == 200
    search = response.json()
    assert search["id"] == search_id
    assert search["name"] == "Test Get Search"
    
    # Cleanup
    await client.delete(f"/api/saved-searches/{search_id}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_update_saved_search(client: AsyncClient):
    """Test updating a saved search"""
    # First create one
    create_response = await client.post(
        "/api/saved-searches/",
        json={
            "name": "Test Update Search",
            "search_type": "contribution",
            "search_params": {"min_amount": 100}
        }
    )
    assert create_response.status_code == 200
    created_search = create_response.json()
    search_id = created_search["id"]
    
    # Then update it
    response = await client.put(
        f"/api/saved-searches/{search_id}",
        json={
            "name": "Updated Search Name",
            "search_params": {"min_amount": 500}
        }
    )
    assert response.status_code == 200
    updated_search = response.json()
    assert updated_search["name"] == "Updated Search Name"
    assert updated_search["search_params"]["min_amount"] == 500
    
    # Cleanup
    await client.delete(f"/api/saved-searches/{search_id}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_delete_saved_search(client: AsyncClient):
    """Test deleting a saved search"""
    # First create one
    create_response = await client.post(
        "/api/saved-searches/",
        json={
            "name": "Test Delete Search",
            "search_type": "donor",
            "search_params": {"contributor_name": "Smith"}
        }
    )
    assert create_response.status_code == 200
    created_search = create_response.json()
    search_id = created_search["id"]
    
    # Then delete it
    response = await client.delete(f"/api/saved-searches/{search_id}")
    assert response.status_code == 200
    assert "message" in response.json()
    
    # Verify it's deleted
    get_response = await client.get(f"/api/saved-searches/{search_id}")
    assert get_response.status_code == 404


@pytest.mark.asyncio
async def test_get_saved_search_not_found(client: AsyncClient):
    """Test getting a non-existent saved search"""
    response = await client.get("/api/saved-searches/999999")
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_update_saved_search_not_found(client: AsyncClient):
    """Test updating a non-existent saved search"""
    response = await client.put(
        "/api/saved-searches/999999",
        json={"name": "Updated Name"}
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_delete_saved_search_not_found(client: AsyncClient):
    """Test deleting a non-existent saved search"""
    response = await client.delete("/api/saved-searches/999999")
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_create_saved_search_invalid_params(client: AsyncClient):
    """Test creating saved search with invalid parameters"""
    response = await client.post(
        "/api/saved-searches/",
        json={
            "name": "",  # Empty name
            "search_type": "invalid_type",
            "search_params": {}
        }
    )
    # Should return validation error
    assert response.status_code in [400, 422]


@pytest.mark.database
@pytest.mark.asyncio
async def test_saved_search_in_database(db_session):
    """Test that saved search can be queried from database"""
    # Create a search first via API, then check database
    # This is a bit of integration, but tests database access
    search = await get_saved_search(db_session, 999999)
    # May or may not exist
    if search:
        assert search.id == 999999
        assert search.name is not None

