"""
Tests for Settings API endpoints
"""
import pytest
from httpx import AsyncClient


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_api_key_status(client: AsyncClient):
    """Test getting API key status"""
    response = await client.get("/api/settings/api-key")
    assert response.status_code == 200
    status = response.json()
    assert "has_key" in status
    assert "source" in status
    assert isinstance(status["has_key"], bool)
    assert status["source"] in ["ui", "env"]
    
    if status["has_key"]:
        assert "key_preview" in status
        assert isinstance(status["key_preview"], str)
        # Key should be masked
        if len(status["key_preview"]) > 8:
            assert "..." in status["key_preview"]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_set_api_key(client: AsyncClient):
    """Test setting API key"""
    test_key = "TEST_API_KEY_12345678901234567890"
    
    response = await client.post(
        "/api/settings/api-key",
        json={"api_key": test_key}
    )
    assert response.status_code == 200
    data = response.json()
    assert "message" in data
    assert "key_preview" in data
    assert "..." in data["key_preview"]
    
    # Verify it was set
    get_response = await client.get("/api/settings/api-key")
    assert get_response.status_code == 200
    status = get_response.json()
    assert status["has_key"] is True
    assert status["source"] == "ui"
    
    # Cleanup - delete the key
    delete_response = await client.delete("/api/settings/api-key")
    assert delete_response.status_code == 200


@pytest.mark.integration
@pytest.mark.asyncio
async def test_update_api_key(client: AsyncClient):
    """Test updating API key"""
    # Set initial key
    initial_key = "INITIAL_KEY_12345678901234567890"
    await client.post(
        "/api/settings/api-key",
        json={"api_key": initial_key}
    )
    
    # Update to new key
    new_key = "NEW_KEY_12345678901234567890"
    response = await client.post(
        "/api/settings/api-key",
        json={"api_key": new_key}
    )
    assert response.status_code == 200
    
    # Verify it was updated
    get_response = await client.get("/api/settings/api-key")
    assert get_response.status_code == 200
    status = get_response.json()
    assert status["has_key"] is True
    
    # Cleanup
    await client.delete("/api/settings/api-key")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_delete_api_key(client: AsyncClient):
    """Test deleting API key"""
    # First set a key
    test_key = "DELETE_TEST_KEY_12345678901234567890"
    await client.post(
        "/api/settings/api-key",
        json={"api_key": test_key}
    )
    
    # Then delete it
    response = await client.delete("/api/settings/api-key")
    assert response.status_code == 200
    assert "message" in response.json()
    
    # Verify it was deleted
    get_response = await client.get("/api/settings/api-key")
    assert get_response.status_code == 200
    status = get_response.json()
    # May still show has_key=True if env var exists
    if status["source"] == "ui":
        assert status["has_key"] is False


@pytest.mark.asyncio
async def test_set_api_key_too_short(client: AsyncClient):
    """Test setting API key that's too short"""
    response = await client.post(
        "/api/settings/api-key",
        json={"api_key": "short"}  # Less than 10 characters
    )
    assert response.status_code == 400
    assert "at least 10" in response.json()["detail"].lower()


@pytest.mark.asyncio
async def test_set_api_key_empty(client: AsyncClient):
    """Test setting empty API key"""
    response = await client.post(
        "/api/settings/api-key",
        json={"api_key": ""}
    )
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_delete_api_key_when_none_exists(client: AsyncClient):
    """Test deleting API key when none exists in UI"""
    # Try to delete (may fail if env var exists)
    response = await client.delete("/api/settings/api-key")
    # Should return 200 (success) or 404 (not found)
    assert response.status_code in [200, 404]


@pytest.mark.asyncio
async def test_api_key_masking(client: AsyncClient):
    """Test that API key is properly masked in preview"""
    test_key = "VERY_LONG_API_KEY_123456789012345678901234567890"
    
    # Set key
    set_response = await client.post(
        "/api/settings/api-key",
        json={"api_key": test_key}
    )
    assert set_response.status_code == 200
    set_data = set_response.json()
    
    # Check preview is masked
    assert "key_preview" in set_data
    preview = set_data["key_preview"]
    assert "..." in preview
    assert len(preview) < len(test_key)
    # Should show first 4 and last 4 characters
    assert preview.startswith(test_key[:4])
    assert preview.endswith(test_key[-4:])
    
    # Get status and verify masking
    get_response = await client.get("/api/settings/api-key")
    assert get_response.status_code == 200
    status = get_response.json()
    if status["has_key"] and status["source"] == "ui":
        assert "..." in status["key_preview"]
    
    # Cleanup
    await client.delete("/api/settings/api-key")

