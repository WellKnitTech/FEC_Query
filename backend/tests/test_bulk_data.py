"""
Tests for Bulk Data Management API endpoints
"""
import pytest
from httpx import AsyncClient
from tests.helpers.db_helpers import get_bulk_data_status, get_job


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_data_types(client: AsyncClient):
    """Test getting list of available data types"""
    response = await client.get("/api/bulk-data/data-types")
    assert response.status_code == 200
    data = response.json()
    assert "data_types" in data
    assert "count" in data
    assert isinstance(data["data_types"], list)
    assert isinstance(data["count"], int)
    
    if data["data_types"]:
        data_type = data["data_types"][0]
        assert "data_type" in data_type
        assert "description" in data_type
        assert "file_format" in data_type


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_bulk_data_status(client: AsyncClient):
    """Test getting bulk data status"""
    response = await client.get("/api/bulk-data/status")
    assert response.status_code == 200
    status = response.json()
    # Status structure may vary, but should be a dict
    assert isinstance(status, dict)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_cycle_status(client: AsyncClient, sample_cycle: int):
    """Test getting status for a specific cycle"""
    response = await client.get(f"/api/bulk-data/status/{sample_cycle}")
    assert response.status_code == 200
    data = response.json()
    assert "cycle" in data
    assert "data_types" in data
    assert "count" in data
    assert data["cycle"] == sample_cycle
    assert isinstance(data["data_types"], list)
    assert isinstance(data["count"], int)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_available_cycles(client: AsyncClient):
    """Test getting available cycles"""
    response = await client.get("/api/bulk-data/cycles")
    assert response.status_code == 200
    data = response.json()
    assert "cycles" in data
    assert "count" in data
    assert isinstance(data["cycles"], list)
    assert isinstance(data["count"], int)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_available_cycles_without_api(client: AsyncClient):
    """Test getting available cycles without querying FEC API"""
    response = await client.get(
        "/api/bulk-data/cycles",
        params={"use_fec_api": False}
    )
    assert response.status_code == 200
    data = response.json()
    assert "cycles" in data
    assert "source" in data
    assert data["source"] == "fallback"


@pytest.mark.database
@pytest.mark.asyncio
async def test_bulk_data_status_in_database(db_session, sample_cycle: int):
    """Test getting bulk data status from database"""
    statuses = await get_bulk_data_status(db_session, sample_cycle)
    assert isinstance(statuses, list)
    # May be empty if no data imported


@pytest.mark.slow
@pytest.mark.asyncio
async def test_download_bulk_data(client: AsyncClient, sample_cycle: int):
    """Test starting a bulk data download (runs in background)"""
    response = await client.post(
        "/api/bulk-data/download",
        params={"cycle": sample_cycle, "data_type": "individual_contributions"}
    )
    # Should return 200 and start job in background
    assert response.status_code == 200
    data = response.json()
    assert "job_id" in data
    assert "status" in data
    assert data["status"] == "started"
    assert "message" in data


@pytest.mark.asyncio
async def test_download_bulk_data_invalid_cycle(client: AsyncClient):
    """Test downloading bulk data with invalid cycle"""
    response = await client.post(
        "/api/bulk-data/download",
        params={"cycle": 1900, "data_type": "individual_contributions"}
    )
    # May return 200 (starts job) or 400 (validation error)
    assert response.status_code in [200, 400]


@pytest.mark.asyncio
async def test_download_bulk_data_invalid_type(client: AsyncClient, sample_cycle: int):
    """Test downloading bulk data with invalid data type"""
    response = await client.post(
        "/api/bulk-data/download",
        params={"cycle": sample_cycle, "data_type": "invalid_type"}
    )
    # Should return 400 (validation error)
    assert response.status_code == 400
    assert "invalid" in response.json()["detail"].lower()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_job_status(client: AsyncClient):
    """Test getting job status (may not exist)"""
    # Try with a likely non-existent job ID
    response = await client.get("/api/bulk-data/jobs/test-job-id-12345/status")
    # Should return 404 if job doesn't exist, or 200 if it does
    assert response.status_code in [200, 404]


@pytest.mark.asyncio
async def test_cancel_job(client: AsyncClient):
    """Test cancelling a job (may not exist)"""
    response = await client.post("/api/bulk-data/jobs/test-job-id-12345/cancel")
    # Should return 400 if job doesn't exist or can't be cancelled, or 200 if successful
    assert response.status_code in [200, 400, 404]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_incomplete_jobs(client: AsyncClient):
    """Test getting incomplete jobs"""
    response = await client.get("/api/bulk-data/jobs/incomplete")
    assert response.status_code == 200
    data = response.json()
    assert "jobs" in data
    assert "count" in data
    assert isinstance(data["jobs"], list)
    assert isinstance(data["count"], int)


@pytest.mark.database
@pytest.mark.asyncio
async def test_job_in_database(db_session):
    """Test that job can be queried from database"""
    job = await get_job(db_session, "test-job-id-12345")
    # May or may not exist
    if job:
        assert job.id == "test-job-id-12345"
        assert job.status in ["pending", "running", "completed", "failed", "cancelled"]


@pytest.mark.asyncio
async def test_backfill_candidate_ids_stats(client: AsyncClient):
    """Test getting backfill stats"""
    response = await client.get("/api/bulk-data/backfill-candidate-ids/stats")
    assert response.status_code == 200
    stats = response.json()
    assert "contributions_missing_candidate_id" in stats
    assert isinstance(stats["contributions_missing_candidate_id"], int)
    assert stats["contributions_missing_candidate_id"] >= 0


@pytest.mark.asyncio
async def test_get_invalid_committee_ids(client: AsyncClient):
    """Test getting invalid committee IDs"""
    response = await client.get("/api/bulk-data/committee-ids/invalid")
    assert response.status_code == 200
    data = response.json()
    assert "total" in data
    assert "valid" in data
    assert "invalid" in data
    assert isinstance(data["total"], int)
    assert isinstance(data["valid"], int)
    assert isinstance(data["invalid"], int)

