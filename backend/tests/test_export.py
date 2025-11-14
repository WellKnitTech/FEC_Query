"""
Tests for Export API endpoints
"""
import pytest
from httpx import AsyncClient


@pytest.mark.integration
@pytest.mark.asyncio
async def test_export_candidate_pdf(client: AsyncClient, sample_candidate_id: str):
    """Test exporting candidate report as PDF"""
    response = await client.get(
        f"/api/export/candidate/{sample_candidate_id}",
        params={"format": "pdf"}
    )
    assert response.status_code in [200, 404]
    
    if response.status_code == 200:
        assert response.headers["content-type"] == "application/pdf"
        assert "attachment" in response.headers.get("content-disposition", "").lower()
        assert len(response.content) > 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_export_candidate_docx(client: AsyncClient, sample_candidate_id: str):
    """Test exporting candidate report as DOCX"""
    response = await client.get(
        f"/api/export/candidate/{sample_candidate_id}",
        params={"format": "docx"}
    )
    assert response.status_code in [200, 404]
    
    if response.status_code == 200:
        assert "wordprocessingml" in response.headers["content-type"]
        assert "attachment" in response.headers.get("content-disposition", "").lower()
        assert len(response.content) > 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_export_candidate_csv(client: AsyncClient, sample_candidate_id: str):
    """Test exporting candidate report as CSV"""
    response = await client.get(
        f"/api/export/candidate/{sample_candidate_id}",
        params={"format": "csv"}
    )
    assert response.status_code in [200, 404]
    
    if response.status_code == 200:
        assert response.headers["content-type"] == "text/csv"
        assert "attachment" in response.headers.get("content-disposition", "").lower()
        assert len(response.content) > 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_export_candidate_excel(client: AsyncClient, sample_candidate_id: str):
    """Test exporting candidate report as Excel"""
    response = await client.get(
        f"/api/export/candidate/{sample_candidate_id}",
        params={"format": "excel"}
    )
    assert response.status_code in [200, 404]
    
    if response.status_code == 200:
        assert "spreadsheetml" in response.headers["content-type"]
        assert "attachment" in response.headers.get("content-disposition", "").lower()
        assert len(response.content) > 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_export_candidate_markdown(client: AsyncClient, sample_candidate_id: str):
    """Test exporting candidate report as Markdown"""
    response = await client.get(
        f"/api/export/candidate/{sample_candidate_id}",
        params={"format": "md"}
    )
    assert response.status_code in [200, 404]
    
    if response.status_code == 200:
        assert response.headers["content-type"] == "text/markdown"
        assert "attachment" in response.headers.get("content-disposition", "").lower()
        assert len(response.content) > 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_export_candidate_with_cycle(client: AsyncClient, sample_candidate_id: str, sample_cycle: int):
    """Test exporting candidate report with specific cycle"""
    response = await client.get(
        f"/api/export/candidate/{sample_candidate_id}",
        params={"format": "pdf", "cycle": sample_cycle}
    )
    assert response.status_code in [200, 404]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_export_race_pdf(client: AsyncClient):
    """Test exporting race report as PDF"""
    candidate_ids = ["P00003392", "P80000722"]
    
    response = await client.post(
        "/api/export/race",
        json={
            "candidate_ids": candidate_ids,
            "office": "P",
            "state": "CA",
            "format": "pdf"
        }
    )
    assert response.status_code in [200, 400, 404]
    
    if response.status_code == 200:
        assert response.headers["content-type"] == "application/pdf"
        assert len(response.content) > 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_export_race_docx(client: AsyncClient):
    """Test exporting race report as DOCX"""
    candidate_ids = ["P00003392", "P80000722"]
    
    response = await client.post(
        "/api/export/race",
        json={
            "candidate_ids": candidate_ids,
            "office": "P",
            "state": "CA",
            "format": "docx"
        }
    )
    assert response.status_code in [200, 400, 404]
    
    if response.status_code == 200:
        assert "wordprocessingml" in response.headers["content-type"]
        assert len(response.content) > 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_export_race_with_district(client: AsyncClient):
    """Test exporting race report with district"""
    candidate_ids = ["H00000001", "H00000002"]
    
    response = await client.post(
        "/api/export/race",
        json={
            "candidate_ids": candidate_ids,
            "office": "H",
            "state": "TX",
            "district": "01",
            "format": "pdf"
        }
    )
    assert response.status_code in [200, 400, 404]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_export_contributions_csv(client: AsyncClient, sample_candidate_id: str):
    """Test exporting contributions as CSV"""
    response = await client.get(
        "/api/export/contributions/csv",
        params={"candidate_id": sample_candidate_id, "limit": 100}
    )
    assert response.status_code in [200, 404]
    
    if response.status_code == 200:
        assert response.headers["content-type"] == "text/csv"
        assert "attachment" in response.headers.get("content-disposition", "").lower()
        # Verify CSV content
        content = response.text
        assert len(content) > 0
        # Should have header row
        assert "Contribution ID" in content or "contributor" in content.lower()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_export_contributions_excel(client: AsyncClient, sample_candidate_id: str):
    """Test exporting contributions as Excel"""
    response = await client.get(
        "/api/export/contributions/excel",
        params={"candidate_id": sample_candidate_id, "limit": 100}
    )
    assert response.status_code in [200, 404]
    
    if response.status_code == 200:
        assert "spreadsheetml" in response.headers["content-type"]
        assert "attachment" in response.headers.get("content-disposition", "").lower()
        assert len(response.content) > 0


@pytest.mark.asyncio
async def test_export_candidate_invalid_format(client: AsyncClient, sample_candidate_id: str):
    """Test exporting candidate with invalid format"""
    response = await client.get(
        f"/api/export/candidate/{sample_candidate_id}",
        params={"format": "invalid_format"}
    )
    # Should return validation error
    assert response.status_code in [400, 422]


@pytest.mark.asyncio
async def test_export_candidate_invalid_id(client: AsyncClient):
    """Test exporting candidate with invalid ID"""
    response = await client.get(
        "/api/export/candidate/INVALID_ID_12345",
        params={"format": "pdf"}
    )
    assert response.status_code in [404, 500]


@pytest.mark.asyncio
async def test_export_race_empty_candidates(client: AsyncClient):
    """Test exporting race with empty candidate list"""
    response = await client.post(
        "/api/export/race",
        json={
            "candidate_ids": [],
            "office": "P",
            "state": "CA",
            "format": "pdf"
        }
    )
    # Should return validation error
    assert response.status_code in [400, 422]


@pytest.mark.asyncio
async def test_export_contributions_no_filters(client: AsyncClient):
    """Test exporting contributions without filters"""
    response = await client.get(
        "/api/export/contributions/csv",
        params={"limit": 10}
    )
    # Should work but may return empty or limited results
    assert response.status_code in [200, 400]

