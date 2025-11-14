"""
Pytest configuration and fixtures for FEC Query API tests
"""
import pytest
import asyncio
import os
import sys
from typing import AsyncGenerator, Generator
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import text
from datetime import datetime
import tempfile
import shutil

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.main import app
from app.db.database import Base, AsyncSessionLocal, engine
from app.db.database import (
    Contribution, Candidate, Committee, BulkDataMetadata,
    BulkImportJob, SavedSearch, ApiKeySetting, FinancialTotal
)


# Test database URL - use in-memory SQLite for tests
TEST_DATABASE_URL = "sqlite+aiosqlite:///:memory:"


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="function")
async def test_db() -> AsyncGenerator[AsyncSession, None]:
    """
    Create a test database session with in-memory SQLite.
    Each test gets a fresh database.
    """
    # Create a new in-memory database engine for tests
    test_engine = create_async_engine(
        TEST_DATABASE_URL,
        echo=False,
        future=True,
    )
    
    # Create all tables
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    # Create session factory
    TestSessionLocal = async_sessionmaker(
        test_engine, class_=AsyncSession, expire_on_commit=False
    )
    
    # Create session
    async with TestSessionLocal() as session:
        yield session
        await session.rollback()
    
    # Cleanup
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await test_engine.dispose()


@pytest.fixture(scope="function")
async def client() -> AsyncGenerator[AsyncClient, None]:
    """
    Create a test HTTP client for making API requests.
    Uses the actual FastAPI app.
    """
    async with AsyncClient(app=app, base_url="http://test") as ac:
        yield ac


@pytest.fixture(scope="function")
async def db_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Get a database session from the actual database (not test DB).
    Use this for tests that need to query the real database.
    """
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()


@pytest.fixture(scope="function")
def mock_fec_api_key(monkeypatch):
    """Mock FEC API key for tests"""
    monkeypatch.setenv("FEC_API_KEY", "TEST_API_KEY_12345")
    yield "TEST_API_KEY_12345"


@pytest.fixture(scope="function")
def sample_candidate_id() -> str:
    """Return a sample candidate ID for testing"""
    return "P00003392"  # Real candidate ID (example)


@pytest.fixture(scope="function")
def sample_committee_id() -> str:
    """Return a sample committee ID for testing"""
    return "C00000042"  # Real committee ID (example)


@pytest.fixture(scope="function")
def sample_cycle() -> int:
    """Return a sample election cycle for testing"""
    return 2024


@pytest.fixture(scope="function")
async def sample_contribution_data(test_db: AsyncSession):
    """Create sample contribution data in test database"""
    contribution = Contribution(
        contribution_id="TEST_CONTRIB_001",
        candidate_id="P00003392",
        committee_id="C00000042",
        contributor_name="John Doe",
        contributor_city="Washington",
        contributor_state="DC",
        contributor_zip="20001",
        contributor_employer="Test Corp",
        contributor_occupation="Engineer",
        contribution_amount=1000.0,
        contribution_date=datetime(2024, 1, 15),
        contribution_type="24K",
        raw_data={"test": "data"}
    )
    test_db.add(contribution)
    await test_db.commit()
    return contribution


@pytest.fixture(scope="function")
async def sample_candidate_data(test_db: AsyncSession):
    """Create sample candidate data in test database"""
    candidate = Candidate(
        candidate_id="P00003392",
        name="Test Candidate",
        office="P",
        party="DEM",
        state="DC",
        district=None,
        election_years=[2024],
        active_through=2024,
        raw_data={"test": "data"}
    )
    test_db.add(candidate)
    await test_db.commit()
    return candidate


@pytest.fixture(scope="function")
async def sample_committee_data(test_db: AsyncSession):
    """Create sample committee data in test database"""
    committee = Committee(
        committee_id="C00000042",
        name="Test Committee",
        committee_type="H",
        committee_type_full="House",
        candidate_ids=["P00003392"],
        party="DEM",
        state="DC",
        raw_data={"test": "data"}
    )
    test_db.add(committee)
    await test_db.commit()
    return committee


@pytest.fixture(scope="function")
def api_base_url() -> str:
    """Base URL for API requests"""
    return "http://test"


# Helper function to check if database has data
async def has_database_data(session: AsyncSession, table_class, **filters) -> bool:
    """Check if database has data matching filters"""
    from sqlalchemy import select
    query = select(table_class)
    for key, value in filters.items():
        query = query.where(getattr(table_class, key) == value)
    result = await session.execute(query)
    return result.scalar_one_or_none() is not None


# Helper function to get database count
async def get_database_count(session: AsyncSession, table_class, **filters) -> int:
    """Get count of records matching filters"""
    from sqlalchemy import select, func
    query = select(func.count()).select_from(table_class)
    for key, value in filters.items():
        query = query.where(getattr(table_class, key) == value)
    result = await session.execute(query)
    return result.scalar_one()


@pytest.fixture(autouse=True)
async def cleanup_test_data():
    """Cleanup test data after each test"""
    yield
    # Any cleanup logic here if needed

