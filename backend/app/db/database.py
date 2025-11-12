from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, String, Float, DateTime, Integer, Text, JSON, Index, text, UniqueConstraint
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

Base = declarative_base()


class APICache(Base):
    """Cache table for API responses"""
    __tablename__ = "api_cache"
    
    id = Column(Integer, primary_key=True, index=True)
    cache_key = Column(String, unique=True, index=True)
    response_data = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)
    expires_at = Column(DateTime)


class Contribution(Base):
    """Stored contribution data"""
    __tablename__ = "contributions"
    
    id = Column(Integer, primary_key=True, index=True)
    contribution_id = Column(String, unique=True, index=True)
    candidate_id = Column(String, index=True)
    committee_id = Column(String, index=True)
    contributor_name = Column(String, index=True)
    contributor_city = Column(String)
    contributor_state = Column(String, index=True)
    contributor_zip = Column(String)
    contributor_employer = Column(String)
    contributor_occupation = Column(String)
    contribution_amount = Column(Float)
    contribution_date = Column(DateTime)
    contribution_type = Column(String)
    raw_data = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_contributor_name', 'contributor_name'),
        Index('idx_contribution_date', 'contribution_date'),
        Index('idx_candidate_committee', 'candidate_id', 'committee_id'),
    )


class BulkDataMetadata(Base):
    """Metadata for bulk CSV downloads"""
    __tablename__ = "bulk_data_metadata"
    
    id = Column(Integer, primary_key=True, index=True)
    cycle = Column(Integer, index=True)
    data_type = Column(String, index=True)  # "schedule_a", etc.
    download_date = Column(DateTime, default=datetime.utcnow)
    file_path = Column(String)
    record_count = Column(Integer, default=0)
    last_updated = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_cycle_data_type', 'cycle', 'data_type'),
    )


class Candidate(Base):
    """Stored candidate data"""
    __tablename__ = "candidates"
    
    id = Column(Integer, primary_key=True, index=True)
    candidate_id = Column(String, unique=True, index=True)
    name = Column(String, index=True)
    office = Column(String, index=True)
    party = Column(String)
    state = Column(String, index=True)
    district = Column(String)
    election_years = Column(JSON)  # List of years
    active_through = Column(Integer)
    raw_data = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_office_state', 'office', 'state'),
        Index('idx_state_district', 'state', 'district'),
    )


class Committee(Base):
    """Stored committee data"""
    __tablename__ = "committees"
    
    id = Column(Integer, primary_key=True, index=True)
    committee_id = Column(String, unique=True, index=True)
    name = Column(String, index=True)
    committee_type = Column(String)
    committee_type_full = Column(String)
    candidate_ids = Column(JSON)  # List of candidate IDs
    party = Column(String)
    state = Column(String)
    raw_data = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_committee_type', 'committee_type'),
        Index('idx_name', 'name'),
    )


class FinancialTotal(Base):
    """Stored financial totals for candidates"""
    __tablename__ = "financial_totals"
    
    id = Column(Integer, primary_key=True, index=True)
    candidate_id = Column(String, index=True)
    cycle = Column(Integer, index=True)
    total_receipts = Column(Float, default=0.0)
    total_disbursements = Column(Float, default=0.0)
    cash_on_hand = Column(Float, default=0.0)
    total_contributions = Column(Float, default=0.0)
    individual_contributions = Column(Float, default=0.0)
    pac_contributions = Column(Float, default=0.0)
    party_contributions = Column(Float, default=0.0)
    raw_data = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_candidate_cycle', 'candidate_id', 'cycle', unique=True),
    )


class BulkImportJob(Base):
    """Track bulk import job progress"""
    __tablename__ = "bulk_import_jobs"
    
    id = Column(String, primary_key=True, index=True)  # UUID
    job_type = Column(String, index=True)  # 'single_cycle', 'all_cycles', 'cleanup_reimport'
    status = Column(String, index=True)  # 'pending', 'running', 'completed', 'failed', 'cancelled'
    cycle = Column(Integer, nullable=True, index=True)
    cycles = Column(JSON, nullable=True)  # For multi-cycle jobs
    total_cycles = Column(Integer, default=0)
    completed_cycles = Column(Integer, default=0)
    current_cycle = Column(Integer, nullable=True)
    total_records = Column(Integer, default=0)
    imported_records = Column(Integer, default=0)
    skipped_records = Column(Integer, default=0)
    current_chunk = Column(Integer, default=0)
    total_chunks = Column(Integer, default=0)
    error_message = Column(Text, nullable=True)
    started_at = Column(DateTime, default=datetime.utcnow, index=True)
    completed_at = Column(DateTime, nullable=True)
    progress_data = Column(JSON)  # Detailed progress info
    
    __table_args__ = (
        Index('idx_status_started', 'status', 'started_at'),
    )


class IndependentExpenditure(Base):
    """Stored independent expenditure data"""
    __tablename__ = "independent_expenditures"
    
    id = Column(Integer, primary_key=True, index=True)
    expenditure_id = Column(String, unique=True, index=True)
    cycle = Column(Integer, index=True)
    committee_id = Column(String, index=True)
    candidate_id = Column(String, index=True)
    candidate_name = Column(String)
    support_oppose_indicator = Column(String)  # 'S' for support, 'O' for oppose
    expenditure_amount = Column(Float)
    expenditure_date = Column(DateTime)
    payee_name = Column(String)
    expenditure_purpose = Column(Text)
    raw_data = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    data_age_days = Column(Integer, default=0)  # Days since data was current
    
    __table_args__ = (
        Index('idx_indep_exp_cycle_committee', 'cycle', 'committee_id'),
        Index('idx_indep_exp_cycle_candidate', 'cycle', 'candidate_id'),
        Index('idx_indep_exp_date', 'expenditure_date'),
    )


class OperatingExpenditure(Base):
    """Stored operating expenditure data"""
    __tablename__ = "operating_expenditures"
    
    id = Column(Integer, primary_key=True, index=True)
    expenditure_id = Column(String, unique=True, index=True)
    cycle = Column(Integer, index=True)
    committee_id = Column(String, index=True)
    payee_name = Column(String, index=True)
    expenditure_amount = Column(Float)
    expenditure_date = Column(DateTime)
    expenditure_purpose = Column(Text)
    raw_data = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    data_age_days = Column(Integer, default=0)  # Days since data was current
    
    __table_args__ = (
        Index('idx_op_exp_cycle_committee', 'cycle', 'committee_id'),
        Index('idx_op_exp_date', 'expenditure_date'),
    )


class CandidateSummary(Base):
    """Stored candidate summary data from bulk files"""
    __tablename__ = "candidate_summaries"
    
    id = Column(Integer, primary_key=True, index=True)
    candidate_id = Column(String, index=True)
    cycle = Column(Integer, index=True)
    candidate_name = Column(String)
    office = Column(String)
    party = Column(String)
    state = Column(String)
    district = Column(String)
    total_receipts = Column(Float, default=0.0)
    total_disbursements = Column(Float, default=0.0)
    cash_on_hand = Column(Float, default=0.0)
    raw_data = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    data_age_days = Column(Integer, default=0)  # Days since data was current
    
    __table_args__ = (
        Index('idx_cand_summary_candidate_cycle', 'candidate_id', 'cycle', unique=True),
        Index('idx_cand_summary_office_state', 'office', 'state'),
    )


class CommitteeSummary(Base):
    """Stored committee summary data from bulk files"""
    __tablename__ = "committee_summaries"
    
    id = Column(Integer, primary_key=True, index=True)
    committee_id = Column(String, index=True)
    cycle = Column(Integer, index=True)
    committee_name = Column(String)
    committee_type = Column(String)
    total_receipts = Column(Float, default=0.0)
    total_disbursements = Column(Float, default=0.0)
    cash_on_hand = Column(Float, default=0.0)
    raw_data = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    data_age_days = Column(Integer, default=0)  # Days since data was current
    
    __table_args__ = (
        Index('idx_committee_cycle', 'committee_id', 'cycle', unique=True),
    )


class BulkDataImportStatus(Base):
    """Track import status per data type per cycle"""
    __tablename__ = "bulk_data_import_status"
    
    id = Column(Integer, primary_key=True, index=True)
    data_type = Column(String, index=True)  # DataType enum value
    cycle = Column(Integer, index=True)
    status = Column(String, index=True)  # 'imported', 'not_imported', 'failed', 'in_progress'
    record_count = Column(Integer, default=0)
    last_imported_at = Column(DateTime, nullable=True)
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    __table_args__ = (
        UniqueConstraint('data_type', 'cycle', name='uq_data_type_cycle'),
        Index('idx_data_type_cycle', 'data_type', 'cycle'),
        Index('idx_status', 'status'),
    )


class ElectioneeringComm(Base):
    """Electioneering communications data"""
    __tablename__ = "electioneering_comm"
    
    id = Column(Integer, primary_key=True, index=True)
    cycle = Column(Integer, index=True)
    committee_id = Column(String, index=True)
    candidate_id = Column(String, index=True)
    candidate_name = Column(String)
    communication_date = Column(DateTime)
    communication_amount = Column(Float)
    raw_data = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    data_age_days = Column(Integer, default=0)
    
    __table_args__ = (
        Index('idx_electioneering_cycle_committee', 'cycle', 'committee_id'),
        Index('idx_electioneering_date', 'communication_date'),
    )


class CommunicationCost(Base):
    """Communication costs data"""
    __tablename__ = "communication_costs"
    
    id = Column(Integer, primary_key=True, index=True)
    cycle = Column(Integer, index=True)
    committee_id = Column(String, index=True)
    candidate_id = Column(String, index=True)
    candidate_name = Column(String)
    communication_date = Column(DateTime)
    communication_amount = Column(Float)
    raw_data = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    data_age_days = Column(Integer, default=0)
    
    __table_args__ = (
        Index('idx_comm_cost_cycle_committee', 'cycle', 'committee_id'),
        Index('idx_comm_cost_date', 'communication_date'),
    )


# Database setup
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./fec_data.db")
if DATABASE_URL.startswith("sqlite"):
    # For SQLite, we need to use aiosqlite
    # Configure connection pool and timeout settings for better concurrency handling
    # Note: aiosqlite uses different connection args than sqlite3
    engine = create_async_engine(
        DATABASE_URL.replace("sqlite://", "sqlite+aiosqlite://"),
        echo=False,
        future=True,
        pool_pre_ping=True,  # Verify connections before using
        pool_size=5,  # Limit concurrent connections
        max_overflow=10,  # Allow overflow connections
        connect_args={
            "timeout": 30.0,  # 30 second timeout for database operations
        }
    )
else:
    engine = create_async_engine(DATABASE_URL, echo=False, future=True)

AsyncSessionLocal = async_sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)


async def get_db():
    """Dependency for getting database session"""
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()


async def init_db():
    """Initialize database tables"""
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        async with engine.begin() as conn:
            # Enable WAL mode for better concurrency (allows readers and writers simultaneously)
            if DATABASE_URL.startswith("sqlite"):
                try:
                    await conn.execute(text("PRAGMA journal_mode=WAL"))
                    logger.info("SQLite WAL mode enabled for better concurrency")
                except Exception as e:
                    logger.warning(f"Could not enable WAL mode: {e}")
            
            await conn.run_sync(Base.metadata.create_all)
        logger.info("Database tables initialized successfully")
    except Exception as e:
        # If indexes already exist, try to drop the conflicting indexes and recreate
        if "already exists" in str(e) or "duplicate" in str(e).lower():
            logger.warning(f"Index conflict detected: {e}")
            logger.info("Attempting to fix index conflicts...")
            
            # Extract the conflicting index name from the error
            import re
            index_match = re.search(r"index (\w+) already exists", str(e), re.IGNORECASE)
            if index_match:
                conflicting_index = index_match.group(1)
                logger.info(f"Dropping conflicting index: {conflicting_index}")
                try:
                    async with engine.begin() as conn:
                        # Drop the specific conflicting index
                        await conn.execute(text(f"DROP INDEX IF EXISTS {conflicting_index}"))
                        # Now try creating all again
                        await conn.run_sync(Base.metadata.create_all)
                    logger.info("Index conflicts resolved, schema created successfully")
                except Exception as e2:
                    logger.error(f"Failed to resolve index conflict: {e2}")
                    # If that fails, try dropping all indexes and recreating
                    logger.warning("Attempting to drop all indexes and recreate...")
                    try:
                        async with engine.begin() as conn:
                            # Get all index names
                            result = await conn.execute(text(
                                "SELECT name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%'"
                            ))
                            indexes = [row[0] for row in result.fetchall()]
                            for idx_name in indexes:
                                try:
                                    await conn.execute(text(f"DROP INDEX IF EXISTS {idx_name}"))
                                except:
                                    pass
                            # Now recreate
                            await conn.run_sync(Base.metadata.create_all)
                        logger.info("All indexes recreated successfully")
                    except Exception as e3:
                        logger.error(f"Failed to recreate indexes: {e3}")
                        raise
            else:
                logger.error(f"Could not identify conflicting index from error: {e}")
                raise
        else:
            logger.error(f"Database initialization failed: {e}")
            raise

