"""
Database models and connection management

This module defines SQLAlchemy models for all database tables and provides
database connection management, initialization, and migration support.

Models:
- APICache: Cache for FEC API responses
- Contribution: Individual contribution records
- Candidate: Candidate information
- Committee: Committee information
- BulkDataMetadata: Metadata for bulk data imports
- BulkImportJob: Tracks bulk import progress
- OperatingExpenditure: Operating expenditure records
- IndependentExpenditure: Independent expenditure records
- ContributionLimit: Historical contribution limits
- AvailableCycle: Available election cycles

The module also provides:
- Database engine and session management
- Connection pooling configuration
- WAL mode configuration for SQLite
- Database initialization and migration execution

Example:
    ```python
    from app.db.database import AsyncSessionLocal, Contribution
    
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Contribution).where(
                Contribution.candidate_id == "P00003392"
            )
        )
        contributions = result.scalars().all()
    ```
"""
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, String, Float, DateTime, Integer, Text, JSON, Index, text, UniqueConstraint, Boolean
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
    
    __table_args__ = (
        Index('idx_cache_key_expires', 'cache_key', 'expires_at'),
    )


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
    # Additional FEC fields from Schedule A
    amendment_indicator = Column(String)  # AMNDT_IND
    report_type = Column(String, index=True)  # RPT_TP
    transaction_id = Column(String, index=True)  # TRAN_ID
    entity_type = Column(String, index=True)  # ENTITY_TP
    other_id = Column(String)  # OTHER_ID
    file_number = Column(String)  # FILE_NUM
    memo_code = Column(String)  # MEMO_CD
    memo_text = Column(Text)  # MEMO_TEXT
    raw_data = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)
    # Data source tracking (optional, for debugging and data provenance)
    data_source = Column(String)  # 'bulk', 'api', or 'both' - tracks which sources contributed data
    last_updated_from = Column(String)  # Tracks the last source that updated this record
    
    __table_args__ = (
        Index('idx_contributor_name', 'contributor_name'),
        Index('idx_contribution_date', 'contribution_date'),
        Index('idx_candidate_committee', 'candidate_id', 'committee_id'),
        Index('idx_report_type', 'report_type'),
        Index('idx_entity_type', 'entity_type'),
        Index('idx_transaction_id', 'transaction_id'),
    )


class BulkDataMetadata(Base):
    """Metadata for bulk CSV downloads"""
    __tablename__ = "bulk_data_metadata"
    
    id = Column(Integer, primary_key=True, index=True)
    cycle = Column(Integer, index=True)
    data_type = Column(String, index=True)  # "schedule_a", etc.
    download_date = Column(DateTime, default=datetime.utcnow)
    file_path = Column(String)
    file_size = Column(Integer, nullable=True)  # File size in bytes
    file_hash = Column(String, nullable=True, index=True)  # MD5 hash of file content
    imported = Column(Boolean, default=False, index=True)  # Whether file has been imported
    record_count = Column(Integer, default=0)
    last_updated = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_cycle_data_type', 'cycle', 'data_type'),
        Index('idx_file_hash', 'file_hash'),
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
    # Contact information fields
    street_address = Column(String)
    city = Column(String)
    zip = Column(String)
    email = Column(String)
    phone = Column(String)
    website = Column(String)
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
    # Contact information fields
    street_address = Column(String)
    street_address_2 = Column(String)
    city = Column(String)
    zip = Column(String)
    email = Column(String)
    phone = Column(String)
    website = Column(String)
    treasurer_name = Column(String)
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
    loan_contributions = Column(Float, default=0.0)
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
    file_position = Column(Integer, default=0)  # File position in bytes for resumable imports
    data_type = Column(String, nullable=True)  # Data type being imported (e.g., 'individual_contributions')
    file_path = Column(String, nullable=True)  # Path to file being imported
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
    # Additional FEC fields from oppexp files
    amendment_indicator = Column(String)  # AMNDT_IND
    report_year = Column(Integer)  # RPT_YR
    report_type = Column(String, index=True)  # RPT_TP
    image_number = Column(String)  # IMAGE_NUM
    line_number = Column(String)  # LINE_NUM
    form_type_code = Column(String)  # FORM_TP_CD
    schedule_type_code = Column(String)  # SCHED_TP_CD
    transaction_pgi = Column(String)  # TRANSACTION_PGI
    category = Column(String, index=True)  # CATEGORY
    category_description = Column(String)  # CATEGORY_DESC
    memo_code = Column(String)  # MEMO_CD
    memo_text = Column(Text)  # MEMO_TEXT
    entity_type = Column(String, index=True)  # ENTITY_TP
    file_number = Column(String)  # FILE_NUM
    transaction_id = Column(String, index=True)  # TRAN_ID
    back_reference_transaction_id = Column(String)  # BACK_REF_TRAN_ID
    raw_data = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    data_age_days = Column(Integer, default=0)  # Days since data was current
    
    __table_args__ = (
        Index('idx_op_exp_cycle_committee', 'cycle', 'committee_id'),
        Index('idx_op_exp_date', 'expenditure_date'),
        Index('idx_op_exp_report_type', 'report_type'),
        Index('idx_op_exp_category', 'category'),
        Index('idx_op_exp_entity_type', 'entity_type'),
        Index('idx_op_exp_transaction_id', 'transaction_id'),
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


class SavedSearch(Base):
    """Saved search queries"""
    __tablename__ = "saved_searches"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    search_type = Column(String, index=True)  # 'candidate', 'committee', 'contribution', etc.
    search_params = Column(JSON)  # Store search parameters as JSON
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_saved_search_type', 'search_type'),
        Index('idx_saved_search_created', 'created_at'),
    )


class ApiKeySetting(Base):
    """Stored API key configuration"""
    __tablename__ = "api_key_settings"
    
    id = Column(Integer, primary_key=True, index=True)
    api_key = Column(String, nullable=False)  # FEC keys are public, stored as plain text
    source = Column(String, default="ui")  # 'ui' or 'env'
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    is_active = Column(Integer, default=1)  # 1 = active, 0 = deleted (soft delete)
    
    __table_args__ = (
        Index('idx_api_key_active', 'is_active'),
    )


class ContributionLimit(Base):
    """FEC contribution limits by year, contributor category, and recipient category"""
    __tablename__ = "contribution_limits"
    
    id = Column(Integer, primary_key=True, index=True)
    effective_year = Column(Integer, nullable=False, index=True)  # Year limits take effect (Jan 1)
    contributor_category = Column(String, nullable=False, index=True)  # 'individual', 'multicandidate_pac', 'non_multicandidate_pac', 'party_committee', etc.
    recipient_category = Column(String, nullable=False, index=True)  # 'candidate', 'pac', 'party_committee', etc.
    limit_amount = Column(Float, nullable=False)  # Limit amount in dollars
    limit_type = Column(String, nullable=False)  # 'per_election', 'per_year', 'per_calendar_year'
    notes = Column(Text, nullable=True)  # Additional notes about the limit
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    __table_args__ = (
        UniqueConstraint('effective_year', 'contributor_category', 'recipient_category', 'limit_type', 
                        name='uq_contribution_limit'),
        Index('idx_contribution_limit_lookup', 'effective_year', 'contributor_category', 'recipient_category'),
    )


class AvailableCycle(Base):
    """Stored available cycles from FEC API"""
    __tablename__ = "available_cycles"
    
    id = Column(Integer, primary_key=True, index=True)
    cycle = Column(Integer, unique=True, nullable=False, index=True)  # Election cycle year
    last_updated = Column(DateTime, default=datetime.utcnow, index=True)  # When this cycle was last verified
    created_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_cycle', 'cycle'),
        Index('idx_last_updated', 'last_updated'),
    )


class PreComputedAnalysis(Base):
    """Stored pre-computed analysis results"""
    __tablename__ = "precomputed_analyses"
    
    id = Column(Integer, primary_key=True, index=True)
    analysis_type = Column(String, nullable=False, index=True)  # 'donor_states', 'employer', 'velocity'
    candidate_id = Column(String, nullable=True, index=True)  # For candidate-specific analysis
    committee_id = Column(String, nullable=True, index=True)  # For committee-specific analysis
    cycle = Column(Integer, nullable=True, index=True)  # For cycle-specific analysis
    result_data = Column(JSON, nullable=False)  # Stores the analysis result
    computed_at = Column(DateTime, default=datetime.utcnow, index=True)
    last_updated = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    data_version = Column(Integer, default=1)  # Tracks data changes for incremental updates
    
    __table_args__ = (
        Index('idx_analysis_type_candidate_cycle', 'analysis_type', 'candidate_id', 'cycle'),
        Index('idx_analysis_type_cycle', 'analysis_type', 'cycle'),
        Index('idx_analysis_type_candidate', 'analysis_type', 'candidate_id'),
        Index('idx_analysis_type_committee', 'analysis_type', 'committee_id'),
    )


class AnalysisComputationJob(Base):
    """Track analysis computation job progress"""
    __tablename__ = "analysis_computation_jobs"
    
    id = Column(String, primary_key=True, index=True)  # UUID
    job_type = Column(String, index=True)  # 'cycle', 'candidate', 'committee', 'batch'
    status = Column(String, index=True)  # 'pending', 'running', 'completed', 'failed', 'cancelled'
    analysis_type = Column(String, nullable=True, index=True)  # 'donor_states', 'employer', 'velocity', or None for all
    candidate_id = Column(String, nullable=True, index=True)
    committee_id = Column(String, nullable=True, index=True)
    cycle = Column(Integer, nullable=True, index=True)
    total_items = Column(Integer, default=0)  # Total items to process (candidates, cycles, etc.)
    completed_items = Column(Integer, default=0)
    current_item = Column(String, nullable=True)  # Current item being processed
    error_message = Column(Text, nullable=True)
    started_at = Column(DateTime, default=datetime.utcnow, index=True)
    completed_at = Column(DateTime, nullable=True)
    progress_data = Column(JSON)  # Detailed progress info
    
    __table_args__ = (
        Index('idx_status_started', 'status', 'started_at'),
        Index('idx_job_type_status', 'job_type', 'status'),
    )


# Database setup - use centralized config
from app.config import config

DATABASE_URL = config.DATABASE_URL

if DATABASE_URL.startswith("sqlite"):
    # For SQLite, we need to use aiosqlite
    # Configure connection pool and timeout settings for better concurrency handling
    # Note: aiosqlite uses different connection args than sqlite3
    # SQLite works better with smaller pools due to file-based locking
    # WAL mode allows concurrent readers, but writes still need coordination
    engine = create_async_engine(
        DATABASE_URL.replace("sqlite://", "sqlite+aiosqlite://"),
        echo=False,
        future=True,
        pool_pre_ping=True,  # Verify connections before using
        pool_size=config.SQLITE_POOL_SIZE,  # Smaller pool for SQLite (10 instead of 20)
        max_overflow=config.SQLITE_MAX_OVERFLOW,  # Reduced overflow for SQLite (10 instead of 30)
        pool_timeout=120.0,  # Timeout for getting connection from pool (2 minutes)
        pool_recycle=3600,  # Recycle connections after 1 hour to prevent stale connections
        connect_args={
            "timeout": 120.0,  # 120 second timeout for database operations
        }
    )
else:
    # PostgreSQL or other database - use larger pool settings
    engine = create_async_engine(
        DATABASE_URL,
        echo=False,
        future=True,
        pool_pre_ping=True,
        pool_size=config.POSTGRES_POOL_SIZE,
        max_overflow=config.POSTGRES_MAX_OVERFLOW,
        pool_timeout=120.0,
        pool_recycle=3600
    )

AsyncSessionLocal = async_sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)


async def get_db():
    """Dependency for getting database session"""
    async with AsyncSessionLocal() as session:
        try:
            yield session
        except Exception as e:
            await session.rollback()
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Database session error: {e}", exc_info=True)
            raise
        finally:
            await session.close()


async def init_db():
    """Initialize database tables and run Alembic migrations"""
    import logging
    logger = logging.getLogger(__name__)
    logger.info("Starting database initialization...")
    
    # Run Alembic migrations
    logger.info("Running Alembic migrations...")
    try:
        from alembic.config import Config
        from alembic import command
        from pathlib import Path
        
        # Get path to alembic.ini (should be in backend/ directory)
        backend_dir = Path(__file__).parent.parent.parent
        alembic_ini_path = backend_dir / "alembic.ini"
        
        if alembic_ini_path.exists():
            alembic_cfg = Config(str(alembic_ini_path))
            # Run migrations to head
            command.upgrade(alembic_cfg, "head")
            logger.info("Alembic migrations completed successfully")
        else:
            logger.warning(f"Alembic config not found at {alembic_ini_path}. Skipping migrations.")
    except Exception as e:
        logger.error(f"Error running Alembic migrations: {e}", exc_info=True)
        # Don't fail initialization if migrations fail - database might already be up to date
        logger.warning("Continuing with database initialization despite migration error")
    
    try:
        logger.info("Opening database connection...")
        async with engine.begin() as conn:
            # Enable WAL mode for better concurrency (allows readers and writers simultaneously)
            if DATABASE_URL.startswith("sqlite"):
                try:
                    # Check if integrity check should be skipped (for large databases or via env var)
                    skip_integrity_check = os.getenv("SKIP_DB_INTEGRITY_CHECK", "false").lower() == "true"
                    
                    # Check database file size - skip integrity check for databases > 1GB
                    # Parse database file path from URL
                    db_file = DATABASE_URL.replace("sqlite:///", "").replace("sqlite+aiosqlite:///", "")
                    # Handle relative paths - convert to absolute path
                    if not os.path.isabs(db_file):
                        # Get the backend directory (parent of app directory)
                        backend_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                        db_file = os.path.join(backend_dir, db_file.lstrip("./"))
                    
                    if os.path.exists(db_file):
                        db_size = os.path.getsize(db_file)
                        db_size_gb = db_size / (1024 * 1024 * 1024)
                        if db_size_gb > 1.0:
                            logger.info(f"Database is {db_size_gb:.2f}GB - skipping integrity check for faster startup")
                            logger.info("To run integrity check manually, use: PRAGMA quick_check or PRAGMA integrity_check")
                            skip_integrity_check = True
                    
                    if not skip_integrity_check:
                        # Check database integrity - use quick_check for large databases (much faster)
                        # Full integrity_check can take hours on large databases
                        logger.info("Performing quick database integrity check...")
                        try:
                            # Use quick_check instead of full integrity_check for speed
                            # quick_check is much faster but less thorough
                            result = await conn.execute(text("PRAGMA quick_check"))
                            integrity_result = result.fetchone()
                            if integrity_result and integrity_result[0] != "ok":
                                logger.warning(f"Database quick check found issues: {integrity_result[0]}")
                                logger.warning("Database may have issues. For full check, run: PRAGMA integrity_check")
                                # Don't fail startup, just warn - full integrity check can be run manually
                            else:
                                logger.debug("Database quick check passed")
                        except Exception as e:
                            if "disk I/O error" in str(e).lower() or "corrupted" in str(e).lower():
                                logger.error("Database corruption detected during integrity check!")
                                logger.error("Please see backend/migrations/REPAIR_INSTRUCTIONS.md for recovery steps")
                                raise RuntimeError("Database is corrupted and cannot be used") from e
                            # If it's a different error (e.g., database doesn't exist yet), continue
                            logger.debug(f"Integrity check skipped (database may not exist yet): {e}")
                    else:
                        logger.info("Skipping database integrity check (large database or SKIP_DB_INTEGRITY_CHECK=true)")
                    
                    logger.info("Configuring WAL mode...")
                    await conn.execute(text("PRAGMA journal_mode=WAL"))
                    # Set WAL checkpoint settings for better performance
                    await conn.execute(text("PRAGMA wal_autocheckpoint=500"))  # More frequent checkpoints
                    await conn.execute(text("PRAGMA synchronous=NORMAL"))  # Balance between safety and speed
                    await conn.execute(text("PRAGMA busy_timeout=60000"))  # 60 second timeout
                    logger.info("SQLite WAL mode enabled with optimized settings")
                except RuntimeError:
                    # Re-raise corruption errors
                    raise
                except Exception as e:
                    logger.warning(f"Could not configure WAL mode: {e}")
            
            logger.info("Creating database tables and indexes...")
            await conn.run_sync(Base.metadata.create_all)
            logger.info("Database tables initialized successfully")
        
        # Populate contribution limits after tables are created
        logger.info("Populating contribution limits...")
        try:
            from app.services.contribution_limits import ContributionLimitsService
            async with AsyncSessionLocal() as session:
                limits_service = ContributionLimitsService(session)
                count = await limits_service.populate_historical_limits()
                logger.info(f"Populated {count} contribution limits")
        except Exception as e:
            logger.warning(f"Could not populate contribution limits: {e}")
            # Don't fail startup if limits can't be populated
        
        logger.info("Database initialization complete")
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

