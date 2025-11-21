"""
Alembic environment configuration for database migrations

This module configures Alembic to work with async SQLAlchemy and our application models.
It uses the DATABASE_URL from app.config and supports both SQLite and PostgreSQL.

Note: For autogenerate to work, we need to convert async URLs to sync URLs.
"""
from logging.config import fileConfig
import asyncio
import os
from pathlib import Path

from sqlalchemy import pool, create_engine
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from alembic import context

# Import Base and all models for autogenerate support
from app.db.database import Base
from app.db.database import (
    APICache,
    Contribution,
    Candidate,
    Committee,
    BulkDataMetadata,
    BulkImportJob,
    OperatingExpenditure,
    IndependentExpenditure,
    ContributionLimit,
    AvailableCycle,
    CandidateSummary,
    FinancialTotal,
    ApiKeySetting,
    PreComputedAnalysis,
    AnalysisComputationJob
)

# Import config to get DATABASE_URL
from app.config import config

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
alembic_config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if alembic_config.config_file_name is not None:
    fileConfig(alembic_config.config_file_name)

# Set target_metadata for autogenerate support
target_metadata = Base.metadata

# Get database URL and convert async URL to sync for autogenerate
# Alembic's autogenerate needs a synchronous connection
database_url = config.DATABASE_URL
# Convert async URLs to sync for autogenerate
sync_url = database_url.replace("sqlite+aiosqlite://", "sqlite://")
sync_url = sync_url.replace("postgresql+asyncpg://", "postgresql://")
alembic_config.set_main_option("sqlalchemy.url", sync_url)

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = alembic_config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    For autogenerate, we use a synchronous engine.
    For actual migrations, we can use async if needed, but Alembic works
    fine with sync engines for both operations.

    """
    # Use sync engine for compatibility with Alembic autogenerate
    # The sync URL was already set in the config above
    connectable = create_engine(
        sync_url,
        poolclass=pool.NullPool,
        echo=False,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            compare_server_default=True,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
