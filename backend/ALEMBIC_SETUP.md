# Alembic Migration Setup Guide

This document describes how to set up and use Alembic for database migrations in the FEC Query application.

## Current Migration System

The application currently uses a custom migration system that runs Python scripts via subprocess calls. These migrations are located in the `migrations/` directory and are executed during database initialization in `app/db/database.py`.

## Setting Up Alembic

### Step 1: Install Alembic

Add Alembic to `requirements.txt`:

```bash
alembic>=1.13.0
```

Then install:

```bash
pip install alembic
```

### Step 2: Initialize Alembic

Run the following command in the `backend/` directory:

```bash
alembic init alembic
```

This creates:
- `alembic/` directory with migration scripts
- `alembic.ini` configuration file

### Step 3: Configure Alembic

Edit `alembic.ini` to set the database URL:

```ini
sqlalchemy.url = sqlite+aiosqlite:///./fec_data.db
```

Or use environment variable:

```ini
sqlalchemy.url = ${DATABASE_URL}
```

Edit `alembic/env.py` to:
1. Import your Base and models
2. Set target_metadata to Base.metadata
3. Configure async engine support

Example `alembic/env.py`:

```python
from logging.config import fileConfig
from sqlalchemy import engine_from_config
from sqlalchemy import pool
from alembic import context
import asyncio
from sqlalchemy.ext.asyncio import AsyncEngine

# Import your Base and models
from app.db.database import Base
from app.db.database import (
    APICache, Contribution, Candidate, Committee,
    BulkDataMetadata, BulkImportJob, OperatingExpenditure,
    IndependentExpenditure, ContributionLimit, AvailableCycle
)

# this is the Alembic Config object
config = context.config

# Interpret the config file for Python logging
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Set target_metadata for autogenerate
target_metadata = Base.metadata

def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode with async support."""
    connectable = AsyncEngine(
        engine_from_config(
            config.get_section(config.config_ini_section, {}),
            prefix="sqlalchemy.",
            poolclass=pool.NullPool,
        )
    )

    async def do_run_migrations(connection):
        context.configure(
            connection=connection, target_metadata=target_metadata
        )
        with context.begin_transaction():
            context.run_migrations()

    asyncio.run(do_run_migrations(connectable.sync_engine))

if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
```

### Step 4: Create Initial Migration

Create an initial migration that captures the current database state:

```bash
alembic revision --autogenerate -m "Initial schema"
```

Review the generated migration in `alembic/versions/` before applying.

### Step 5: Apply Migrations

```bash
alembic upgrade head
```

## Migrating Existing Migrations

To migrate from the current system to Alembic:

1. **Create Alembic migrations for existing schema changes:**
   - Review existing migrations in `migrations/`
   - Create equivalent Alembic migrations
   - Test on a copy of the database

2. **Update `app/db/database.py`:**
   - Remove subprocess-based migration execution (lines 523-552)
   - Add Alembic migration execution:
   ```python
   from alembic.config import Config
   from alembic import command
   
   async def init_db():
       # ... existing initialization code ...
       
       # Run Alembic migrations
       alembic_cfg = Config("alembic.ini")
       command.upgrade(alembic_cfg, "head")
   ```

3. **Test thoroughly:**
   - Test on development database
   - Verify all existing migrations are captured
   - Test rollback functionality

## Using Alembic

### Creating a New Migration

```bash
# Auto-generate from model changes
alembic revision --autogenerate -m "Description of changes"

# Create empty migration for manual changes
alembic revision -m "Description of changes"
```

### Applying Migrations

```bash
# Apply all pending migrations
alembic upgrade head

# Apply specific migration
alembic upgrade <revision>

# Rollback one migration
alembic downgrade -1

# Rollback to specific revision
alembic downgrade <revision>
```

### Viewing Migration History

```bash
# Show current revision
alembic current

# Show migration history
alembic history

# Show pending migrations
alembic heads
```

## Best Practices

1. **Always review auto-generated migrations** before applying
2. **Test migrations on a copy** of production data
3. **Keep migrations small and focused** - one logical change per migration
4. **Never edit applied migrations** - create new migrations instead
5. **Document complex migrations** with comments in the migration file
6. **Use transactions** for data migrations when possible

## Troubleshooting

### Migration Conflicts

If you have conflicts between branches:

```bash
# Merge heads
alembic merge -m "Merge branch migrations" <revision1> <revision2>
```

### Database Out of Sync

If the database schema doesn't match migrations:

```bash
# Stamp database with current revision (use with caution)
alembic stamp head
```

### SQLite-Specific Issues

SQLite has limitations with ALTER TABLE. For complex changes:
1. Create new table with new schema
2. Copy data
3. Drop old table
4. Rename new table

See Alembic batch operations for SQLite.

## References

- [Alembic Documentation](https://alembic.sqlalchemy.org/)
- [SQLAlchemy Migrations Guide](https://docs.sqlalchemy.org/en/20/core/metadata.html)

