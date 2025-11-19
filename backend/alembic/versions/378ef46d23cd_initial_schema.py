"""Initial schema

Revision ID: 378ef46d23cd
Revises: 
Create Date: 2025-11-19 10:43:37.923397

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '378ef46d23cd'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema.
    
    This is a baseline migration representing the current database state.
    Since the database already has all these schema elements, this migration
    is effectively a no-op. It exists to establish a baseline for future migrations.
    
    Note: SQLite has limited ALTER TABLE support, so type changes are not included.
    The existing schema is already correct.
    """
    # This migration represents the current state of the database.
    # All schema elements already exist, so this is a no-op baseline migration.
    # Future migrations will build on this baseline.
    pass


def downgrade() -> None:
    """Downgrade schema.
    
    This is a baseline migration, so downgrade is also a no-op.
    """
    # This is a baseline migration - no downgrade needed
    pass
