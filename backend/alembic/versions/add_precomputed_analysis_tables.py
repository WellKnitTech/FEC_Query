"""add precomputed analysis tables

Revision ID: add_precomputed_analysis
Revises: 378ef46d23cd
Create Date: 2025-01-20 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import sqlite


# revision identifiers, used by Alembic.
revision: str = 'add_precomputed_analysis'
down_revision: Union[str, None] = '378ef46d23cd'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add precomputed_analyses and analysis_computation_jobs tables"""
    
    # Create precomputed_analyses table
    op.create_table(
        'precomputed_analyses',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('analysis_type', sa.String(), nullable=False),
        sa.Column('candidate_id', sa.String(), nullable=True),
        sa.Column('committee_id', sa.String(), nullable=True),
        sa.Column('cycle', sa.Integer(), nullable=True),
        sa.Column('result_data', sa.JSON(), nullable=False),
        sa.Column('computed_at', sa.DateTime(), nullable=True),
        sa.Column('last_updated', sa.DateTime(), nullable=True),
        sa.Column('data_version', sa.Integer(), nullable=True, server_default='1'),
        sa.PrimaryKeyConstraint('id')
    )
    
    # Create indexes for precomputed_analyses (with error handling for existing indexes)
    try:
        op.create_index('idx_analysis_type_candidate_cycle', 'precomputed_analyses', ['analysis_type', 'candidate_id', 'cycle'])
    except Exception:
        pass
    try:
        op.create_index('idx_analysis_type_cycle', 'precomputed_analyses', ['analysis_type', 'cycle'])
    except Exception:
        pass
    try:
        op.create_index('idx_analysis_type_candidate', 'precomputed_analyses', ['analysis_type', 'candidate_id'])
    except Exception:
        pass
    try:
        op.create_index('idx_analysis_type_committee', 'precomputed_analyses', ['analysis_type', 'committee_id'])
    except Exception:
        pass
    try:
        op.create_index(op.f('ix_precomputed_analyses_analysis_type'), 'precomputed_analyses', ['analysis_type'])
    except Exception:
        pass
    try:
        op.create_index(op.f('ix_precomputed_analyses_candidate_id'), 'precomputed_analyses', ['candidate_id'])
    except Exception:
        pass
    try:
        op.create_index(op.f('ix_precomputed_analyses_committee_id'), 'precomputed_analyses', ['committee_id'])
    except Exception:
        pass
    try:
        op.create_index(op.f('ix_precomputed_analyses_computed_at'), 'precomputed_analyses', ['computed_at'])
    except Exception:
        pass
    try:
        op.create_index(op.f('ix_precomputed_analyses_cycle'), 'precomputed_analyses', ['cycle'])
    except Exception:
        pass
    try:
        op.create_index(op.f('ix_precomputed_analyses_id'), 'precomputed_analyses', ['id'])
    except Exception:
        pass
    
    # Create analysis_computation_jobs table
    op.create_table(
        'analysis_computation_jobs',
        sa.Column('id', sa.String(), nullable=False),
        sa.Column('job_type', sa.String(), nullable=True),
        sa.Column('status', sa.String(), nullable=True),
        sa.Column('analysis_type', sa.String(), nullable=True),
        sa.Column('candidate_id', sa.String(), nullable=True),
        sa.Column('committee_id', sa.String(), nullable=True),
        sa.Column('cycle', sa.Integer(), nullable=True),
        sa.Column('total_items', sa.Integer(), nullable=True, server_default='0'),
        sa.Column('completed_items', sa.Integer(), nullable=True, server_default='0'),
        sa.Column('current_item', sa.String(), nullable=True),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('started_at', sa.DateTime(), nullable=True),
        sa.Column('completed_at', sa.DateTime(), nullable=True),
        sa.Column('progress_data', sa.JSON(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
    
    # Create indexes for analysis_computation_jobs (with IF NOT EXISTS for SQLite)
    # SQLite doesn't support IF NOT EXISTS in CREATE INDEX, so we'll use try/except
    try:
        op.create_index('idx_status_started', 'analysis_computation_jobs', ['status', 'started_at'])
    except Exception:
        pass  # Index already exists
    try:
        op.create_index('idx_job_type_status', 'analysis_computation_jobs', ['job_type', 'status'])
    except Exception:
        pass
    try:
        op.create_index(op.f('ix_analysis_computation_jobs_analysis_type'), 'analysis_computation_jobs', ['analysis_type'])
    except Exception:
        pass
    try:
        op.create_index(op.f('ix_analysis_computation_jobs_candidate_id'), 'analysis_computation_jobs', ['candidate_id'])
    except Exception:
        pass
    try:
        op.create_index(op.f('ix_analysis_computation_jobs_committee_id'), 'analysis_computation_jobs', ['committee_id'])
    except Exception:
        pass
    try:
        op.create_index(op.f('ix_analysis_computation_jobs_cycle'), 'analysis_computation_jobs', ['cycle'])
    except Exception:
        pass
    try:
        op.create_index(op.f('ix_analysis_computation_jobs_id'), 'analysis_computation_jobs', ['id'])
    except Exception:
        pass
    try:
        op.create_index(op.f('ix_analysis_computation_jobs_job_type'), 'analysis_computation_jobs', ['job_type'])
    except Exception:
        pass
    try:
        op.create_index(op.f('ix_analysis_computation_jobs_started_at'), 'analysis_computation_jobs', ['started_at'])
    except Exception:
        pass
    try:
        op.create_index(op.f('ix_analysis_computation_jobs_status'), 'analysis_computation_jobs', ['status'])
    except Exception:
        pass


def downgrade() -> None:
    """Remove precomputed_analyses and analysis_computation_jobs tables"""
    
    # Drop indexes first
    op.drop_index(op.f('ix_analysis_computation_jobs_status'), table_name='analysis_computation_jobs')
    op.drop_index(op.f('ix_analysis_computation_jobs_started_at'), table_name='analysis_computation_jobs')
    op.drop_index(op.f('ix_analysis_computation_jobs_job_type'), table_name='analysis_computation_jobs')
    op.drop_index(op.f('ix_analysis_computation_jobs_id'), table_name='analysis_computation_jobs')
    op.drop_index(op.f('ix_analysis_computation_jobs_cycle'), table_name='analysis_computation_jobs')
    op.drop_index(op.f('ix_analysis_computation_jobs_committee_id'), table_name='analysis_computation_jobs')
    op.drop_index(op.f('ix_analysis_computation_jobs_candidate_id'), table_name='analysis_computation_jobs')
    op.drop_index(op.f('ix_analysis_computation_jobs_analysis_type'), table_name='analysis_computation_jobs')
    op.drop_index('idx_job_type_status', table_name='analysis_computation_jobs')
    op.drop_index('idx_status_started', table_name='analysis_computation_jobs')
    
    op.drop_table('analysis_computation_jobs')
    
    op.drop_index(op.f('ix_precomputed_analyses_id'), table_name='precomputed_analyses')
    op.drop_index(op.f('ix_precomputed_analyses_cycle'), table_name='precomputed_analyses')
    op.drop_index(op.f('ix_precomputed_analyses_computed_at'), table_name='precomputed_analyses')
    op.drop_index(op.f('ix_precomputed_analyses_committee_id'), table_name='precomputed_analyses')
    op.drop_index(op.f('ix_precomputed_analyses_candidate_id'), table_name='precomputed_analyses')
    op.drop_index(op.f('ix_precomputed_analyses_analysis_type'), table_name='precomputed_analyses')
    op.drop_index('idx_analysis_type_committee', table_name='precomputed_analyses')
    op.drop_index('idx_analysis_type_candidate', table_name='precomputed_analyses')
    op.drop_index('idx_analysis_type_cycle', table_name='precomputed_analyses')
    op.drop_index('idx_analysis_type_candidate_cycle', table_name='precomputed_analyses')
    
    op.drop_table('precomputed_analyses')

