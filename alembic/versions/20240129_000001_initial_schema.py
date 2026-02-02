"""Initial database schema

Revision ID: 20240129_000001
Revises: 
Create Date: 2024-01-29

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '20240129_000001'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create workflow_definitions table
    op.create_table(
        'workflow_definitions',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('name', sa.String(255), nullable=False, unique=True),
        sa.Column('dag_definition', postgresql.JSONB(), nullable=False),
        sa.Column('default_retry_config', postgresql.JSONB(), nullable=False, default={}),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_workflow_definitions_name', 'workflow_definitions', ['name'], unique=True)

    # Create workflow_executions table
    op.create_table(
        'workflow_executions',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('workflow_definition_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('state', sa.String(50), nullable=False, default='PENDING'),
        sa.Column('input_params', postgresql.JSONB(), nullable=False, default={}),
        sa.Column('output_data', postgresql.JSONB(), nullable=True),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('started_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('completed_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('last_checkpoint_at', sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.ForeignKeyConstraint(['workflow_definition_id'], ['workflow_definitions.id'], ondelete='RESTRICT'),
    )
    op.create_index('ix_workflow_executions_state', 'workflow_executions', ['state'])
    op.create_index('ix_workflow_executions_state_created', 'workflow_executions', ['state', 'created_at'])
    op.create_index('ix_workflow_executions_created_at', 'workflow_executions', ['created_at'])
    op.create_index('ix_workflow_executions_definition_id', 'workflow_executions', ['workflow_definition_id'])

    # Create node_executions table
    op.create_table(
        'node_executions',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('workflow_execution_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('node_id', sa.String(255), nullable=False),
        sa.Column('state', sa.String(50), nullable=False, default='PENDING'),
        sa.Column('attempt', sa.Integer(), nullable=False, default=1),
        sa.Column('worker_id', sa.String(255), nullable=True),
        sa.Column('input_data', postgresql.JSONB(), nullable=False, default={}),
        sa.Column('output_data', postgresql.JSONB(), nullable=True),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('error_type', sa.String(255), nullable=True),
        sa.Column('error_stack_trace', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('started_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('completed_at', sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.ForeignKeyConstraint(['workflow_execution_id'], ['workflow_executions.id'], ondelete='CASCADE'),
        sa.UniqueConstraint('workflow_execution_id', 'node_id', name='uq_node_execution_workflow_node'),
    )
    op.create_index('ix_node_executions_state', 'node_executions', ['state'])
    op.create_index('ix_node_executions_workflow_node', 'node_executions', ['workflow_execution_id', 'node_id'])


def downgrade() -> None:
    op.drop_table('node_executions')
    op.drop_table('workflow_executions')
    op.drop_table('workflow_definitions')
