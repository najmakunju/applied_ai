"""
SQLAlchemy models for PostgreSQL persistence.

Implements durable storage for workflow definitions and executions.
"""

from datetime import datetime
from typing import Any, Optional
from uuid import UUID, uuid4

from sqlalchemy import (
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID as PGUUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Base class for all SQLAlchemy models."""
    
    type_annotation_map = {
        dict[str, Any]: JSONB,
    }


class WorkflowDefinitionModel(Base):
    """
    Stores workflow definitions.
    
    Each workflow definition has a unique name. To update a workflow,
    delete the existing one and create a new definition.
    """
    
    __tablename__ = "workflow_definitions"
    
    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True, index=True)
    
    # DAG definition stored as JSON
    dag_definition: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    
    # Default configurations
    default_retry_config: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    
    # Metadata
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now()
    )
    
    # Relationships
    executions: Mapped[list["WorkflowExecutionModel"]] = relationship(
        back_populates="workflow_definition",
        lazy="dynamic"
    )
    


class WorkflowExecutionModel(Base):
    """
    Stores workflow execution state.
    
    This is the primary record for tracking workflow execution.
    """
    
    __tablename__ = "workflow_executions"
    
    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    workflow_definition_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("workflow_definitions.id", ondelete="RESTRICT"),
        nullable=False,
        index=True
    )
    
    # State
    state: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        default="PENDING",
        index=True
    )
    
    # Input/Output
    input_params: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    output_data: Mapped[Optional[dict[str, Any]]] = mapped_column(JSONB, nullable=True)
    
    # Error tracking
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now()
    )
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    
    # Checkpoint for crash recovery
    last_checkpoint_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    
    # Relationships
    workflow_definition: Mapped[WorkflowDefinitionModel] = relationship(back_populates="executions")
    node_executions: Mapped[list["NodeExecutionModel"]] = relationship(
        back_populates="workflow_execution",
        lazy="selectin",
        cascade="all, delete-orphan"
    )
    
    __table_args__ = (
        Index("ix_workflow_executions_state_created", "state", "created_at"),
        Index("ix_workflow_executions_created_at", "created_at"),
    )


class NodeExecutionModel(Base):
    """
    Stores node execution state within a workflow.
    
    Each node in a workflow execution has its own record.
    """
    
    __tablename__ = "node_executions"
    
    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    workflow_execution_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("workflow_executions.id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )
    node_id: Mapped[str] = mapped_column(String(255), nullable=False)
    
    # State
    state: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        default="PENDING",
        index=True
    )
    
    # Execution tracking
    attempt: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    worker_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    
    # Input/Output
    input_data: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    output_data: Mapped[Optional[dict[str, Any]]] = mapped_column(JSONB, nullable=True)
    
    # Error tracking
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    error_type: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    error_stack_trace: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now()
    )
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    
    # Relationships
    workflow_execution: Mapped[WorkflowExecutionModel] = relationship(back_populates="node_executions")
    
    __table_args__ = (
        UniqueConstraint("workflow_execution_id", "node_id", name="uq_node_execution_workflow_node"),
        Index("ix_node_executions_state", "state"),
        Index("ix_node_executions_workflow_node", "workflow_execution_id", "node_id"),
    )
