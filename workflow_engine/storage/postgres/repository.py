"""
Repository layer for workflow data access.

Provides high-level data access methods with proper transaction handling.
"""

from datetime import datetime
from typing import Any, Optional
from uuid import UUID

from sqlalchemy import and_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from workflow_engine.core.models import (
    DAGDefinition,
    NodeExecution,
    RetryConfig,
    WorkflowDefinition,
    WorkflowExecution,
)
from workflow_engine.storage.postgres.models import (
    NodeExecutionModel,
    WorkflowDefinitionModel,
    WorkflowExecutionModel,
)


class WorkflowRepository:
    """
    Repository for workflow definitions and executions.
    
    All methods operate within the provided session's transaction.
    """
    
    def __init__(self, session: AsyncSession):
        self.session = session
    
    # ==================== Workflow Definition Operations ====================
    
    async def create_workflow_definition(
        self,
        definition: WorkflowDefinition,
    ) -> WorkflowDefinitionModel:
        """Create a new workflow definition."""
        model = WorkflowDefinitionModel(
            id=definition.id,
            name=definition.name,
            dag_definition=definition.dag.model_dump(),
            default_retry_config=definition.default_retry_config.model_dump(),
            description=definition.description,
        )
        
        self.session.add(model)
        await self.session.flush()
        return model
    
    async def get_workflow_definition(
        self,
        definition_id: UUID,
    ) -> Optional[WorkflowDefinitionModel]:
        """Get workflow definition by ID."""
        result = await self.session.execute(
            select(WorkflowDefinitionModel)
            .where(WorkflowDefinitionModel.id == definition_id)
        )
        return result.scalar_one_or_none()
    
    async def get_workflow_definition_by_name(
        self,
        name: str,
    ) -> Optional[WorkflowDefinitionModel]:
        """Get workflow definition by name."""
        result = await self.session.execute(
            select(WorkflowDefinitionModel).where(
                WorkflowDefinitionModel.name == name
            )
        )
        return result.scalars().first()
    
    # ==================== Workflow Execution Operations ====================
    
    async def create_workflow_execution(
        self,
        execution: WorkflowExecution,
    ) -> WorkflowExecutionModel:
        """Create a new workflow execution."""
        model = WorkflowExecutionModel(
            id=execution.id,
            workflow_definition_id=execution.workflow_definition_id,
            state=execution.state,
            input_params=execution.input_params,
        )
        
        self.session.add(model)
        await self.session.flush()
        return model
    
    async def get_workflow_execution(
        self,
        execution_id: UUID,
    ) -> Optional[WorkflowExecutionModel]:
        """Get workflow execution by ID."""
        result = await self.session.execute(
            select(WorkflowExecutionModel)
            .where(WorkflowExecutionModel.id == execution_id)
        )
        return result.scalar_one_or_none()
    
    async def update_workflow_execution_state(
        self,
        execution_id: UUID,
        new_state: str,
        error_message: Optional[str] = None,
        output_data: Optional[dict[str, Any]] = None,
    ) -> None:
        """Update workflow execution state."""
        values: dict[str, Any] = {"state": new_state}
        
        if new_state == "RUNNING" and not error_message:
            values["started_at"] = datetime.utcnow()
        elif new_state in ("COMPLETED", "FAILED", "CANCELLED"):
            values["completed_at"] = datetime.utcnow()
        
        if error_message:
            values["error_message"] = error_message
        if output_data:
            values["output_data"] = output_data
        
        await self.session.execute(
            update(WorkflowExecutionModel)
            .where(WorkflowExecutionModel.id == execution_id)
            .values(**values)
        )
    
    async def get_incomplete_workflow_executions(
        self,
        before: datetime,
    ) -> list[WorkflowExecutionModel]:
        """Get incomplete workflow executions for crash recovery."""
        result = await self.session.execute(
            select(WorkflowExecutionModel)
            .where(
                and_(
                    WorkflowExecutionModel.state.in_(["PENDING", "RUNNING", "PAUSED"]),
                    WorkflowExecutionModel.created_at < before,
                )
            )
            .order_by(WorkflowExecutionModel.created_at)
        )
        return list(result.scalars().all())
    
    async def checkpoint_workflow_execution(
        self,
        execution_id: UUID,
    ) -> None:
        """Update checkpoint timestamp for crash recovery."""
        await self.session.execute(
            update(WorkflowExecutionModel)
            .where(WorkflowExecutionModel.id == execution_id)
            .values(last_checkpoint_at=datetime.utcnow())
        )
    
    # ==================== Node Execution Operations ====================
    
    async def create_node_execution(
        self,
        node_execution: NodeExecution,
    ) -> NodeExecutionModel:
        """Create a new node execution."""
        model = NodeExecutionModel(
            id=node_execution.id,
            workflow_execution_id=node_execution.workflow_execution_id,
            node_id=node_execution.node_id,
            state=node_execution.state,
            input_data=node_execution.input_data,
        )
        
        self.session.add(model)
        await self.session.flush()
        return model
    
    async def update_node_execution_state(
        self,
        node_execution_id: UUID,
        new_state: str,
        worker_id: Optional[str] = None,
        output_data: Optional[dict[str, Any]] = None,
        error_message: Optional[str] = None,
        error_type: Optional[str] = None,
        error_stack_trace: Optional[str] = None,
    ) -> None:
        """
        Update node execution state.
        
        Note: This method will NOT downgrade terminal states (COMPLETED, FAILED, CANCELLED).
        If the node is already in a terminal state in the database, the update will be
        skipped to prevent race conditions in multi-orchestrator deployments.
        """
        terminal_states = ("COMPLETED", "FAILED", "CANCELLED")
        
        values: dict[str, Any] = {"state": new_state}
        
        if new_state == "RUNNING":
            values["started_at"] = datetime.utcnow()
        elif new_state in terminal_states:
            values["completed_at"] = datetime.utcnow()
        
        if worker_id:
            values["worker_id"] = worker_id
        if output_data:
            values["output_data"] = output_data
        if error_message:
            values["error_message"] = error_message
        if error_type:
            values["error_type"] = error_type
        if error_stack_trace:
            values["error_stack_trace"] = error_stack_trace
        
        # Build WHERE clause to prevent race conditions in multi-orchestrator deployments.
        # 
        # Rules:
        # 1. If new_state is terminal: Always allow (first to complete wins, subsequent are idempotent)
        # 2. If new_state is non-terminal: Only allow if DB state is also non-terminal
        #    (prevents downgrading COMPLETED → QUEUED from stale checkpoint)
        if new_state in terminal_states:
            # Always allow terminal state updates (idempotent)
            await self.session.execute(
                update(NodeExecutionModel)
                .where(NodeExecutionModel.id == node_execution_id)
                .values(**values)
            )
        else:
            # Non-terminal update: only if DB state is not already terminal
            await self.session.execute(
                update(NodeExecutionModel)
                .where(
                    and_(
                        NodeExecutionModel.id == node_execution_id,
                        # Only update if current DB state is NOT terminal
                        NodeExecutionModel.state.notin_(terminal_states),
                    )
                )
                .values(**values)
            )
    
    # ==================== Helper Methods ====================
    
    def model_to_workflow_definition(
        self,
        model: WorkflowDefinitionModel,
    ) -> WorkflowDefinition:
        """Convert database model to domain model."""
        return WorkflowDefinition(
            id=model.id,
            name=model.name,
            dag=DAGDefinition(**model.dag_definition),
            default_retry_config=RetryConfig(**model.default_retry_config),
            description=model.description,
            created_at=model.created_at,
            updated_at=model.updated_at,
        )
    
    def model_to_workflow_execution(
        self,
        model: WorkflowExecutionModel,
    ) -> WorkflowExecution:
        """Convert database model to domain model."""
        node_executions = {}
        for node_model in model.node_executions:
            node_executions[node_model.node_id] = NodeExecution(
                id=node_model.id,
                node_id=node_model.node_id,
                workflow_execution_id=node_model.workflow_execution_id,
                state=node_model.state,
                created_at=node_model.created_at,
                started_at=node_model.started_at,
                completed_at=node_model.completed_at,
                attempt=node_model.attempt,
                worker_id=node_model.worker_id,
                input_data=node_model.input_data,
                output_data=node_model.output_data,
                error_message=node_model.error_message,
                error_type=node_model.error_type,
                error_stack_trace=node_model.error_stack_trace,
            )
        
        return WorkflowExecution(
            id=model.id,
            workflow_definition_id=model.workflow_definition_id,
            state=model.state,
            created_at=model.created_at,
            started_at=model.started_at,
            completed_at=model.completed_at,
            input_params=model.input_params,
            output_data=model.output_data,
            node_executions=node_executions,
            error_message=model.error_message,
        )
