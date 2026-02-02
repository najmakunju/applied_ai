"""
Workflow orchestrator engine.

Manages the lifecycle of workflow executions including:
- DAG parsing and validation
- Dependency resolution
- Task dispatching
- State management
- Parallel execution coordination
- Crash recovery and fan-in state reconstruction
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any, Optional
from uuid import UUID, uuid4

import redis.asyncio as redis

from workflow_engine.config import get_settings
from workflow_engine.core.dag import DAGParser, parse_workflow_json
from workflow_engine.core.models import (
    DAGDefinition,
    HandlerType,
    NodeConfig,
    NodeDefinition,
    NodeExecution,
    RetryConfig,
    TaskMessage,
    WorkflowDefinition,
    WorkflowExecution,
)
from workflow_engine.core.state_machine import (
    NodeState,
    NodeStateMachine,
    WorkflowState,
    WorkflowStateMachine,
    compute_workflow_state_from_nodes,
)
from workflow_engine.messaging.broker import MessageBroker, ResultQueue
from workflow_engine.orchestrator.coordinator import FanInCoordinator
from sqlalchemy.exc import IntegrityError

from workflow_engine.storage.postgres.database import Database
from workflow_engine.storage.postgres.repository import WorkflowRepository
from workflow_engine.storage.redis.cache import RedisCache
from workflow_engine.template.resolver import TemplateResolver, resolve_node_input

logger = logging.getLogger(__name__)


class WorkflowOrchestrator:
    """
    Main orchestrator for workflow execution.
    
    Responsibilities:
    - Parse and validate workflow definitions
    - Track execution state
    - Dispatch tasks to workers
    - Handle node completion and failure
    - Coordinate parallel execution (fan-out/fan-in)
    """
    
    def __init__(
        self,
        redis_client: redis.Redis,
        database: Optional[Database] = None,
        orchestrator_id: Optional[str] = None,
    ):
        self.redis = redis_client
        self.database = database
        self.orchestrator_id = orchestrator_id or f"orchestrator-{uuid4().hex[:8]}"
        self.settings = get_settings()
        
        self.cache = RedisCache(redis_client)
        self.broker = MessageBroker(redis_client)
        self.fan_in_coordinator = FanInCoordinator(redis_client)
        self._result_queue: Optional[ResultQueue] = None
        
        # In-memory state (also persisted to Redis)
        self._active_executions: dict[UUID, WorkflowExecution] = {}
        self._workflow_definitions: dict[UUID, WorkflowDefinition] = {}
        self._dag_parsers: dict[UUID, DAGParser] = {}
        
        self._running = False
        self._result_processor_task: Optional[asyncio.Task] = None
        self._stale_result_claimer_task: Optional[asyncio.Task] = None
        self._checkpoint_task: Optional[asyncio.Task] = None
        self._recovery_completed = False
    
    async def init(self) -> None:
        """Initialize the orchestrator."""
        await self.fan_in_coordinator.init()
        
        # Initialize result queue for consuming worker results
        self._result_queue = await self.broker.get_result_queue()
        
        logger.info(f"Orchestrator {self.orchestrator_id} initialized")
    
    async def start(self) -> None:
        """Start the orchestrator processing loop."""
        if self._running:
            return
        
        self._running = True
        logger.info(f"Starting orchestrator {self.orchestrator_id}")
        
        # Drain pending results BEFORE recovery
        # This ensures node states are up-to-date when recovery runs,
        # preventing re-dispatch of nodes that already have results waiting
        await self._drain_pending_results()
        
        # Recover incomplete workflows if database is available. ie when there are stuck wf
        if self.database:
            try:
                await self._recover_incomplete_workflows()
                self._recovery_completed = True
            except Exception as e:
                logger.error(f"Recovery failed: {e}", exc_info=True)
                # Continue startup even if recovery fails
        
        # Start result processing task (consumes worker results from Redis Stream)
        self._result_processor_task = asyncio.create_task(self._process_results_loop())
        
        # Start stale result claimer task (handles results from dead orchestrators)
        self._stale_result_claimer_task = asyncio.create_task(self._claim_stale_results_loop())
        
        # Start checkpoint task if database is available
        if self.database:
            self._checkpoint_task = asyncio.create_task(self._checkpoint_loop())
    
    async def stop(self) -> None:
        """Stop the orchestrator gracefully."""
        if not self._running:
            return
        
        self._running = False
        logger.info(f"Stopping orchestrator {self.orchestrator_id}")
        
        # Stop checkpoint task
        if self._checkpoint_task:
            self._checkpoint_task.cancel()
            try:
                await self._checkpoint_task
            except asyncio.CancelledError:
                pass
        
        # Stop stale result claimer task
        if self._stale_result_claimer_task:
            self._stale_result_claimer_task.cancel()
            try:
                await self._stale_result_claimer_task
            except asyncio.CancelledError:
                pass
        
        if self._result_processor_task:
            self._result_processor_task.cancel()
            try:
                await self._result_processor_task
            except asyncio.CancelledError:
                pass
    
    # ==================== Workflow Submission ====================
    
    async def submit_workflow(
        self,
        workflow_json: dict[str, Any],
    ) -> tuple[UUID, UUID]:
        """
        Submit a new workflow for execution.
        
        Args:
            workflow_json: Workflow definition JSON
            
        Returns:
            Tuple of (workflow_definition_id, execution_id)
            
        Raises:
            ValueError: If workflow validation fails
        """
        # Parse and validate
        definition, validation_result = parse_workflow_json(workflow_json)
        
        if not validation_result.is_valid:
            errors = [f"{e.code}: {e.message}" for e in validation_result.errors]
            raise ValueError(f"Workflow validation failed: {errors}")
        
        # Store definition
        self._workflow_definitions[definition.id] = definition
        self._dag_parsers[definition.id] = DAGParser(definition)
        
        # Create execution
        execution = WorkflowExecution(
            workflow_definition_id=definition.id,
            state=WorkflowState.PENDING.value,
        )
        
        # Initialize node executions
        for node in definition.dag.nodes:
            node_exec = NodeExecution(
                node_id=node.id,
                workflow_execution_id=execution.id,
                state=NodeState.PENDING.value,
            )
            execution.node_executions[node.id] = node_exec
        
        # Store execution in memory
        self._active_executions[execution.id] = execution
        
        # Persist to database
        if self.database:
            try:
                async with self.database.session() as session:
                    repo = WorkflowRepository(session)
                    
                    # Create workflow definition in DB
                    await repo.create_workflow_definition(definition)
                    
                    # Create workflow execution in DB
                    await repo.create_workflow_execution(execution)
                    
                    # Create node executions in DB
                    for node_exec in execution.node_executions.values():
                        await repo.create_node_execution(node_exec)
            except IntegrityError as e:
                # Clean up in-memory state
                self._workflow_definitions.pop(definition.id, None)
                self._dag_parsers.pop(definition.id, None)
                self._active_executions.pop(execution.id, None)
                
                # Check if it's a duplicate name error
                if "workflow_definitions_name_key" in str(e):
                    raise ValueError(
                        f"Workflow with name '{definition.name}' already exists. "
                        f"Please use a different name or delete the existing workflow."
                    ) from None
                raise
        
        # Cache in Redis
        await self.cache.cache_workflow_execution(
            execution.id,
            execution.model_dump(mode="json"),
        )
        
        logger.info(
            f"Workflow submitted: definition={definition.id}, execution={execution.id}"
        )
        
        return definition.id, execution.id
    
    async def trigger_workflow(
        self,
        execution_id: UUID,
        input_params: Optional[dict[str, Any]] = None,
    ) -> None:
        """
        Trigger execution of a submitted workflow.
        
        Handles both starting a new workflow (PENDING state) and resuming
        a paused workflow (PAUSED state).
        
        Args:
            execution_id: The execution ID to trigger
            input_params: Optional input parameters
        """
        # Load from memory, Redis, or PostgreSQL
        execution, definition = await self._ensure_execution_loaded(execution_id)
        if not execution:
            raise ValueError(f"Execution not found: {execution_id}")
        if not definition:
            raise ValueError(f"Workflow definition not found for execution: {execution_id}")
        
        current_state = execution.state
        
        if current_state == WorkflowState.PENDING.value:
            # Start new workflow
            await self._start_workflow(execution, definition, input_params)
        elif current_state == WorkflowState.PAUSED.value:
            # Resume paused workflow
            await self._resume_workflow(execution, definition, input_params)
        else:
            raise ValueError(
                f"Cannot trigger workflow in state: {current_state}. "
                f"Only PENDING or PAUSED workflows can be triggered."
            )
    
    async def _start_workflow(
        self,
        execution: WorkflowExecution,
        definition: WorkflowDefinition,
        input_params: Optional[dict[str, Any]] = None,
    ) -> None:
        """Start a new workflow from PENDING state."""
        # Update input params
        if input_params:
            execution.input_params = input_params
        
        # Transition to RUNNING
        execution.state = WorkflowState.RUNNING.value
        execution.started_at = datetime.utcnow()
        
        # Initialize fan-in counters for nodes with multiple dependencies
        for node in definition.dag.nodes:
            if len(node.dependencies) > 1:
                await self.fan_in_coordinator.initialize_fan_in(
                    execution.id,
                    node.id,
                    len(node.dependencies),
                )
        
        # Schedule ready nodes (those with no dependencies)
        await self._schedule_ready_nodes(execution, definition)
        
        # Update cache
        await self.cache.cache_workflow_execution(
            execution.id,
            execution.model_dump(mode="json"),
        )
        
        logger.info(f"Workflow started: {execution.id}")
    
    async def _resume_workflow(
        self,
        execution: WorkflowExecution,
        definition: WorkflowDefinition,
        input_params: Optional[dict[str, Any]] = None,
    ) -> None:
        """Resume a paused workflow from where it stopped."""
        # Update input params if provided
        if input_params:
            execution.input_params = {**execution.input_params, **input_params}
        
        # Transition to RUNNING
        execution.state = WorkflowState.RUNNING.value
        
        # Re-initialize fan-in counters for incomplete fan-in nodes
        # Only for nodes that haven't completed yet
        completed_nodes = {
            node_id
            for node_id, node_exec in execution.node_executions.items()
            if node_exec.state == NodeState.COMPLETED.value
        }
        
        for node in definition.dag.nodes:
            if len(node.dependencies) > 1 and node.id not in completed_nodes:
                # Count how many dependencies are already completed
                completed_deps = sum(
                    1 for dep_id in node.dependencies if dep_id in completed_nodes
                )
                remaining = len(node.dependencies) - completed_deps
                
                if remaining > 0:
                    await self.fan_in_coordinator.initialize_fan_in(
                        execution.id,
                        node.id,
                        remaining,
                    )
        
        # Re-dispatch paused nodes
        paused_nodes = [
            node for node in definition.dag.nodes
            if execution.node_executions.get(node.id)
            and execution.node_executions[node.id].state == NodeState.PAUSED.value
        ]
        
        for node in paused_nodes:
            await self._dispatch_node(execution, definition, node)
        
        # Schedule any pending nodes that are now ready
        await self._schedule_ready_nodes(execution, definition)
        
        # Update cache
        await self.cache.cache_workflow_execution(
            execution.id,
            execution.model_dump(mode="json"),
        )
        
        logger.info(
            f"Workflow resumed: {execution.id}, "
            f"re-dispatched {len(paused_nodes)} paused nodes"
        )
    
    # ==================== Node Scheduling ====================
    
    async def _schedule_ready_nodes(
        self,
        execution: WorkflowExecution,
        definition: WorkflowDefinition,
    ) -> None:
        """Schedule all nodes that are ready to execute."""
        parser = self._dag_parsers.get(definition.id)
        if not parser:
            return
        
        # Get completed node IDs
        completed = {
            node_id
            for node_id, node_exec in execution.node_executions.items()
            if node_exec.state == NodeState.COMPLETED.value
        }
        
        # Find ready nodes
        ready_nodes = parser.get_ready_nodes(completed)
        
        for node in ready_nodes:
            node_exec = execution.node_executions.get(node.id)
            if node_exec and node_exec.state == NodeState.PENDING.value:
                await self._dispatch_node(execution, definition, node)
    
    async def _dispatch_node(
        self,
        execution: WorkflowExecution,
        definition: WorkflowDefinition,
        node: NodeDefinition,
    ) -> None:
        """Dispatch a node for execution."""
        node_exec = execution.node_executions[node.id]
        
        # Transition to QUEUED
        node_exec.state = NodeState.QUEUED.value
        
        # Resolve input data from dependencies
        completed_outputs = execution.get_completed_node_outputs()
        
        # For fan-in nodes, also get aggregated outputs
        if len(node.dependencies) > 1:
            fan_in_outputs = await self.fan_in_coordinator.get_aggregated_outputs(
                execution.id,
                node.id,
            )
            completed_outputs.update(fan_in_outputs)
        
        # Resolve templates in config
        resolved_config = resolve_node_input(
            node.config.model_dump(),
            completed_outputs,
            execution.input_params,
        )
        
        # Build input data from dependencies
        input_data = {}
        for dep_id in node.dependencies:
            if dep_id in completed_outputs:
                input_data[dep_id] = completed_outputs[dep_id]
        
        # Add workflow input params
        input_data["_input"] = execution.input_params
        
        # Get effective configs
        parser = self._dag_parsers.get(definition.id)
        retry_config = parser.get_node_effective_retry_config(node.id) if parser else definition.default_retry_config
        
        # Create task message
        task_message = TaskMessage(
            node_execution_id=node_exec.id,
            workflow_execution_id=execution.id,
            node_id=node.id,
            handler=node.handler,
            config=NodeConfig(**resolved_config),
            input_data=input_data,
            attempt=node_exec.attempt,
            retry_config=retry_config,
            idempotency_key=f"{execution.id}:{node.id}:{node_exec.attempt}",
        )
        
        # Publish to appropriate queue
        queue_name = f"tasks:{node.handler.value}"
        await self.broker.publish_task(task_message, queue_name)
        
        # Update cache
        await self.cache.cache_node_execution(
            execution.id,
            node.id,
            node_exec.model_dump(mode="json"),
        )
        
        logger.info(f"Dispatched node {node.id} for execution {execution.id}")
    
    # ==================== Result Processing ====================
    
    async def _ensure_execution_loaded(
        self,
        workflow_execution_id: UUID,
    ) -> tuple[Optional[WorkflowExecution], Optional[WorkflowDefinition]]:
        """
        Ensure workflow execution is loaded in memory.
        
        Tries to load from:
        1. In-memory cache
        2. Redis cache
        3. PostgreSQL database (if configured)
        
        Returns tuple of (execution, definition) or (None, None) if not found.
        """
        # Check in-memory first
        execution = self._active_executions.get(workflow_execution_id)
        if execution:
            definition = self._workflow_definitions.get(execution.workflow_definition_id)
            return execution, definition
        
        # Try to load from Redis cache
        cached = await self.cache.get_workflow_execution(workflow_execution_id)
        if cached:
            logger.info(f"Loading execution {workflow_execution_id} from Redis cache")
            
            # Reconstruct execution from cached data
            execution = WorkflowExecution(
                id=UUID(cached["id"]),
                workflow_definition_id=UUID(cached["workflow_definition_id"]),
                state=cached.get("state", "PENDING"),
                input_params=cached.get("input_params", {}),
                output_data=cached.get("output_data"),
                error_message=cached.get("error_message"),
            )
            
            # Parse timestamps
            if cached.get("created_at"):
                execution.created_at = datetime.fromisoformat(cached["created_at"].replace("Z", "+00:00"))
            if cached.get("started_at"):
                execution.started_at = datetime.fromisoformat(cached["started_at"].replace("Z", "+00:00"))
            if cached.get("completed_at"):
                execution.completed_at = datetime.fromisoformat(cached["completed_at"].replace("Z", "+00:00"))
            
            # Reconstruct node executions
            for node_id, node_data in cached.get("node_executions", {}).items():
                node_exec = NodeExecution(
                    id=UUID(node_data["id"]),
                    node_id=node_id,
                    workflow_execution_id=workflow_execution_id,
                    state=node_data.get("state", "PENDING"),
                    attempt=node_data.get("attempt", 1),
                    worker_id=node_data.get("worker_id"),
                    input_data=node_data.get("input_data", {}),
                    output_data=node_data.get("output_data"),
                    error_message=node_data.get("error_message"),
                    error_type=node_data.get("error_type"),
                )
                execution.node_executions[node_id] = node_exec
            
            # Store in memory
            self._active_executions[execution.id] = execution
            
            # Try to load definition
            definition = await self._ensure_definition_loaded(execution.workflow_definition_id)
            
            return execution, definition
        
        # Try to load from PostgreSQL
        if self.database:
            logger.info(f"Loading execution {workflow_execution_id} from PostgreSQL")
            async with self.database.session() as session:
                repo = WorkflowRepository(session)
                
                wf_model = await repo.get_workflow_execution(workflow_execution_id)
                if wf_model:
                    # Load definition
                    def_model = await repo.get_workflow_definition(wf_model.workflow_definition_id)
                    if def_model:
                        definition = repo.model_to_workflow_definition(def_model)
                        execution = repo.model_to_workflow_execution(wf_model)
                        
                        # Store in memory
                        self._workflow_definitions[definition.id] = definition
                        self._dag_parsers[definition.id] = DAGParser(definition)
                        self._active_executions[execution.id] = execution
                        
                        # Update Redis cache
                        await self.cache.cache_workflow_execution(
                            execution.id,
                            execution.model_dump(mode="json"),
                        )
                        
                        return execution, definition
        
        return None, None
    
    async def _ensure_definition_loaded(
        self,
        definition_id: UUID,
    ) -> Optional[WorkflowDefinition]:
        """Ensure workflow definition is loaded in memory."""
        # Check in-memory first
        definition = self._workflow_definitions.get(definition_id)
        if definition:
            return definition
        
        # Try to load from PostgreSQL
        if self.database:
            async with self.database.session() as session:
                repo = WorkflowRepository(session)
                def_model = await repo.get_workflow_definition(definition_id)
                if def_model:
                    definition = repo.model_to_workflow_definition(def_model)
                    self._workflow_definitions[definition.id] = definition
                    self._dag_parsers[definition.id] = DAGParser(definition)
                    return definition
        
        return None
    
    async def handle_node_completed(
        self,
        workflow_execution_id: UUID,
        node_id: str,
        output_data: dict[str, Any],
        worker_id: str,
    ) -> None:
        """
        Handle successful node completion.
        
        Args:
            workflow_execution_id: Workflow execution ID
            node_id: Completed node ID
            output_data: Node output data
            worker_id: Worker that processed the node
        """
        # Ensure execution is loaded (from memory, cache, or DB)
        execution, definition = await self._ensure_execution_loaded(workflow_execution_id)
        
        if not execution:
            logger.error(
                f"Execution not found anywhere: {workflow_execution_id}. "
                "Result will be lost!"
            )
            return
        
        if not definition:
            logger.error(
                f"Definition not found for execution {workflow_execution_id}. "
                "Cannot process completion."
            )
            return
        
        node_exec = execution.node_executions.get(node_id)
        if not node_exec:
            logger.warning(f"Node execution not found: {node_id}")
            return
        
        # Update node state
        node_exec.state = NodeState.COMPLETED.value
        node_exec.completed_at = datetime.utcnow()
        node_exec.output_data = output_data
        node_exec.worker_id = worker_id
        
        logger.info(f"Node {node_id} completed for execution {workflow_execution_id}")
        
        # Check for dependent nodes and handle fan-in
        for node in definition.dag.nodes:
            if node_id in node.dependencies:
                if len(node.dependencies) == 1:
                    # Simple dependency - schedule immediately
                    dep_node_exec = execution.node_executions.get(node.id)
                    if dep_node_exec and dep_node_exec.state == NodeState.PENDING.value:
                        await self._dispatch_node(execution, definition, node)
                else:
                    # Fan-in - use recovery-aware coordinator
                    should_trigger = await self._handle_fan_in_dependency_completed(
                        execution,
                        definition,
                        node,
                        node_id,
                        output_data,
                    )
                    if should_trigger:
                        dep_node_exec = execution.node_executions.get(node.id)
                        if dep_node_exec and dep_node_exec.state == NodeState.PENDING.value:
                            await self._dispatch_node(execution, definition, node)
        
        # Check if workflow is complete
        await self._check_workflow_completion(execution, definition)
        
        # Update cache
        await self.cache.cache_workflow_execution(
            execution.id,
            execution.model_dump(mode="json"),
        )
    
    async def handle_node_failed(
        self,
        workflow_execution_id: UUID,
        node_id: str,
        error_message: str,
        error_type: str,
        is_retryable: bool,
        worker_id: str,
    ) -> None:
        """Handle node failure."""
        # Ensure execution is loaded (from memory, cache, or DB)
        execution, definition = await self._ensure_execution_loaded(workflow_execution_id)
        
        if not execution:
            logger.error(
                f"Execution not found anywhere: {workflow_execution_id}. "
                "Failure result will be lost!"
            )
            return
        
        if not definition:
            logger.error(
                f"Definition not found for execution {workflow_execution_id}. "
                "Cannot process failure."
            )
            return
        
        node_exec = execution.node_executions.get(node_id)
        if not node_exec:
            logger.warning(f"Node execution not found: {node_id}")
            return
        
        parser = self._dag_parsers.get(definition.id)
        retry_config = parser.get_node_effective_retry_config(node_id) if parser else definition.default_retry_config
        
        # Check if we should retry
        if is_retryable and node_exec.attempt < retry_config.max_retries:
            # Retry
            node_exec.attempt += 1
            node_exec.state = NodeState.PENDING.value
            node_exec.error_message = error_message
            
            logger.info(
                f"Retrying node {node_id}, attempt {node_exec.attempt}/{retry_config.max_retries}"
            )
            
            # Re-dispatch
            node = definition.dag.get_node(node_id)
            if node:
                await self._dispatch_node(execution, definition, node)
        else:
            # Mark as failed
            node_exec.state = NodeState.FAILED.value
            node_exec.completed_at = datetime.utcnow()
            node_exec.error_message = error_message
            node_exec.error_type = error_type
            node_exec.worker_id = worker_id
            
            logger.error(f"Node {node_id} failed for execution {workflow_execution_id}")
            
            # Fail the workflow
            execution.state = WorkflowState.FAILED.value
            execution.completed_at = datetime.utcnow()
            execution.error_message = f"Node {node_id} failed: {error_message}"
        
        # Update cache
        await self.cache.cache_workflow_execution(
            execution.id,
            execution.model_dump(mode="json"),
        )
    
    async def _handle_fan_in_dependency_completed(
        self,
        execution: WorkflowExecution,
        definition: WorkflowDefinition,
        target_node: NodeDefinition,
        completed_node_id: str,
        output_data: dict[str, Any],
    ) -> bool:
        """
        Handle fan-in dependency completion with recovery support.
        
        This method wraps the fan-in coordinator with logic to recover
        the counter from node states if it's missing (e.g., after Redis restart).
        
        Args:
            execution: The workflow execution
            definition: The workflow definition
            target_node: The fan-in node waiting for dependencies
            completed_node_id: The dependency that just completed
            output_data: Output from the completed dependency
            
        Returns:
            True if all dependencies are complete and target should run
        """
        # First, check if counter exists and recover if missing
        completed_deps = []
        completed_outputs: dict[str, dict] = {}
        
        for dep_id in target_node.dependencies:
            dep_exec = execution.node_executions.get(dep_id)
            if dep_exec and dep_exec.state == NodeState.COMPLETED.value:
                completed_deps.append(dep_id)
                if dep_exec.output_data:
                    completed_outputs[dep_id] = dep_exec.output_data
        
        # Add current completion if not already in the list
        if completed_node_id not in completed_deps:
            completed_deps.append(completed_node_id)
            completed_outputs[completed_node_id] = output_data
        
        # Check and recover counter if needed
        was_recovered, remaining = await self.fan_in_coordinator.check_and_recover_counter(
            execution.id,
            target_node.id,
            len(target_node.dependencies),
            completed_deps,
            completed_outputs,
        )
        
        if was_recovered:
            logger.info(
                f"Recovered fan-in counter for {target_node.id}: {remaining} remaining"
            )
            # Counter was just recovered, check if ready
            return remaining == 0
        
        # Counter exists, proceed with normal decrement
        should_trigger = await self.fan_in_coordinator.dependency_completed(
            execution.id,
            target_node.id,
            completed_node_id,
            output_data,
        )
        
        return should_trigger
    
    async def _check_workflow_completion(
        self,
        execution: WorkflowExecution,
        definition: WorkflowDefinition,
    ) -> None:
        """Check if workflow execution is complete."""
        node_states = {
            node_id: NodeState(node_exec.state)
            for node_id, node_exec in execution.node_executions.items()
        }
        all_nodes = set(execution.node_executions.keys())
        
        computed_state = compute_workflow_state_from_nodes(node_states, all_nodes)
        
        if computed_state in (WorkflowState.COMPLETED, WorkflowState.FAILED):
            execution.state = computed_state.value
            execution.completed_at = datetime.utcnow()
            
            # Aggregate output from leaf nodes (output handlers)
            if computed_state == WorkflowState.COMPLETED:
                output_data = {}
                for node in definition.dag.get_leaf_nodes():
                    node_exec = execution.node_executions.get(node.id)
                    if node_exec and node_exec.output_data:
                        output_data[node.id] = node_exec.output_data
                
                # If single output node, flatten
                if len(output_data) == 1:
                    execution.output_data = list(output_data.values())[0]
                else:
                    execution.output_data = output_data
            
            # Persist final state to database immediately
            await self._persist_workflow_completion(execution)
            
            # Cleanup: remove from in-memory and Redis cache
            await self._cleanup_completed_workflow(execution)
            
            logger.info(
                f"Workflow {execution.id} completed with state: {computed_state.value}"
            )
    
    async def _persist_workflow_completion(
        self,
        execution: WorkflowExecution,
    ) -> None:
        """Persist workflow and node states to database when workflow completes."""
        if not self.database:
            return
        
        try:
            async with self.database.session() as session:
                repo = WorkflowRepository(session)
                
                # Update workflow execution state
                await repo.update_workflow_execution_state(
                    execution.id,
                    execution.state,
                    error_message=execution.error_message,
                    output_data=execution.output_data,
                )
                
                # Update all node execution states
                for node_exec in execution.node_executions.values():
                    await repo.update_node_execution_state(
                        node_exec.id,
                        node_exec.state,
                        worker_id=node_exec.worker_id,
                        output_data=node_exec.output_data,
                        error_message=node_exec.error_message,
                        error_type=node_exec.error_type,
                    )
                
                logger.debug(f"Persisted final state for workflow {execution.id}")
                
        except Exception as e:
            logger.error(f"Failed to persist workflow completion {execution.id}: {e}")
    
    async def _cleanup_completed_workflow(
        self,
        execution: WorkflowExecution,
    ) -> None:
        """
        Cleanup resources after workflow completion.
        
        Removes workflow from:
        - In-memory active executions dict
        - Redis cache
        
        The workflow data remains in PostgreSQL for historical queries.
        """
        try:
            # Remove from in-memory dict
            if execution.id in self._active_executions:
                del self._active_executions[execution.id]
                logger.debug(f"Removed workflow {execution.id} from active executions")
            
            # Remove from Redis cache
            await self.cache.delete_workflow_execution(execution.id)
            logger.debug(f"Removed workflow {execution.id} from Redis cache")
            
        except Exception as e:
            # Log but don't fail - cleanup is best-effort
            logger.warning(f"Cleanup failed for workflow {execution.id}: {e}")
    
    async def _drain_pending_results(self) -> None:
        """
        Drain all pending results from the stream before recovery.
        
        This ensures node states are up-to-date when recovery runs,
        preventing re-dispatch of nodes that already have completed results
        waiting in the stream.
        
        Called during startup, before _recover_incomplete_workflows().
        """
        if not self._result_queue:
            logger.warning("Result queue not initialized, skipping drain")
            return
        
        logger.info("Draining pending results before recovery...")
        total_processed = 0
        
        # Process results in batches until none remain
        while True:
            try:
                # Use non-blocking consume (block_ms=0) to check for pending results
                results = await self._result_queue.consume(
                    consumer_id=self.orchestrator_id,
                    count=50,  # Larger batch for faster draining
                    block_ms=100,  # Short timeout - just check what's pending
                )
                
                if not results:
                    # No more pending results
                    break
                
                for msg_id, result_data in results:
                    try:
                        await self._handle_result_message(result_data)
                        await self._result_queue.acknowledge(msg_id)
                        total_processed += 1
                    except Exception as e:
                        logger.error(
                            f"Error processing result during drain {msg_id}: {e}",
                            exc_info=True,
                        )
                        # Don't acknowledge - will be retried later
                        
            except Exception as e:
                logger.error(f"Error during result drain: {e}", exc_info=True)
                break
        
        if total_processed > 0:
            logger.info(f"Drained {total_processed} pending results before recovery")
        else:
            logger.info("No pending results to drain")
    
    async def _process_results_loop(self) -> None:
        """
        Background loop to consume and process task results from Redis Stream.
        
        Uses consumer groups for reliable processing:
        - Multiple orchestrators can share the load
        - Failed results are automatically retried
        - At-least-once delivery guarantee
        """
        if not self._result_queue:
            logger.error("Result queue not initialized, cannot process results")
            return
        
        block_ms = self.settings.redis.stream_block_ms
        
        while self._running:
            try:
                # Consume results from the stream
                results = await self._result_queue.consume(
                    consumer_id=self.orchestrator_id,
                    count=10,  # Process up to 10 results at a time
                    block_ms=block_ms,
                )
                
                for msg_id, result_data in results:
                    try:
                        await self._handle_result_message(result_data)
                        
                        # Acknowledge successful processing
                        await self._result_queue.acknowledge(msg_id)
                        
                    except Exception as e:
                        logger.error(
                            f"Error processing result {msg_id}: {e}",
                            exc_info=True,
                        )
                        # Don't acknowledge - will be retried or claimed by another orchestrator
                        
            except asyncio.CancelledError:
                logger.debug("Result processor loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error in result processor loop: {e}", exc_info=True)
                await asyncio.sleep(1)
    
    async def _handle_result_message(self, result_data: dict) -> None:
        """
        Handle a single result message from the stream.
        
        Routes to handle_node_completed or handle_node_failed based on success.
        """
        workflow_execution_id = UUID(result_data["workflow_execution_id"])
        node_id = result_data["node_id"]
        success = result_data["success"]
        worker_id = result_data["worker_id"]
        
        if success:
            await self.handle_node_completed(
                workflow_execution_id=workflow_execution_id,
                node_id=node_id,
                output_data=result_data.get("output_data", {}),
                worker_id=worker_id,
            )
        else:
            await self.handle_node_failed(
                workflow_execution_id=workflow_execution_id,
                node_id=node_id,
                error_message=result_data.get("error_message") or "Unknown error",
                error_type=result_data.get("error_type") or "UnknownError",
                is_retryable=result_data.get("is_retryable", True),
                worker_id=worker_id,
            )
    
    async def _claim_stale_results_loop(self) -> None:
        """
        Periodically claim stale results from dead orchestrators.
        
        If an orchestrator dies while processing a result, the message
        remains pending. This loop claims those messages and reprocesses them.
        """
        if not self._result_queue:
            return
        
        claim_interval = self.settings.orchestrator.stale_claim_interval
        stale_timeout_ms = int(self.settings.orchestrator.stale_result_timeout * 1000)
        
        while self._running:
            try:
                # Claim results that have been idle longer than the configured timeout
                claimed = await self._result_queue.claim_stale_results(
                    consumer_id=self.orchestrator_id,
                    min_idle_ms=stale_timeout_ms,
                    count=5,
                )
                
                for msg_id, result_data in claimed:
                    try:
                        logger.info(f"Claimed stale result: {msg_id}")
                        await self._handle_result_message(result_data)
                        await self._result_queue.acknowledge(msg_id)
                    except Exception as e:
                        logger.error(
                            f"Error processing claimed result {msg_id}: {e}",
                            exc_info=True,
                        )
                
                await asyncio.sleep(claim_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in stale result claimer: {e}", exc_info=True)
                await asyncio.sleep(5)
    
    # ==================== Recovery ====================
    
    async def _recover_incomplete_workflows(self) -> None:
        """
        Recover incomplete workflows after a restart.
        
        This method:
        1. Finds workflows in PENDING/RUNNING/PAUSED state from PostgreSQL
        2. Loads their definitions and node states
        3. Reconstructs fan-in counters from completed node states
        4. Re-dispatches any nodes that were stuck in QUEUED/RUNNING state
        """
        if not self.database:
            logger.warning("Cannot recover: no database configured")
            return
        
        logger.info("Starting workflow recovery...")
        
        # Get incomplete workflows that were created before now.
        recovery_cutoff = datetime.utcnow() - timedelta(
            seconds=self.settings.orchestrator.recovery_timeout
        )
        
        async with self.database.session() as session:
            repo = WorkflowRepository(session)
            
            incomplete_workflows = await repo.get_incomplete_workflow_executions(
                before=recovery_cutoff
            )
            
            logger.info(f"Found {len(incomplete_workflows)} incomplete workflows to recover")
            
            for wf_model in incomplete_workflows:
                try:
                    await self._recover_single_workflow(repo, wf_model)
                except Exception as e:
                    logger.error(
                        f"Failed to recover workflow {wf_model.id}: {e}",
                        exc_info=True
                    )
    
    async def _recover_single_workflow(
        self,
        repo: WorkflowRepository,
        wf_model: Any,
    ) -> None:
        """Recover a single workflow execution."""
        logger.info(f"Recovering workflow: {wf_model.id}")
        
        # Load workflow definition
        def_model = await repo.get_workflow_definition(wf_model.workflow_definition_id)
        if not def_model:
            logger.error(f"Definition not found for workflow {wf_model.id}")
            return
        
        # Convert to domain models
        definition = repo.model_to_workflow_definition(def_model)
        execution = repo.model_to_workflow_execution(wf_model)
        
        # Store in memory
        self._workflow_definitions[definition.id] = definition
        self._dag_parsers[definition.id] = DAGParser(definition)
        self._active_executions[execution.id] = execution
        
        # Recover fan-in counters
        await self._recover_fan_in_counters(execution, definition)
        
        # Re-dispatch stuck nodes
        await self._recover_stuck_nodes(execution, definition)
        
        # Update Redis cache
        await self.cache.cache_workflow_execution(
            execution.id,
            execution.model_dump(mode="json"),
        )
        
        logger.info(f"Successfully recovered workflow: {execution.id}")
    
    async def _recover_fan_in_counters(
        self,
        execution: WorkflowExecution,
        definition: WorkflowDefinition,
    ) -> None:
        """
        Recover fan-in counters from node states.
        
        For each node with multiple dependencies, reconstruct the counter
        based on how many dependencies have completed.
        """
        for node in definition.dag.nodes:
            if len(node.dependencies) <= 1:
                # Not a fan-in node
                continue
            
            # Check if counter already exists in Redis
            is_initialized = await self.fan_in_coordinator.is_counter_initialized(
                execution.id,
                node.id,
            )
            
            if is_initialized:
                logger.debug(f"Fan-in counter for {node.id} already exists")
                continue
            
            # Determine which dependencies have completed
            completed_deps = []
            completed_outputs: dict[str, dict] = {}
            
            for dep_id in node.dependencies:
                dep_exec = execution.node_executions.get(dep_id)
                if dep_exec and dep_exec.state == NodeState.COMPLETED.value:
                    completed_deps.append(dep_id)
                    if dep_exec.output_data:
                        completed_outputs[dep_id] = dep_exec.output_data
            
            # Recover the counter
            remaining = await self.fan_in_coordinator.recover_fan_in_state(
                execution.id,
                node.id,
                len(node.dependencies),
                completed_deps,
                completed_outputs,
            )
            
            # If all dependencies are already complete, dispatch the node
            if remaining == 0:
                node_exec = execution.node_executions.get(node.id)
                if node_exec and node_exec.state == NodeState.PENDING.value:
                    logger.info(
                        f"Recovery: All dependencies complete for {node.id}, dispatching"
                    )
                    await self._dispatch_node(execution, definition, node)
    
    async def _recover_stuck_nodes(
        self,
        execution: WorkflowExecution,
        definition: WorkflowDefinition,
    ) -> None:
        """
        Re-dispatch nodes that were stuck in QUEUED or RUNNING state.
        
        These nodes may have been picked up by workers that died, or the
        messages may have been lost.
        """
        stuck_states = {NodeState.QUEUED.value, NodeState.RUNNING.value}
        
        for node_id, node_exec in execution.node_executions.items():
            if node_exec.state not in stuck_states:
                continue
            
            node = definition.dag.get_node(node_id)
            if not node:
                continue
            
            logger.info(f"Recovery: Re-dispatching stuck node {node_id} (was {node_exec.state})")
            
            # Reset to PENDING and increment attempt
            node_exec.state = NodeState.PENDING.value
            node_exec.attempt += 1
            
            # Check if this is a fan-in node - ensure counter is recovered first
            if len(node.dependencies) > 1:
                # For fan-in nodes, check if all dependencies are complete
                all_deps_complete = all(
                    execution.node_executions.get(dep_id, NodeExecution(
                        node_id=dep_id, 
                        workflow_execution_id=execution.id
                    )).state == NodeState.COMPLETED.value
                    for dep_id in node.dependencies
                )
                
                if not all_deps_complete:
                    logger.info(
                        f"Recovery: Fan-in node {node_id} waiting for dependencies"
                    )
                    continue
            
            # Re-dispatch
            await self._dispatch_node(execution, definition, node)
    
    # ==================== Checkpointing ====================
    
    async def _checkpoint_loop(self) -> None:
        """Periodically checkpoint active workflow states to PostgreSQL."""
        interval = self.settings.orchestrator.checkpoint_interval
        
        while self._running:
            try:
                await self._checkpoint_active_workflows()
            except Exception as e:
                logger.error(f"Checkpoint failed: {e}", exc_info=True)
            
            await asyncio.sleep(interval)
    
    async def _checkpoint_active_workflows(self) -> None:
        """
        Checkpoint all active workflow executions.
        
        Each workflow gets its own transaction for resilience:
        - If one workflow checkpoint fails, others still succeed
        - Crash mid-checkpoint only loses uncommitted workflows
        """
        if not self.database or not self._active_executions:
            return
        
        checkpointed = 0
        failed = 0
        
        for execution_id, execution in list(self._active_executions.items()):
            # Skip completed workflows
            if execution.state in (
                WorkflowState.COMPLETED.value,
                WorkflowState.FAILED.value,
                WorkflowState.CANCELLED.value,
            ):
                continue
            
            # Each workflow gets its own transaction for isolation
            try:
                async with self.database.session() as session:
                    repo = WorkflowRepository(session)
                    
                    # Update workflow state in PostgreSQL
                    await repo.update_workflow_execution_state(
                        execution_id,
                        execution.state,
                        error_message=execution.error_message,
                        output_data=execution.output_data,
                    )
                    
                    # Update node states
                    for node_exec in execution.node_executions.values():
                        await repo.update_node_execution_state(
                            node_exec.id,
                            node_exec.state,
                            worker_id=node_exec.worker_id,
                            output_data=node_exec.output_data,
                            error_message=node_exec.error_message,
                            error_type=node_exec.error_type,
                        )
                    
                    # Update checkpoint timestamp
                    await repo.checkpoint_workflow_execution(execution_id)
                
                # Transaction committed successfully
                checkpointed += 1
                
            except Exception as e:
                failed += 1
                logger.error(
                    f"Failed to checkpoint workflow {execution_id}: {e}"
                )
        
        if checkpointed > 0 or failed > 0:
            logger.debug(
                f"Checkpoint complete: {checkpointed} succeeded, {failed} failed"
            )
    
    # ==================== Query Methods ====================
    
    async def get_workflow_status(
        self,
        execution_id: UUID,
    ) -> Optional[dict[str, Any]]:
        """Get current workflow status."""
        # Check in-memory first (active workflows)
        execution = self._active_executions.get(execution_id)
        if execution:
            return {
                "id": str(execution.id),
                "state": execution.state,
                "started_at": execution.started_at.isoformat() if execution.started_at else None,
                "completed_at": execution.completed_at.isoformat() if execution.completed_at else None,
                "node_states": {
                    node_id: node_exec.state
                    for node_id, node_exec in execution.node_executions.items()
                },
            }
        
        # Check Redis cache (active workflows)
        cached = await self.cache.get_workflow_execution(execution_id)
        if cached:
            return {
                "id": cached.get("id"),
                "state": cached.get("state"),
                "started_at": cached.get("started_at"),
                "completed_at": cached.get("completed_at"),
                "node_states": {
                    node_id: node_data.get("state")
                    for node_id, node_data in cached.get("node_executions", {}).items()
                },
            }
        
        # Check PostgreSQL (completed/historical workflows)
        if self.database:
            async with self.database.session() as session:
                repo = WorkflowRepository(session)
                wf_model = await repo.get_workflow_execution(execution_id)
                if wf_model:
                    return {
                        "id": str(wf_model.id),
                        "state": wf_model.state,
                        "started_at": wf_model.started_at.isoformat() if wf_model.started_at else None,
                        "completed_at": wf_model.completed_at.isoformat() if wf_model.completed_at else None,
                        "node_states": {
                            node_exec.node_id: node_exec.state
                            for node_exec in wf_model.node_executions
                        },
                    }
        
        return None
    
    async def get_workflow_results(
        self,
        execution_id: UUID,
    ) -> Optional[dict[str, Any]]:
        """Get workflow execution results."""
        # Check in-memory first (active workflows)
        execution = self._active_executions.get(execution_id)
        if execution:
            return {
                "id": str(execution.id),
                "state": execution.state,
                "output_data": execution.output_data,
                "completed_at": execution.completed_at.isoformat() if execution.completed_at else None,
            }
        
        # Check Redis cache (active workflows)
        cached = await self.cache.get_workflow_execution(execution_id)
        if cached:
            return {
                "id": cached.get("id"),
                "state": cached.get("state"),
                "output_data": cached.get("output_data"),
                "completed_at": cached.get("completed_at"),
            }
        
        # Check PostgreSQL (completed/historical workflows)
        if self.database:
            async with self.database.session() as session:
                repo = WorkflowRepository(session)
                wf_model = await repo.get_workflow_execution(execution_id)
                if wf_model:
                    return {
                        "id": str(wf_model.id),
                        "state": wf_model.state,
                        "output_data": wf_model.output_data,
                        "completed_at": wf_model.completed_at.isoformat() if wf_model.completed_at else None,
                    }
        
        return None
