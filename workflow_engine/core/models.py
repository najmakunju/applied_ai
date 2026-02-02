"""
Domain models for the workflow orchestration engine.

All models use Pydantic for validation and serialization with full Python 3.10+ type hints.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, field_validator, model_validator


class HandlerType(str, Enum):
    """Supported node handler types."""
    
    INPUT = "input"
    OUTPUT = "output"
    CALL_EXTERNAL_SERVICE = "call_external_service"
    LLM_SERVICE = "llm_service"


class RetryConfig(BaseModel):
    """Configuration for retry behavior."""
    
    max_retries: int = Field(default=3, ge=0, le=100, description="Maximum retry attempts")
    initial_delay: float = Field(default=1.0, ge=0.1, description="Initial delay in seconds")
    max_delay: float = Field(default=60.0, ge=1.0, description="Maximum delay in seconds")
    exponential_base: float = Field(default=2.0, ge=1.0, le=10.0, description="Backoff base")
    jitter: bool = Field(default=True, description="Add randomized jitter")
    
    @model_validator(mode="after")
    def validate_delays(self) -> "RetryConfig":
        """Ensure max_delay is greater than initial_delay."""
        if self.max_delay < self.initial_delay:
            raise ValueError("max_delay must be >= initial_delay")
        return self


class NodeConfig(BaseModel):
    """Configuration specific to a node handler."""
    
    # External service config
    url: Optional[str] = Field(default=None, description="URL for external service calls")
    method: str = Field(default="GET", description="HTTP method for external calls")
    headers: dict[str, str] = Field(default_factory=dict, description="HTTP headers")
    body: Optional[dict[str, Any]] = Field(default=None, description="Request body")
    
    # LLM service config
    prompt: Optional[str] = Field(default=None, description="Prompt template for LLM")
    model: Optional[str] = Field(default=None, description="LLM model identifier")
    temperature: float = Field(default=0.7, ge=0.0, le=2.0, description="LLM temperature")
    max_tokens: int = Field(default=1000, ge=1, description="Max tokens for LLM response")
    
    # Generic config - allows arbitrary key-value pairs
    extra: dict[str, Any] = Field(default_factory=dict, description="Additional config")
    
    class Config:
        extra = "allow"


class NodeDefinition(BaseModel):
    """Definition of a single node in the workflow DAG."""
    
    id: str = Field(..., min_length=1, max_length=255, description="Unique node identifier")
    handler: HandlerType = Field(..., description="Handler type for this node")
    dependencies: list[str] = Field(default_factory=list, description="List of dependency node IDs")
    config: NodeConfig = Field(default_factory=NodeConfig, description="Node-specific configuration")
    retry_config: Optional[RetryConfig] = Field(default=None, description="Override retry settings")

   
    @field_validator("id")
    @classmethod
    def validate_id(cls, v: str) -> str:
        """Validate node ID format."""
        if not v.replace("_", "").replace("-", "").isalnum():
            raise ValueError("Node ID must be alphanumeric with underscores/hyphens")
        return v
    
    @field_validator("dependencies")
    @classmethod
    def validate_dependencies(cls, v: list[str]) -> list[str]:
        """Validate dependencies list."""
        if len(v) != len(set(v)):
            raise ValueError("Duplicate dependencies not allowed")
        return v


class DAGDefinition(BaseModel):
    """Definition of a Directed Acyclic Graph of nodes."""
    
    nodes: list[NodeDefinition] = Field(..., min_length=1, description="List of node definitions")
    
    @field_validator("nodes")
    @classmethod
    def validate_unique_node_ids(cls, v: list[NodeDefinition]) -> list[NodeDefinition]:
        """Ensure all node IDs are unique."""
        ids = [node.id for node in v]
        if len(ids) != len(set(ids)):
            duplicates = [x for x in ids if ids.count(x) > 1]
            raise ValueError(f"Duplicate node IDs found: {set(duplicates)}")
        return v
    
    def get_node(self, node_id: str) -> Optional[NodeDefinition]:
        """Get node by ID."""
        for node in self.nodes:
            if node.id == node_id:
                return node
        return None
    
    def get_root_nodes(self) -> list[NodeDefinition]:
        """Get nodes with no dependencies (entry points)."""
        return [node for node in self.nodes if not node.dependencies]
    
    def get_leaf_nodes(self) -> list[NodeDefinition]:
        """Get nodes that no other nodes depend on (exit points)."""
        all_deps = set()
        for node in self.nodes:
            all_deps.update(node.dependencies)
        return [node for node in self.nodes if node.id not in all_deps]


class WorkflowDefinition(BaseModel):
    """Complete workflow definition. Each workflow has a unique name."""
    
    id: UUID = Field(default_factory=uuid4, description="Unique workflow definition ID")
    name: str = Field(..., min_length=1, max_length=255, description="Unique workflow name")
    dag: DAGDefinition = Field(..., description="DAG definition")
    
    # Default configurations
    default_retry_config: RetryConfig = Field(
        default_factory=RetryConfig,
        description="Default retry configuration for all nodes"
    )
    
    # Metadata
    description: Optional[str] = Field(default=None, description="Optional workflow description")
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class NodeExecution(BaseModel):
    """Runtime state of a node execution."""
    
    id: UUID = Field(default_factory=uuid4, description="Unique execution ID")
    node_id: str = Field(..., description="Reference to node definition")
    workflow_execution_id: UUID = Field(..., description="Parent workflow execution")
    
    # State
    state: str = Field(default="PENDING", description="Current execution state")
    
    # Timing
    created_at: datetime = Field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = Field(default=None)
    completed_at: Optional[datetime] = Field(default=None)
    
    # Execution details
    attempt: int = Field(default=1, ge=1, description="Current attempt number")
    worker_id: Optional[str] = Field(default=None, description="Assigned worker")
    
    # Input/Output
    input_data: dict[str, Any] = Field(default_factory=dict)
    output_data: Optional[dict[str, Any]] = Field(default=None)
    
    # Error tracking
    error_message: Optional[str] = Field(default=None)
    error_type: Optional[str] = Field(default=None)
    error_stack_trace: Optional[str] = Field(default=None)


class WorkflowExecution(BaseModel):
    """Runtime state of a workflow execution."""
    
    id: UUID = Field(default_factory=uuid4, description="Unique execution ID")
    workflow_definition_id: UUID = Field(..., description="Reference to workflow definition")
    
    # State
    state: str = Field(default="PENDING", description="Current execution state")
    
    # Timing
    created_at: datetime = Field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = Field(default=None)
    completed_at: Optional[datetime] = Field(default=None)
    
    # Input/Output
    input_params: dict[str, Any] = Field(default_factory=dict)
    output_data: Optional[dict[str, Any]] = Field(default=None)
    
    # Node states - maps node_id to NodeExecution
    node_executions: dict[str, NodeExecution] = Field(default_factory=dict)
    
    # Error tracking
    error_message: Optional[str] = Field(default=None)
    
    def get_completed_node_outputs(self) -> dict[str, Any]:
        """Get outputs from all completed nodes."""
        outputs = {}
        for node_id, execution in self.node_executions.items():
            if execution.state == "COMPLETED" and execution.output_data:
                outputs[node_id] = execution.output_data
        return outputs


class TaskMessage(BaseModel):
    """Message sent to workers for task execution."""
    
    id: UUID = Field(default_factory=uuid4, description="Unique message ID")
    node_execution_id: UUID = Field(..., description="Node execution to process")
    workflow_execution_id: UUID = Field(..., description="Parent workflow execution")
    
    # Node details
    node_id: str = Field(..., description="Node identifier")
    handler: HandlerType = Field(..., description="Handler type")
    config: NodeConfig = Field(..., description="Node configuration")
    
    # Input data (with resolved templates)
    input_data: dict[str, Any] = Field(default_factory=dict)
    
    # Retry info
    attempt: int = Field(default=1, ge=1)
    retry_config: RetryConfig = Field(default_factory=RetryConfig)
    
    # Idempotency
    idempotency_key: str = Field(..., description="Key for idempotent execution")
    
    # Timing
    created_at: datetime = Field(default_factory=datetime.utcnow)
    expires_at: Optional[datetime] = Field(default=None)


class TaskResult(BaseModel):
    """Result of a task execution from a worker."""
    
    id: UUID = Field(default_factory=uuid4)
    task_message_id: UUID = Field(..., description="Original task message ID")
    node_execution_id: UUID = Field(..., description="Node execution ID")
    workflow_execution_id: UUID = Field(..., description="Parent workflow execution")
    worker_id: str = Field(..., description="Worker that processed the task")
    
    # Result
    success: bool = Field(..., description="Whether execution succeeded")
    output_data: Optional[dict[str, Any]] = Field(default=None)
    
    # Error details
    error_message: Optional[str] = Field(default=None)
    error_type: Optional[str] = Field(default=None)
    error_stack_trace: Optional[str] = Field(default=None)
    is_retryable: bool = Field(default=True, description="Whether error is retryable")
    
    # Timing
    started_at: datetime = Field(...)
    completed_at: datetime = Field(default_factory=datetime.utcnow)
    duration_ms: int = Field(default=0, ge=0, description="Execution duration in milliseconds")
