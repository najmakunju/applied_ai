"""
Unit tests for domain models.
"""

import pytest
from uuid import uuid4

from workflow_engine.core.models import (
    DAGDefinition,
    HandlerType,
    NodeConfig,
    NodeDefinition,
    RetryConfig,
    TaskMessage,
    WorkflowDefinition,
    WorkflowExecution,
)


class TestRetryConfig:
    """Tests for RetryConfig model."""
    
    def test_default_values(self):
        """Test default configuration values."""
        config = RetryConfig()
        
        assert config.max_retries == 3
        assert config.initial_delay == 1.0
        assert config.max_delay == 60.0
        assert config.exponential_base == 2.0
        assert config.jitter is True
    
    def test_custom_values(self):
        """Test custom configuration values."""
        config = RetryConfig(
            max_retries=5,
            initial_delay=0.5,
            max_delay=30.0,
        )
        
        assert config.max_retries == 5
        assert config.initial_delay == 0.5
        assert config.max_delay == 30.0
    
    def test_validation_max_delay(self):
        """Test that max_delay must be >= initial_delay."""
        with pytest.raises(ValueError):
            RetryConfig(initial_delay=10.0, max_delay=5.0)
    
    # Edge cases
    def test_boundary_max_retries_zero(self):
        """Test max_retries at minimum boundary (0)."""
        config = RetryConfig(max_retries=0)
        assert config.max_retries == 0
    
    def test_boundary_max_retries_maximum(self):
        """Test max_retries at maximum boundary (100)."""
        config = RetryConfig(max_retries=100)
        assert config.max_retries == 100
    
    def test_boundary_max_retries_exceeds_maximum(self):
        """Test max_retries exceeding maximum (>100) is rejected."""
        with pytest.raises(ValueError):
            RetryConfig(max_retries=101)
    
    def test_boundary_max_retries_negative(self):
        """Test negative max_retries is rejected."""
        with pytest.raises(ValueError):
            RetryConfig(max_retries=-1)
    
    def test_boundary_initial_delay_minimum(self):
        """Test initial_delay at minimum boundary (0.1)."""
        config = RetryConfig(initial_delay=0.1, max_delay=60.0)
        assert config.initial_delay == 0.1
    
    def test_boundary_initial_delay_below_minimum(self):
        """Test initial_delay below minimum is rejected."""
        with pytest.raises(ValueError):
            RetryConfig(initial_delay=0.05)
    
    def test_boundary_exponential_base_minimum(self):
        """Test exponential_base at minimum (1.0)."""
        config = RetryConfig(exponential_base=1.0)
        assert config.exponential_base == 1.0
    
    def test_boundary_exponential_base_maximum(self):
        """Test exponential_base at maximum (10.0)."""
        config = RetryConfig(exponential_base=10.0)
        assert config.exponential_base == 10.0
    
    def test_boundary_exponential_base_exceeds_maximum(self):
        """Test exponential_base exceeding maximum is rejected."""
        with pytest.raises(ValueError):
            RetryConfig(exponential_base=10.1)
    
    def test_max_delay_equals_initial_delay(self):
        """Test edge case where max_delay equals initial_delay."""
        config = RetryConfig(initial_delay=5.0, max_delay=5.0)
        assert config.max_delay == config.initial_delay
    
    def test_jitter_disabled(self):
        """Test jitter can be explicitly disabled."""
        config = RetryConfig(jitter=False)
        assert config.jitter is False


class TestNodeDefinition:
    """Tests for NodeDefinition model."""
    
    def test_valid_node(self):
        """Test creating a valid node."""
        node = NodeDefinition(
            id="test_node",
            handler=HandlerType.CALL_EXTERNAL_SERVICE,
            dependencies=["input"],
            config=NodeConfig(url="http://example.com"),
        )
        
        assert node.id == "test_node"
        assert node.handler == HandlerType.CALL_EXTERNAL_SERVICE
        assert node.dependencies == ["input"]
    
    def test_invalid_node_id(self):
        """Test that invalid node IDs are rejected."""
        with pytest.raises(ValueError):
            NodeDefinition(
                id="invalid node id!",  # Contains space and special char
                handler=HandlerType.INPUT,
            )
    
    def test_duplicate_dependencies_rejected(self):
        """Test that duplicate dependencies are rejected."""
        with pytest.raises(ValueError):
            NodeDefinition(
                id="node",
                handler=HandlerType.OUTPUT,
                dependencies=["a", "b", "a"],  # Duplicate
            )
    
    def test_valid_node_id_formats(self):
        """Test various valid node ID formats."""
        valid_ids = ["node1", "my_node", "my-node", "Node_123"]
        
        for node_id in valid_ids:
            node = NodeDefinition(id=node_id, handler=HandlerType.INPUT)
            assert node.id == node_id
    
    # Edge cases
    def test_empty_dependencies_allowed(self):
        """Test that empty dependencies list is allowed (root node)."""
        node = NodeDefinition(
            id="root",
            handler=HandlerType.INPUT,
            dependencies=[],
        )
        assert node.dependencies == []
    
    def test_node_id_minimum_length(self):
        """Test node ID at minimum length (1 char)."""
        node = NodeDefinition(id="a", handler=HandlerType.INPUT)
        assert node.id == "a"
    
    def test_node_id_maximum_length(self):
        """Test node ID at maximum length (255 chars)."""
        long_id = "a" * 255
        node = NodeDefinition(id=long_id, handler=HandlerType.INPUT)
        assert node.id == long_id
    
    def test_node_id_exceeds_maximum_length(self):
        """Test node ID exceeding maximum length is rejected."""
        with pytest.raises(ValueError):
            NodeDefinition(id="a" * 256, handler=HandlerType.INPUT)
    
    def test_node_id_empty_rejected(self):
        """Test empty node ID is rejected."""
        with pytest.raises(ValueError):
            NodeDefinition(id="", handler=HandlerType.INPUT)
    
    def test_node_id_special_chars_rejected(self):
        """Test various invalid special characters in node ID."""
        invalid_ids = ["node@id", "node#id", "node$id", "node%id", "node id"]
        for invalid_id in invalid_ids:
            with pytest.raises(ValueError):
                NodeDefinition(id=invalid_id, handler=HandlerType.INPUT)
    
    def test_many_dependencies(self):
        """Test node with many dependencies."""
        deps = [f"dep_{i}" for i in range(50)]
        node = NodeDefinition(
            id="many_deps",
            handler=HandlerType.OUTPUT,
            dependencies=deps,
        )
        assert len(node.dependencies) == 50
    
    def test_default_config_when_not_provided(self):
        """Test that default NodeConfig is created when not provided."""
        node = NodeDefinition(id="node", handler=HandlerType.INPUT)
        assert node.config is not None
        assert node.config.method == "GET"
    
    def test_custom_retry_config_override(self):
        """Test that node can have custom retry config."""
        custom_retry = RetryConfig(max_retries=10, initial_delay=2.0)
        node = NodeDefinition(
            id="node",
            handler=HandlerType.CALL_EXTERNAL_SERVICE,
            retry_config=custom_retry,
        )
        assert node.retry_config.max_retries == 10
        assert node.retry_config.initial_delay == 2.0


class TestNodeConfig:
    """Tests for NodeConfig model edge cases."""
    
    def test_default_values(self):
        """Test default NodeConfig values."""
        config = NodeConfig()
        assert config.method == "GET"
        assert config.temperature == 0.7
        assert config.max_tokens == 1000
    
    def test_temperature_at_boundary_zero(self):
        """Test temperature at minimum boundary (0.0)."""
        config = NodeConfig(temperature=0.0)
        assert config.temperature == 0.0
    
    def test_temperature_at_boundary_max(self):
        """Test temperature at maximum boundary (2.0)."""
        config = NodeConfig(temperature=2.0)
        assert config.temperature == 2.0
    
    def test_temperature_exceeds_maximum(self):
        """Test temperature exceeding maximum is rejected."""
        with pytest.raises(ValueError):
            NodeConfig(temperature=2.1)
    
    def test_temperature_negative_rejected(self):
        """Test negative temperature is rejected."""
        with pytest.raises(ValueError):
            NodeConfig(temperature=-0.1)
    
    def test_max_tokens_minimum(self):
        """Test max_tokens at minimum (1)."""
        config = NodeConfig(max_tokens=1)
        assert config.max_tokens == 1
    
    def test_max_tokens_zero_rejected(self):
        """Test max_tokens of 0 is rejected."""
        with pytest.raises(ValueError):
            NodeConfig(max_tokens=0)
    
    def test_max_tokens_large_value(self):
        """Test max_tokens with large value."""
        config = NodeConfig(max_tokens=100000)
        assert config.max_tokens == 100000
    
    def test_extra_config_allowed(self):
        """Test that extra arbitrary config is allowed."""
        config = NodeConfig(extra={"custom_key": "custom_value"})
        assert config.extra["custom_key"] == "custom_value"
    
    def test_headers_dict(self):
        """Test custom headers configuration."""
        config = NodeConfig(headers={"Authorization": "Bearer token"})
        assert config.headers["Authorization"] == "Bearer token"
    
    def test_body_configuration(self):
        """Test request body configuration."""
        body = {"key": "value", "nested": {"a": 1}}
        config = NodeConfig(body=body)
        assert config.body == body


class TestDAGDefinition:
    """Tests for DAGDefinition model."""
    
    def test_get_node(self):
        """Test getting node by ID."""
        dag = DAGDefinition(
            nodes=[
                NodeDefinition(id="a", handler=HandlerType.INPUT),
                NodeDefinition(id="b", handler=HandlerType.OUTPUT),
            ]
        )
        
        node = dag.get_node("a")
        assert node is not None
        assert node.id == "a"
        
        assert dag.get_node("nonexistent") is None
    
    def test_get_root_nodes(self):
        """Test getting nodes with no dependencies."""
        dag = DAGDefinition(
            nodes=[
                NodeDefinition(id="a", handler=HandlerType.INPUT, dependencies=[]),
                NodeDefinition(id="b", handler=HandlerType.INPUT, dependencies=[]),
                NodeDefinition(id="c", handler=HandlerType.OUTPUT, dependencies=["a", "b"]),
            ]
        )
        
        roots = dag.get_root_nodes()
        
        assert len(roots) == 2
        assert {n.id for n in roots} == {"a", "b"}
    
    def test_get_leaf_nodes(self):
        """Test getting nodes that nothing depends on."""
        dag = DAGDefinition(
            nodes=[
                NodeDefinition(id="a", handler=HandlerType.INPUT),
                NodeDefinition(id="b", handler=HandlerType.OUTPUT, dependencies=["a"]),
                NodeDefinition(id="c", handler=HandlerType.OUTPUT, dependencies=["a"]),
            ]
        )
        
        leaves = dag.get_leaf_nodes()
        
        assert len(leaves) == 2
        assert {n.id for n in leaves} == {"b", "c"}
    
    # Edge cases
    def test_single_node_dag(self):
        """Test DAG with single node (both root and leaf)."""
        dag = DAGDefinition(
            nodes=[NodeDefinition(id="only", handler=HandlerType.INPUT)]
        )
        
        roots = dag.get_root_nodes()
        leaves = dag.get_leaf_nodes()
        
        assert len(roots) == 1
        assert len(leaves) == 1
        assert roots[0].id == "only"
        assert leaves[0].id == "only"
    
    def test_empty_nodes_rejected(self):
        """Test that empty nodes list is rejected."""
        with pytest.raises(ValueError):
            DAGDefinition(nodes=[])
    
    def test_all_nodes_are_roots(self):
        """Test DAG where all nodes are roots (no dependencies)."""
        dag = DAGDefinition(
            nodes=[
                NodeDefinition(id="a", handler=HandlerType.INPUT),
                NodeDefinition(id="b", handler=HandlerType.INPUT),
                NodeDefinition(id="c", handler=HandlerType.INPUT),
            ]
        )
        
        roots = dag.get_root_nodes()
        assert len(roots) == 3
    
    def test_all_nodes_are_leaves(self):
        """Test DAG where all nodes are leaves (independent)."""
        dag = DAGDefinition(
            nodes=[
                NodeDefinition(id="a", handler=HandlerType.INPUT),
                NodeDefinition(id="b", handler=HandlerType.INPUT),
                NodeDefinition(id="c", handler=HandlerType.INPUT),
            ]
        )
        
        leaves = dag.get_leaf_nodes()
        assert len(leaves) == 3
    
    def test_linear_chain_dag(self):
        """Test long linear chain DAG."""
        nodes = [NodeDefinition(id="node_0", handler=HandlerType.INPUT)]
        for i in range(1, 10):
            nodes.append(NodeDefinition(
                id=f"node_{i}",
                handler=HandlerType.OUTPUT,
                dependencies=[f"node_{i-1}"]
            ))
        
        dag = DAGDefinition(nodes=nodes)
        
        roots = dag.get_root_nodes()
        leaves = dag.get_leaf_nodes()
        
        assert len(roots) == 1
        assert roots[0].id == "node_0"
        assert len(leaves) == 1
        assert leaves[0].id == "node_9"
    
    def test_diamond_pattern_dag(self):
        """Test diamond pattern: A -> (B, C) -> D."""
        dag = DAGDefinition(
            nodes=[
                NodeDefinition(id="a", handler=HandlerType.INPUT),
                NodeDefinition(id="b", handler=HandlerType.CALL_EXTERNAL_SERVICE, dependencies=["a"]),
                NodeDefinition(id="c", handler=HandlerType.CALL_EXTERNAL_SERVICE, dependencies=["a"]),
                NodeDefinition(id="d", handler=HandlerType.OUTPUT, dependencies=["b", "c"]),
            ]
        )
        
        roots = dag.get_root_nodes()
        leaves = dag.get_leaf_nodes()
        
        assert len(roots) == 1
        assert roots[0].id == "a"
        assert len(leaves) == 1
        assert leaves[0].id == "d"


class TestWorkflowExecution:
    """Tests for WorkflowExecution model."""
    
    def test_get_completed_node_outputs(self):
        """Test getting outputs from completed nodes."""
        from workflow_engine.core.models import NodeExecution
        
        execution = WorkflowExecution(
            workflow_definition_id=uuid4(),
        )
        
        # Add node executions
        execution.node_executions["a"] = NodeExecution(
            node_id="a",
            workflow_execution_id=execution.id,
            state="COMPLETED",
            output_data={"result": 1},
        )
        execution.node_executions["b"] = NodeExecution(
            node_id="b",
            workflow_execution_id=execution.id,
            state="RUNNING",
            output_data=None,
        )
        execution.node_executions["c"] = NodeExecution(
            node_id="c",
            workflow_execution_id=execution.id,
            state="COMPLETED",
            output_data={"result": 3},
        )
        
        outputs = execution.get_completed_node_outputs()
        
        assert len(outputs) == 2
        assert outputs["a"] == {"result": 1}
        assert outputs["c"] == {"result": 3}
        assert "b" not in outputs
    
    # Edge cases
    def test_empty_node_executions(self):
        """Test workflow with no node executions."""
        execution = WorkflowExecution(workflow_definition_id=uuid4())
        
        outputs = execution.get_completed_node_outputs()
        
        assert len(outputs) == 0
    
    def test_completed_node_with_empty_output(self):
        """Test completed node with empty output data."""
        from workflow_engine.core.models import NodeExecution
        
        execution = WorkflowExecution(workflow_definition_id=uuid4())
        execution.node_executions["a"] = NodeExecution(
            node_id="a",
            workflow_execution_id=execution.id,
            state="COMPLETED",
            output_data={},  # Empty but not None
        )
        
        outputs = execution.get_completed_node_outputs()
        
        # Empty dict should not be included (falsy)
        assert "a" not in outputs
    
    def test_completed_node_with_null_output(self):
        """Test completed node with null output."""
        from workflow_engine.core.models import NodeExecution
        
        execution = WorkflowExecution(workflow_definition_id=uuid4())
        execution.node_executions["a"] = NodeExecution(
            node_id="a",
            workflow_execution_id=execution.id,
            state="COMPLETED",
            output_data=None,
        )
        
        outputs = execution.get_completed_node_outputs()
        
        assert "a" not in outputs
    
    def test_failed_node_excluded(self):
        """Test that FAILED nodes are not included in outputs."""
        from workflow_engine.core.models import NodeExecution
        
        execution = WorkflowExecution(workflow_definition_id=uuid4())
        execution.node_executions["a"] = NodeExecution(
            node_id="a",
            workflow_execution_id=execution.id,
            state="FAILED",
            output_data={"partial": "data"},
        )
        
        outputs = execution.get_completed_node_outputs()
        
        assert "a" not in outputs
    
    def test_all_terminal_states_handling(self):
        """Test handling of all terminal states."""
        from workflow_engine.core.models import NodeExecution
        
        execution = WorkflowExecution(workflow_definition_id=uuid4())
        
        states_with_output = {
            "completed": ("COMPLETED", {"result": 1}),
            "failed": ("FAILED", {"error": "test"}),
            "cancelled": ("CANCELLED", None),
        }
        
        for node_id, (state, output) in states_with_output.items():
            execution.node_executions[node_id] = NodeExecution(
                node_id=node_id,
                workflow_execution_id=execution.id,
                state=state,
                output_data=output,
            )
        
        outputs = execution.get_completed_node_outputs()
        
        # Only COMPLETED with output should be included
        assert "completed" in outputs
        assert "failed" not in outputs
        assert "cancelled" not in outputs
    
    def test_workflow_default_state_is_pending(self):
        """Test that default workflow state is PENDING."""
        execution = WorkflowExecution(workflow_definition_id=uuid4())
        assert execution.state == "PENDING"
    
    def test_workflow_with_input_params(self):
        """Test workflow with input parameters."""
        params = {"user_id": 123, "action": "process"}
        execution = WorkflowExecution(
            workflow_definition_id=uuid4(),
            input_params=params,
        )
        
        assert execution.input_params == params


class TestNodeExecution:
    """Tests for NodeExecution model edge cases."""
    
    def test_default_values(self):
        """Test default NodeExecution values."""
        from workflow_engine.core.models import NodeExecution
        
        execution = NodeExecution(
            node_id="test",
            workflow_execution_id=uuid4(),
        )
        
        assert execution.state == "PENDING"
        assert execution.attempt == 1
        assert execution.input_data == {}
        assert execution.output_data is None
    
    def test_attempt_increment(self):
        """Test attempt number tracking."""
        from workflow_engine.core.models import NodeExecution
        
        execution = NodeExecution(
            node_id="test",
            workflow_execution_id=uuid4(),
            attempt=3,
        )
        
        assert execution.attempt == 3
    
    def test_attempt_minimum_boundary(self):
        """Test attempt minimum is 1."""
        from workflow_engine.core.models import NodeExecution
        
        with pytest.raises(ValueError):
            NodeExecution(
                node_id="test",
                workflow_execution_id=uuid4(),
                attempt=0,
            )
    
    def test_error_tracking(self):
        """Test error information tracking."""
        from workflow_engine.core.models import NodeExecution
        
        execution = NodeExecution(
            node_id="test",
            workflow_execution_id=uuid4(),
            state="FAILED",
            error_message="Connection timeout",
            error_type="TimeoutError",
            error_stack_trace="Traceback...",
        )
        
        assert execution.error_message == "Connection timeout"
        assert execution.error_type == "TimeoutError"
        assert execution.error_stack_trace == "Traceback..."


class TestTaskMessage:
    """Tests for TaskMessage model."""
    
    def test_task_message_creation(self):
        """Test creating a task message."""
        msg = TaskMessage(
            node_execution_id=uuid4(),
            workflow_execution_id=uuid4(),
            node_id="test_node",
            handler=HandlerType.CALL_EXTERNAL_SERVICE,
            config=NodeConfig(url="http://example.com"),
            input_data={"key": "value"},
            idempotency_key="test-key-123",
        )
        
        assert msg.node_id == "test_node"
        assert msg.handler == HandlerType.CALL_EXTERNAL_SERVICE
        assert msg.attempt == 1
        assert msg.idempotency_key == "test-key-123"
    
    # Edge cases
    def test_task_message_retry_attempt(self):
        """Test task message with retry attempt number."""
        msg = TaskMessage(
            node_execution_id=uuid4(),
            workflow_execution_id=uuid4(),
            node_id="test_node",
            handler=HandlerType.CALL_EXTERNAL_SERVICE,
            config=NodeConfig(),
            idempotency_key="key",
            attempt=3,
        )
        
        assert msg.attempt == 3
    
    def test_task_message_with_expiry(self):
        """Test task message with expiration time."""
        from datetime import datetime, timedelta
        
        expires = datetime.utcnow() + timedelta(hours=1)
        msg = TaskMessage(
            node_execution_id=uuid4(),
            workflow_execution_id=uuid4(),
            node_id="test_node",
            handler=HandlerType.INPUT,
            config=NodeConfig(),
            idempotency_key="key",
            expires_at=expires,
        )
        
        assert msg.expires_at == expires
    
    def test_task_message_all_handler_types(self):
        """Test task message with all handler types."""
        for handler in HandlerType:
            msg = TaskMessage(
                node_execution_id=uuid4(),
                workflow_execution_id=uuid4(),
                node_id="test_node",
                handler=handler,
                config=NodeConfig(),
                idempotency_key=f"key-{handler.value}",
            )
            assert msg.handler == handler
    
    def test_task_message_empty_input_data(self):
        """Test task message with empty input data."""
        msg = TaskMessage(
            node_execution_id=uuid4(),
            workflow_execution_id=uuid4(),
            node_id="test_node",
            handler=HandlerType.INPUT,
            config=NodeConfig(),
            idempotency_key="key",
            input_data={},
        )
        
        assert msg.input_data == {}
    
    def test_task_message_large_input_data(self):
        """Test task message with large input data."""
        large_data = {f"key_{i}": f"value_{i}" * 100 for i in range(100)}
        msg = TaskMessage(
            node_execution_id=uuid4(),
            workflow_execution_id=uuid4(),
            node_id="test_node",
            handler=HandlerType.CALL_EXTERNAL_SERVICE,
            config=NodeConfig(),
            idempotency_key="key",
            input_data=large_data,
        )
        
        assert len(msg.input_data) == 100


class TestTaskResult:
    """Tests for TaskResult model edge cases."""
    
    def test_successful_task_result(self):
        """Test successful task result."""
        from workflow_engine.core.models import TaskResult
        from datetime import datetime
        
        result = TaskResult(
            task_message_id=uuid4(),
            node_execution_id=uuid4(),
            workflow_execution_id=uuid4(),
            worker_id="worker-1",
            success=True,
            output_data={"result": "success"},
            started_at=datetime.utcnow(),
        )
        
        assert result.success is True
        assert result.output_data == {"result": "success"}
        assert result.error_message is None
    
    def test_failed_task_result(self):
        """Test failed task result with error details."""
        from workflow_engine.core.models import TaskResult
        from datetime import datetime
        
        result = TaskResult(
            task_message_id=uuid4(),
            node_execution_id=uuid4(),
            workflow_execution_id=uuid4(),
            worker_id="worker-1",
            success=False,
            error_message="Connection refused",
            error_type="ConnectionError",
            error_stack_trace="Traceback...",
            is_retryable=True,
            started_at=datetime.utcnow(),
        )
        
        assert result.success is False
        assert result.error_message == "Connection refused"
        assert result.is_retryable is True
    
    def test_non_retryable_error(self):
        """Test task result with non-retryable error."""
        from workflow_engine.core.models import TaskResult
        from datetime import datetime
        
        result = TaskResult(
            task_message_id=uuid4(),
            node_execution_id=uuid4(),
            workflow_execution_id=uuid4(),
            worker_id="worker-1",
            success=False,
            error_message="Invalid API key",
            is_retryable=False,
            started_at=datetime.utcnow(),
        )
        
        assert result.is_retryable is False
    
    def test_task_result_duration(self):
        """Test task result with duration tracking."""
        from workflow_engine.core.models import TaskResult
        from datetime import datetime
        
        result = TaskResult(
            task_message_id=uuid4(),
            node_execution_id=uuid4(),
            workflow_execution_id=uuid4(),
            worker_id="worker-1",
            success=True,
            started_at=datetime.utcnow(),
            duration_ms=1500,
        )
        
        assert result.duration_ms == 1500
    
    def test_task_result_duration_minimum(self):
        """Test task result duration at minimum (0)."""
        from workflow_engine.core.models import TaskResult
        from datetime import datetime
        
        result = TaskResult(
            task_message_id=uuid4(),
            node_execution_id=uuid4(),
            workflow_execution_id=uuid4(),
            worker_id="worker-1",
            success=True,
            started_at=datetime.utcnow(),
            duration_ms=0,
        )
        
        assert result.duration_ms == 0
    
    def test_task_result_duration_negative_rejected(self):
        """Test that negative duration is rejected."""
        from workflow_engine.core.models import TaskResult
        from datetime import datetime
        
        with pytest.raises(ValueError):
            TaskResult(
                task_message_id=uuid4(),
                node_execution_id=uuid4(),
                workflow_execution_id=uuid4(),
                worker_id="worker-1",
                success=True,
                started_at=datetime.utcnow(),
                duration_ms=-1,
            )
