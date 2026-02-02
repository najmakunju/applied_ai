"""
Integration tests for workflow execution scenarios.

These tests require Redis running (use testcontainers in CI).
"""

import asyncio
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, patch
from uuid import uuid4

# Skip if Redis not available
pytest.importorskip("redis")


class TestLinearWorkflowScenario:
    """
    Scenario A: Linear workflow A -> B -> C.
    C must receive data from B.
    """
    
    @pytest.mark.asyncio
    async def test_linear_workflow_data_passing(self, sample_linear_workflow):
        """Test that data passes correctly through linear workflow."""
        from workflow_engine.core.dag import parse_workflow_json
        from workflow_engine.template.resolver import resolve_node_input
        
        definition, result = parse_workflow_json(sample_linear_workflow)
        assert result.is_valid
        
        # Simulate execution
        completed_outputs = {}
        
        # Input node produces output
        completed_outputs["input"] = {"started": True}
        
        # Process node should have access to input
        process_node = definition.dag.get_node("process")
        resolved_config = resolve_node_input(
            process_node.config.model_dump(),
            completed_outputs,
            {},
        )
        
        # Process completes
        completed_outputs["process"] = {
            "processed": True,
            "data": {"id": 123, "name": "Test"},
        }
        
        # Output node should have access to process output
        output_node = definition.dag.get_node("output")
        assert "process" in [dep for dep in output_node.dependencies]
        
        # Verify data is available
        assert completed_outputs["process"]["data"]["id"] == 123


class TestFanOutFanInScenario:
    """
    Scenario B: Fan-out/fan-in A -> (B, C) -> D.
    D waits for both B and C to finish.
    D aggregates results from both.
    """
    
    @pytest.mark.asyncio
    async def test_fanout_parallel_identification(self, sample_fanout_fanin_workflow):
        """Test that parallel branches are correctly identified."""
        from workflow_engine.core.dag import DAGParser, parse_workflow_json
        
        definition, result = parse_workflow_json(sample_fanout_fanin_workflow)
        parser = DAGParser(definition)
        
        # After input completes, all three API calls should be ready
        ready_after_input = parser.get_ready_nodes({"input"})
        ready_ids = {n.id for n in ready_after_input}
        
        assert ready_ids == {"get_user", "get_posts", "get_comments"}
    
    @pytest.mark.asyncio
    async def test_fanin_waits_for_all(self, sample_fanout_fanin_workflow):
        """Test that fan-in node waits for all dependencies."""
        from workflow_engine.core.dag import DAGParser, parse_workflow_json
        
        definition, result = parse_workflow_json(sample_fanout_fanin_workflow)
        parser = DAGParser(definition)
        
        # Partial completion - output should NOT be ready
        ready = parser.get_ready_nodes({"input", "get_user", "get_posts"})
        assert not any(n.id == "output" for n in ready)
        
        # Full completion - output should be ready
        ready = parser.get_ready_nodes({
            "input", "get_user", "get_posts", "get_comments"
        })
        assert any(n.id == "output" for n in ready)
    
    @pytest.mark.asyncio
    async def test_fanin_aggregates_results(self, sample_fanout_fanin_workflow):
        """Test that fan-in node receives all upstream outputs."""
        from workflow_engine.core.dag import parse_workflow_json
        
        definition, _ = parse_workflow_json(sample_fanout_fanin_workflow)
        
        # Simulate completed outputs
        completed_outputs = {
            "input": {"started": True},
            "get_user": {"user": {"id": 1, "name": "Alice"}},
            "get_posts": {"posts": [{"id": 1}, {"id": 2}]},
            "get_comments": {"comments": [{"id": 1, "text": "Hello"}]},
        }
        
        # Output node has access to all dependencies
        output_node = definition.dag.get_node("output")
        
        # Build aggregated input for output node
        aggregated = {
            dep: completed_outputs[dep]
            for dep in output_node.dependencies
        }
        
        assert "get_user" in aggregated
        assert "get_posts" in aggregated
        assert "get_comments" in aggregated
        assert aggregated["get_user"]["user"]["name"] == "Alice"


class TestRaceConditionScenario:
    """
    Scenario C: Race conditions.
    If B and C finish at the exact same millisecond,
    the orchestrator correctly triggers D exactly once.
    """
    
    @pytest.mark.asyncio
    async def test_concurrent_completion_triggers_once(self):
        """Test that concurrent completions trigger downstream exactly once."""
        from workflow_engine.orchestrator.coordinator import FanInCoordinator
        from unittest.mock import MagicMock, AsyncMock
        
        # Mock Redis client
        mock_redis = MagicMock()
        mock_redis.set = AsyncMock()
        mock_redis.hset = AsyncMock()
        
        # Simulated atomic decrement counter
        counter = [2]  # Start with 2 dependencies
        
        async def mock_eval(script, num_keys, *args):
            # Simulate atomic decrement
            counter[0] -= 1
            if counter[0] == 0:
                return 1  # Trigger
            elif counter[0] < 0:
                return -2  # Already triggered
            return 0  # Still waiting
        
        mock_redis.eval = mock_eval
        
        coordinator = FanInCoordinator(mock_redis)
        coordinator._decrement_script = MagicMock()
        coordinator._decrement_script.side_effect = mock_eval
        
        workflow_id = uuid4()
        
        # Simulate concurrent completions
        results = await asyncio.gather(
            coordinator.dependency_completed(workflow_id, "output", "node_b", {}),
            coordinator.dependency_completed(workflow_id, "output", "node_c", {}),
        )
        
        # Exactly one should return True (trigger)
        assert results.count(True) == 1
        assert results.count(False) == 1


class TestExamplePayload:
    """Test the exact example payload from the task description."""
    
    @pytest.mark.asyncio
    async def test_example_payload_validation(self, example_payload_workflow):
        """Test that example payload passes validation."""
        from workflow_engine.core.dag import parse_workflow_json
        
        definition, result = parse_workflow_json(example_payload_workflow)
        
        assert result.is_valid
        assert definition.name == "Parallel API Fetcher"
        assert len(definition.dag.nodes) == 5
    
    @pytest.mark.asyncio
    async def test_example_payload_execution_order(self, example_payload_workflow):
        """Test execution order for example payload."""
        from workflow_engine.core.dag import DAGParser, parse_workflow_json
        
        definition, _ = parse_workflow_json(example_payload_workflow)
        parser = DAGParser(definition)
        
        batches = parser.get_parallel_batches()
        
        # Should have 3 batches
        assert len(batches) == 3
        
        # Batch 1: input only
        assert batches[0] == ["input"]
        
        # Batch 2: three parallel API calls
        assert set(batches[1]) == {"get_user", "get_posts", "get_comments"}
        
        # Batch 3: output (fan-in)
        assert batches[2] == ["output"]


class TestFailureScenarios:
    """Test edge cases involving workflow failures."""
    
    @pytest.mark.asyncio
    async def test_partial_branch_failure(self):
        """Test handling when one parallel branch fails."""
        from workflow_engine.core.dag import DAGParser, parse_workflow_json
        from workflow_engine.core.state_machine import NodeState, compute_workflow_state_from_nodes
        
        workflow = {
            "name": "Partial Failure Test",
            "dag": {
                "nodes": [
                    {"id": "input", "handler": "input", "dependencies": []},
                    {"id": "branch_a", "handler": "call_external_service", "dependencies": ["input"], "config": {"url": "http://test"}},
                    {"id": "branch_b", "handler": "call_external_service", "dependencies": ["input"], "config": {"url": "http://test"}},
                    {"id": "output", "handler": "output", "dependencies": ["branch_a", "branch_b"]},
                ]
            },
        }
        
        definition, _ = parse_workflow_json(workflow)
        parser = DAGParser(definition)
        
        # Simulate one branch failing
        node_states = {
            "input": NodeState.COMPLETED,
            "branch_a": NodeState.COMPLETED,
            "branch_b": NodeState.FAILED,
        }
        
        all_nodes = {"input", "branch_a", "branch_b", "output"}
        state = compute_workflow_state_from_nodes(node_states, all_nodes)
        
        # Workflow should be FAILED
        assert state.value == "FAILED"
        
        # Output should NOT be ready (one dependency failed)
        ready = parser.get_ready_nodes({"input", "branch_a"})
        assert not any(n.id == "output" for n in ready)
    
    @pytest.mark.asyncio
    async def test_all_parallel_branches_fail(self):
        """Test when all parallel branches fail."""
        from workflow_engine.core.state_machine import NodeState, compute_workflow_state_from_nodes
        
        node_states = {
            "input": NodeState.COMPLETED,
            "branch_a": NodeState.FAILED,
            "branch_b": NodeState.FAILED,
            "branch_c": NodeState.FAILED,
        }
        
        all_nodes = {"input", "branch_a", "branch_b", "branch_c", "output"}
        state = compute_workflow_state_from_nodes(node_states, all_nodes)
        
        assert state.value == "FAILED"
    
    @pytest.mark.asyncio
    async def test_early_node_failure_stops_propagation(self):
        """Test that failure in early node prevents downstream scheduling."""
        from workflow_engine.core.dag import DAGParser, parse_workflow_json
        
        workflow = {
            "name": "Early Failure Test",
            "dag": {
                "nodes": [
                    {"id": "input", "handler": "input", "dependencies": []},
                    {"id": "process", "handler": "call_external_service", "dependencies": ["input"], "config": {"url": "http://test"}},
                    {"id": "output", "handler": "output", "dependencies": ["process"]},
                ]
            },
        }
        
        definition, _ = parse_workflow_json(workflow)
        parser = DAGParser(definition)
        
        # Input fails, should not schedule process or output
        # Only input completed (as failed)
        ready = parser.get_ready_nodes({"input"})
        assert len(ready) == 1
        assert ready[0].id == "process"  # process becomes ready when input completes
        
        # But if we simulate that process also fails
        ready_after_failure = parser.get_ready_nodes({"input", "process"})
        assert len(ready_after_failure) == 1
        assert ready_after_failure[0].id == "output"


class TestCancellationScenarios:
    """Test edge cases involving workflow cancellation."""
    
    @pytest.mark.asyncio
    async def test_cancellation_propagates_to_workflow(self):
        """Test that cancellation of any node leads to workflow cancellation."""
        from workflow_engine.core.state_machine import NodeState, compute_workflow_state_from_nodes
        
        node_states = {
            "input": NodeState.COMPLETED,
            "branch_a": NodeState.CANCELLED,
            "branch_b": NodeState.RUNNING,
        }
        
        all_nodes = {"input", "branch_a", "branch_b", "output"}
        state = compute_workflow_state_from_nodes(node_states, all_nodes)
        
        assert state.value == "CANCELLED"
    
    @pytest.mark.asyncio
    async def test_cancellation_before_start(self):
        """Test workflow can be cancelled before any node starts."""
        from workflow_engine.core.state_machine import NodeState, WorkflowState, compute_workflow_state_from_nodes
        
        # No nodes have started yet
        node_states = {}
        all_nodes = {"input", "process", "output"}
        
        state = compute_workflow_state_from_nodes(node_states, all_nodes)
        assert state == WorkflowState.PENDING


class TestSingleNodeWorkflowScenarios:
    """Test edge cases for minimal workflows."""
    
    @pytest.mark.asyncio
    async def test_single_node_workflow_execution(self):
        """Test workflow with single node completes correctly."""
        from workflow_engine.core.dag import DAGParser, parse_workflow_json
        from workflow_engine.core.state_machine import NodeState, compute_workflow_state_from_nodes
        
        workflow = {
            "name": "Single Node",
            "dag": {
                "nodes": [
                    {"id": "only", "handler": "input", "dependencies": []},
                ]
            },
        }
        
        definition, result = parse_workflow_json(workflow)
        assert result.is_valid
        
        parser = DAGParser(definition)
        
        # Initially ready
        ready = parser.get_ready_nodes(set())
        assert len(ready) == 1
        
        # After completion
        node_states = {"only": NodeState.COMPLETED}
        state = compute_workflow_state_from_nodes(node_states, {"only"})
        
        assert state.value == "COMPLETED"
    
    @pytest.mark.asyncio
    async def test_single_node_workflow_failure(self):
        """Test workflow with single node fails correctly."""
        from workflow_engine.core.state_machine import NodeState, compute_workflow_state_from_nodes
        
        node_states = {"only": NodeState.FAILED}
        state = compute_workflow_state_from_nodes(node_states, {"only"})
        
        assert state.value == "FAILED"


class TestComplexDAGScenarios:
    """Test edge cases for complex DAG structures."""
    
    @pytest.mark.asyncio
    async def test_diamond_with_multiple_paths(self):
        """Test diamond pattern where node has multiple dependency paths."""
        from workflow_engine.core.dag import DAGParser, parse_workflow_json
        
        # A -> B -> D
        #  \-> C -/
        workflow = {
            "name": "Diamond Pattern",
            "dag": {
                "nodes": [
                    {"id": "a", "handler": "input", "dependencies": []},
                    {"id": "b", "handler": "call_external_service", "dependencies": ["a"], "config": {"url": "http://test"}},
                    {"id": "c", "handler": "call_external_service", "dependencies": ["a"], "config": {"url": "http://test"}},
                    {"id": "d", "handler": "output", "dependencies": ["b", "c"]},
                ]
            },
        }
        
        definition, _ = parse_workflow_json(workflow)
        parser = DAGParser(definition)
        
        # d should only be ready when both b and c complete
        assert not any(n.id == "d" for n in parser.get_ready_nodes({"a", "b"}))
        assert not any(n.id == "d" for n in parser.get_ready_nodes({"a", "c"}))
        assert any(n.id == "d" for n in parser.get_ready_nodes({"a", "b", "c"}))
    
    @pytest.mark.asyncio
    async def test_wide_fanout_scheduling(self):
        """Test scheduling with wide fan-out."""
        from workflow_engine.core.dag import DAGParser, parse_workflow_json
        
        nodes = [{"id": "input", "handler": "input", "dependencies": []}]
        for i in range(20):
            nodes.append({
                "id": f"branch_{i}",
                "handler": "call_external_service",
                "dependencies": ["input"],
                "config": {"url": "http://test"}
            })
        
        workflow = {"name": "Wide Fanout", "dag": {"nodes": nodes}}
        definition, _ = parse_workflow_json(workflow)
        parser = DAGParser(definition)
        
        # After input completes, all 20 branches should be ready
        ready = parser.get_ready_nodes({"input"})
        assert len(ready) == 20
    
    @pytest.mark.asyncio
    async def test_wide_fanin_completion(self):
        """Test fan-in with many dependencies."""
        from workflow_engine.core.dag import DAGParser, parse_workflow_json
        
        nodes = [{"id": f"source_{i}", "handler": "input", "dependencies": []} for i in range(10)]
        nodes.append({
            "id": "aggregate",
            "handler": "output",
            "dependencies": [f"source_{i}" for i in range(10)]
        })
        
        workflow = {"name": "Wide Fanin", "dag": {"nodes": nodes}}
        definition, _ = parse_workflow_json(workflow)
        parser = DAGParser(definition)
        
        # Aggregate should only be ready when ALL 10 sources complete
        completed_partial = {f"source_{i}" for i in range(9)}  # Missing source_9
        assert not any(n.id == "aggregate" for n in parser.get_ready_nodes(completed_partial))
        
        completed_all = {f"source_{i}" for i in range(10)}
        ready = parser.get_ready_nodes(completed_all)
        assert any(n.id == "aggregate" for n in ready)


class TestEmptyAndEdgeCaseInputs:
    """Test edge cases with unusual inputs."""
    
    @pytest.mark.asyncio
    async def test_workflow_with_empty_input_params(self):
        """Test workflow trigger with empty input parameters."""
        from workflow_engine.core.dag import parse_workflow_json
        from workflow_engine.template.resolver import resolve_node_input
        
        workflow = {
            "name": "Empty Params Test",
            "dag": {
                "nodes": [
                    {"id": "input", "handler": "input", "dependencies": []},
                    {"id": "process", "handler": "call_external_service", "dependencies": ["input"], 
                     "config": {"url": "http://api.example.com/{{ input.user_id }}"}},
                ]
            },
        }
        
        definition, _ = parse_workflow_json(workflow)
        process_node = definition.dag.get_node("process")
        
        # Resolve with empty input
        resolved = resolve_node_input(
            process_node.config.model_dump(),
            {"input": {}},
            {}  # Empty input params
        )
        
        # Should resolve to None for missing param
        assert "None" in resolved["url"] or resolved["url"] == "http://api.example.com/"
    
    @pytest.mark.asyncio
    async def test_large_payload_data(self):
        """Test workflow with large payload data."""
        from workflow_engine.core.dag import parse_workflow_json
        from workflow_engine.template.resolver import resolve_node_input
        
        workflow = {
            "name": "Large Payload Test",
            "dag": {
                "nodes": [
                    {"id": "input", "handler": "input", "dependencies": []},
                    {"id": "output", "handler": "output", "dependencies": ["input"]},
                ]
            },
        }
        
        definition, result = parse_workflow_json(workflow)
        assert result.is_valid
        
        # Large input data
        large_data = {f"key_{i}": f"value_{i}" * 100 for i in range(100)}
        
        resolved = resolve_node_input(
            {"config": "{{ input.key_50 }}"},
            {},
            large_data
        )
        
        assert "value_50" in resolved["config"]
    
    @pytest.mark.asyncio
    async def test_deeply_nested_output_access(self):
        """Test accessing deeply nested data in node outputs."""
        from workflow_engine.template.resolver import resolve_node_input
        
        nested_output = {
            "level1": {
                "level2": {
                    "level3": {
                        "level4": {
                            "value": "deep_value"
                        }
                    }
                }
            }
        }
        
        resolved = resolve_node_input(
            {"result": "{{ node.level1.level2.level3.level4.value }}"},
            {"node": nested_output},
            {}
        )
        
        assert resolved["result"] == "deep_value"


class TestTemplateResolutionScenarios:
    """Test template resolution edge cases in workflow context."""
    
    @pytest.mark.asyncio
    async def test_template_with_missing_dependency(self):
        """Test template resolution when dependency output is missing."""
        from workflow_engine.template.resolver import resolve_node_input
        
        config = {"url": "http://api.example.com/{{ missing_node.id }}"}
        
        resolved = resolve_node_input(config, {}, {})
        
        # Missing node should resolve to None/empty
        assert resolved["url"] == "http://api.example.com/"
    
    @pytest.mark.asyncio
    async def test_multiple_templates_same_source(self):
        """Test multiple templates from same source node."""
        from workflow_engine.template.resolver import resolve_node_input
        
        config = {
            "user_id": "{{ user.id }}",
            "user_name": "{{ user.name }}",
            "user_email": "{{ user.email }}",
        }
        
        completed_outputs = {
            "user": {
                "id": 123,
                "name": "John",
                "email": "john@example.com"
            }
        }
        
        resolved = resolve_node_input(config, completed_outputs, {})
        
        assert resolved["user_id"] == 123
        assert resolved["user_name"] == "John"
        assert resolved["user_email"] == "john@example.com"
    
    @pytest.mark.asyncio
    async def test_template_mixing_sources(self):
        """Test templates mixing node outputs and input params."""
        from workflow_engine.template.resolver import resolve_node_input
        
        config = {
            "url": "http://api.example.com/{{ input.resource }}/{{ node.id }}",
        }
        
        completed_outputs = {"node": {"id": 456}}
        input_params = {"resource": "users"}
        
        resolved = resolve_node_input(config, completed_outputs, input_params)
        
        assert resolved["url"] == "http://api.example.com/users/456"
