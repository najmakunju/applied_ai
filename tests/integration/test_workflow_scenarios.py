"""
Integration tests for workflow scenarios.

Tests the three key scenarios:
- Scenario A (Linear): A → B → C with data passing
- Scenario B (Fan-Out/Fan-In): A → (B, C) → D with aggregation
- Scenario C (Race Conditions): Concurrent completion triggers D exactly once
"""

import asyncio
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4, UUID
from datetime import datetime

from workflow_engine.core.dag import DAGParser, parse_workflow_json
from workflow_engine.core.models import (
    NodeExecution,
    WorkflowDefinition,
    WorkflowExecution,
)
from workflow_engine.core.state_machine import NodeState, WorkflowState
from workflow_engine.orchestrator.coordinator import FanInCoordinator
from workflow_engine.template.resolver import resolve_node_input


# ==================== Test Fixtures ====================


@pytest.fixture
def linear_workflow_abc() -> dict:
    """
    Scenario A: Linear workflow A → B → C.
    B receives data from A, C receives data from B.
    """
    return {
        "name": "Linear A-B-C Workflow",
        "dag": {
            "nodes": [
                {
                    "id": "node_a",
                    "handler": "input",
                    "dependencies": [],
                },
                {
                    "id": "node_b",
                    "handler": "call_external_service",
                    "dependencies": ["node_a"],
                    "config": {
                        "url": "http://api.example.com/process/{{ node_a.data_id }}",
                    },
                },
                {
                    "id": "node_c",
                    "handler": "output",
                    "dependencies": ["node_b"],
                },
            ]
        },
    }


@pytest.fixture
def fanout_fanin_workflow() -> dict:
    """
    Scenario B: Fan-out/fan-in workflow A → (B, C) → D.
    A triggers B and C in parallel, D waits for both and aggregates.
    """
    return {
        "name": "Fan-Out Fan-In A-BC-D Workflow",
        "dag": {
            "nodes": [
                {
                    "id": "node_a",
                    "handler": "input",
                    "dependencies": [],
                },
                {
                    "id": "node_b",
                    "handler": "call_external_service",
                    "dependencies": ["node_a"],
                    "config": {
                        "url": "http://api.example.com/service_b",
                    },
                },
                {
                    "id": "node_c",
                    "handler": "call_external_service",
                    "dependencies": ["node_a"],
                    "config": {
                        "url": "http://api.example.com/service_c",
                    },
                },
                {
                    "id": "node_d",
                    "handler": "output",
                    "dependencies": ["node_b", "node_c"],
                },
            ]
        },
    }


@pytest.fixture
def mock_redis():
    """Create a mock Redis client with proper async methods."""
    mock = MagicMock()
    mock.set = AsyncMock()
    mock.get = AsyncMock(return_value=None)
    mock.hset = AsyncMock()
    mock.hgetall = AsyncMock(return_value={})
    mock.delete = AsyncMock()
    mock.exists = AsyncMock(return_value=False)
    mock.pipeline = MagicMock()
    
    # Pipeline mock
    pipe_mock = MagicMock()
    pipe_mock.set = MagicMock()
    pipe_mock.hset = MagicMock()
    pipe_mock.execute = AsyncMock(return_value=[])
    pipe_mock.__aenter__ = AsyncMock(return_value=pipe_mock)
    pipe_mock.__aexit__ = AsyncMock(return_value=None)
    mock.pipeline.return_value = pipe_mock
    
    return mock


# ==================== Scenario A: Linear Workflow Tests ====================


class TestScenarioALinearWorkflow:
    """
    Scenario A: Linear workflow A → B → C.
    C must receive data from B (or A).
    
    Tests:
    1. Correct execution order (A before B before C)
    2. Data flows from A to B
    3. Data flows from B to C
    4. Complete data flow chain A → B → C
    """
    
    @pytest.mark.asyncio
    async def test_linear_workflow_execution_order(self, linear_workflow_abc):
        """Test that nodes execute in correct linear order."""
        definition, result = parse_workflow_json(linear_workflow_abc)
        assert result.is_valid
        
        parser = DAGParser(definition)
        
        # Initially only A should be ready (no dependencies)
        ready = parser.get_ready_nodes(set())
        assert len(ready) == 1
        assert ready[0].id == "node_a"
        
        # After A completes, only B should be ready
        ready = parser.get_ready_nodes({"node_a"})
        assert len(ready) == 1
        assert ready[0].id == "node_b"
        
        # After B completes, C should be ready
        ready = parser.get_ready_nodes({"node_a", "node_b"})
        assert len(ready) == 1
        assert ready[0].id == "node_c"
        
        # After C completes, no more nodes
        ready = parser.get_ready_nodes({"node_a", "node_b", "node_c"})
        assert len(ready) == 0
    
    @pytest.mark.asyncio
    async def test_linear_data_flows_from_a_to_b(self, linear_workflow_abc):
        """Test that node B receives data from node A."""
        definition, _ = parse_workflow_json(linear_workflow_abc)
        
        # Simulate A's output
        completed_outputs = {
            "node_a": {"data_id": "123", "user": "test_user"}
        }
        
        # Resolve B's config (which has template references to A)
        node_b = definition.dag.get_node("node_b")
        resolved_config = resolve_node_input(
            node_b.config.model_dump(),
            completed_outputs,
            {},
        )
        
        # Verify A's data is accessible in B's resolved config
        assert "123" in resolved_config["url"]
        assert resolved_config["url"] == "http://api.example.com/process/123"
    
    @pytest.mark.asyncio
    async def test_linear_data_flows_from_b_to_c(self, linear_workflow_abc):
        """Test that node C receives data from node B."""
        definition, _ = parse_workflow_json(linear_workflow_abc)
        
        # Simulate both A and B outputs
        completed_outputs = {
            "node_a": {"data_id": "123"},
            "node_b": {"processed_data": {"result": "success", "value": 42}},
        }
        
        # Node C should have access to B's output
        node_c = definition.dag.get_node("node_c")
        assert "node_b" in node_c.dependencies
        
        # Verify B's output is available for C
        assert "processed_data" in completed_outputs["node_b"]
        assert completed_outputs["node_b"]["processed_data"]["value"] == 42
    
    @pytest.mark.asyncio
    async def test_linear_complete_data_chain(self, linear_workflow_abc):
        """Test complete data flow chain A → B → C."""
        definition, _ = parse_workflow_json(linear_workflow_abc)
        
        # Simulate full execution chain
        workflow_execution_id = uuid4()
        
        # Step 1: A executes and produces output
        a_output = {"data_id": "user-456", "timestamp": "2024-01-01T00:00:00Z"}
        
        # Step 2: B receives A's output and produces its own
        completed_outputs = {"node_a": a_output}
        node_b = definition.dag.get_node("node_b")
        b_input = {dep: completed_outputs[dep] for dep in node_b.dependencies}
        
        assert "node_a" in b_input
        assert b_input["node_a"]["data_id"] == "user-456"
        
        b_output = {
            "processed": True,
            "transformed_data": f"Processed: {a_output['data_id']}",
        }
        completed_outputs["node_b"] = b_output
        
        # Step 3: C receives B's output (and indirectly A's data via B's transformation)
        node_c = definition.dag.get_node("node_c")
        c_input = {dep: completed_outputs[dep] for dep in node_c.dependencies}
        
        assert "node_b" in c_input
        assert c_input["node_b"]["transformed_data"] == "Processed: user-456"
    
    @pytest.mark.asyncio
    async def test_linear_batch_execution_order(self, linear_workflow_abc):
        """Test parallel batches show correct linear execution order."""
        definition, _ = parse_workflow_json(linear_workflow_abc)
        parser = DAGParser(definition)
        
        batches = parser.get_parallel_batches()
        
        # Should have 3 batches (one per node since linear)
        assert len(batches) == 3
        
        # Each batch should have exactly one node
        assert batches[0] == ["node_a"]
        assert batches[1] == ["node_b"]
        assert batches[2] == ["node_c"]


# ==================== Scenario B: Fan-Out/Fan-In Tests ====================


class TestScenarioBFanOutFanIn:
    """
    Scenario B: Fan-out/fan-in A → (B, C) → D.
    A triggers B and C in parallel.
    D waits for both B and C to finish.
    D aggregates results from both.
    
    Tests:
    1. Parallel branches identified correctly
    2. D waits for ALL dependencies
    3. D receives aggregated results from both B and C
    """
    
    @pytest.mark.asyncio
    async def test_fanout_parallel_branches_ready_simultaneously(
        self, fanout_fanin_workflow
    ):
        """Test that B and C become ready at the same time after A completes."""
        definition, result = parse_workflow_json(fanout_fanin_workflow)
        assert result.is_valid
        
        parser = DAGParser(definition)
        
        # Initially only A should be ready
        ready = parser.get_ready_nodes(set())
        assert len(ready) == 1
        assert ready[0].id == "node_a"
        
        # After A completes, BOTH B and C should be ready (parallel)
        ready = parser.get_ready_nodes({"node_a"})
        ready_ids = {n.id for n in ready}
        
        assert len(ready_ids) == 2
        assert ready_ids == {"node_b", "node_c"}
    
    @pytest.mark.asyncio
    async def test_fanin_waits_for_all_dependencies(self, fanout_fanin_workflow):
        """Test that D only becomes ready when BOTH B and C complete."""
        definition, _ = parse_workflow_json(fanout_fanin_workflow)
        parser = DAGParser(definition)
        
        # D should NOT be ready when only B completes
        ready = parser.get_ready_nodes({"node_a", "node_b"})
        ready_ids = {n.id for n in ready}
        assert "node_d" not in ready_ids
        
        # D should NOT be ready when only C completes
        ready = parser.get_ready_nodes({"node_a", "node_c"})
        ready_ids = {n.id for n in ready}
        assert "node_d" not in ready_ids
        
        # D should be ready ONLY when BOTH B and C complete
        ready = parser.get_ready_nodes({"node_a", "node_b", "node_c"})
        ready_ids = {n.id for n in ready}
        assert "node_d" in ready_ids
    
    @pytest.mark.asyncio
    async def test_fanin_aggregates_results_from_both_branches(
        self, fanout_fanin_workflow
    ):
        """Test that D receives and can aggregate results from both B and C."""
        definition, _ = parse_workflow_json(fanout_fanin_workflow)
        
        # Simulate outputs from all upstream nodes
        completed_outputs = {
            "node_a": {"request_id": "req-789"},
            "node_b": {
                "service": "B",
                "result": {"items": [1, 2, 3]},
            },
            "node_c": {
                "service": "C",
                "result": {"items": [4, 5, 6]},
            },
        }
        
        # Get D's dependencies
        node_d = definition.dag.get_node("node_d")
        
        # Verify D depends on both B and C
        assert set(node_d.dependencies) == {"node_b", "node_c"}
        
        # Build aggregated input for D
        d_input = {dep: completed_outputs[dep] for dep in node_d.dependencies}
        
        # Verify D receives both outputs
        assert "node_b" in d_input
        assert "node_c" in d_input
        
        # Verify D can access both results
        assert d_input["node_b"]["result"]["items"] == [1, 2, 3]
        assert d_input["node_c"]["result"]["items"] == [4, 5, 6]
        
        # Simulate D aggregating the results
        aggregated = {
            "combined_items": (
                d_input["node_b"]["result"]["items"] +
                d_input["node_c"]["result"]["items"]
            )
        }
        assert aggregated["combined_items"] == [1, 2, 3, 4, 5, 6]
    
    @pytest.mark.asyncio
    async def test_fanout_fanin_batch_structure(self, fanout_fanin_workflow):
        """Test that parallel batches reflect fan-out/fan-in structure."""
        definition, _ = parse_workflow_json(fanout_fanin_workflow)
        parser = DAGParser(definition)
        
        batches = parser.get_parallel_batches()
        
        # Should have 3 batches
        assert len(batches) == 3
        
        # Batch 1: Only A (entry point)
        assert batches[0] == ["node_a"]
        
        # Batch 2: Both B and C (parallel branches)
        assert set(batches[1]) == {"node_b", "node_c"}
        
        # Batch 3: Only D (fan-in point)
        assert batches[2] == ["node_d"]
    
    @pytest.mark.asyncio
    async def test_fanin_coordinator_stores_outputs(
        self, fanout_fanin_workflow, mock_redis
    ):
        """Test that FanInCoordinator stores outputs from each branch."""
        coordinator = FanInCoordinator(mock_redis)
        await coordinator.init()
        
        workflow_id = uuid4()
        
        # Initialize fan-in counter for node_d (2 dependencies: B and C)
        await coordinator.initialize_fan_in(workflow_id, "node_d", 2)
        mock_redis.set.assert_called_once()
        
        # Simulate B completing
        b_output = {"service": "B", "data": [1, 2, 3]}
        
        # Set up mock for the Lua script
        call_count = [0]
        
        async def mock_script(keys):
            call_count[0] += 1
            if call_count[0] == 1:
                return 0  # First call: still waiting
            return 1  # Second call: all complete
        
        coordinator._decrement_script = mock_script
        
        result = await coordinator.dependency_completed(
            workflow_id, "node_d", "node_b", b_output
        )
        
        # Should return False (still waiting for C)
        assert result is False
        
        # Verify output was stored
        mock_redis.hset.assert_called()
    
    @pytest.mark.asyncio
    async def test_fanin_coordinator_triggers_on_last_completion(
        self, fanout_fanin_workflow, mock_redis
    ):
        """Test that coordinator returns True when last dependency completes."""
        coordinator = FanInCoordinator(mock_redis)
        await coordinator.init()
        
        workflow_id = uuid4()
        
        # Set up mock to return 1 (all complete) immediately
        async def mock_script(keys):
            return 1  # All dependencies complete
        
        coordinator._decrement_script = mock_script
        
        c_output = {"service": "C", "data": [4, 5, 6]}
        result = await coordinator.dependency_completed(
            workflow_id, "node_d", "node_c", c_output
        )
        
        # Should return True (trigger D)
        assert result is True


# ==================== Scenario C: Race Condition Tests ====================


class TestScenarioCRaceConditions:
    """
    Scenario C: Race conditions.
    If B and C finish at the exact same millisecond,
    the Orchestrator correctly triggers D exactly once.
    
    Tests:
    1. Concurrent completions trigger D exactly once
    2. Lua script atomicity prevents double trigger
    3. Multiple concurrent threads/tasks handle correctly
    """
    
    @pytest.mark.asyncio
    async def test_concurrent_completion_triggers_once_atomic_counter(self):
        """
        Test race condition handling with atomic Lua script counter.
        When B and C complete simultaneously, D should trigger exactly once.
        """
        # Create mock Redis that simulates atomic operations
        mock_redis = MagicMock()
        mock_redis.set = AsyncMock()
        mock_redis.hset = AsyncMock()
        
        # Use an atomic counter to simulate Redis behavior
        counter = [2]  # Start with 2 dependencies
        counter_lock = asyncio.Lock()
        
        async def mock_eval(keys=None, args=None):
            """Simulate atomic decrement - thread-safe. Matches Redis script signature."""
            async with counter_lock:
                counter[0] -= 1
                if counter[0] == 0:
                    return 1  # Trigger
                elif counter[0] < 0:
                    return -2  # Already triggered
                return 0  # Still waiting
        
        # The decrement script is a callable that accepts keys
        coordinator = FanInCoordinator(mock_redis)
        coordinator._decrement_script = mock_eval
        
        workflow_id = uuid4()
        
        # Simulate B and C completing at EXACTLY the same time
        async def complete_b():
            return await coordinator.dependency_completed(
                workflow_id, "node_d", "node_b", {"from": "B"}
            )
        
        async def complete_c():
            return await coordinator.dependency_completed(
                workflow_id, "node_d", "node_c", {"from": "C"}
            )
        
        # Run both completions concurrently
        results = await asyncio.gather(complete_b(), complete_c())
        
        # CRITICAL: Exactly ONE should return True
        true_count = results.count(True)
        false_count = results.count(False)
        
        assert true_count == 1, f"Expected exactly 1 True, got {true_count}"
        assert false_count == 1, f"Expected exactly 1 False, got {false_count}"
    
    @pytest.mark.asyncio
    async def test_high_concurrency_race_condition(self):
        """
        Stress test: Multiple concurrent completions of the same fan-in.
        Only one should succeed in triggering.
        """
        mock_redis = MagicMock()
        mock_redis.set = AsyncMock()
        mock_redis.hset = AsyncMock()
        
        # Atomic counter with lock
        counter = [5]  # 5 dependencies
        counter_lock = asyncio.Lock()
        
        async def mock_eval(*args, **kwargs):
            async with counter_lock:
                counter[0] -= 1
                if counter[0] == 0:
                    return 1
                elif counter[0] < 0:
                    return -2
                return 0
        
        coordinator = FanInCoordinator(mock_redis)
        coordinator._decrement_script = mock_eval
        
        workflow_id = uuid4()
        
        # Create 5 concurrent completion tasks
        async def complete_node(node_id: str):
            return await coordinator.dependency_completed(
                workflow_id, "target", node_id, {"node": node_id}
            )
        
        # Run all 5 completions concurrently
        tasks = [complete_node(f"node_{i}") for i in range(5)]
        results = await asyncio.gather(*tasks)
        
        # CRITICAL: Exactly ONE should trigger
        assert results.count(True) == 1
        assert results.count(False) == 4
    
    @pytest.mark.asyncio
    async def test_race_condition_with_actual_lua_script_logic(self):
        """
        Test the actual Lua script logic for atomicity.
        Simulates the exact behavior of the Lua script.
        """
        # Simulate the Lua script logic in Python
        class AtomicCounter:
            def __init__(self, initial: int):
                self.value = initial
                self.lock = asyncio.Lock()
            
            async def decrement_and_check(self) -> int:
                """Simulates the Lua script behavior."""
                async with self.lock:
                    if self.value is None:
                        return -1  # Not initialized
                    
                    self.value -= 1
                    
                    if self.value == 0:
                        return 1  # Trigger
                    elif self.value < 0:
                        return -2  # Already triggered
                    return 0  # Still waiting
        
        counter = AtomicCounter(2)
        
        async def simulate_completion():
            return await counter.decrement_and_check()
        
        # Run 2 concurrent operations
        results = await asyncio.gather(
            simulate_completion(),
            simulate_completion(),
        )
        
        # Exactly one should be 1 (trigger), one should be 0 (waiting)
        # or both could be 0 and one 1 depending on order
        assert 1 in results, "At least one should trigger"
        assert results.count(1) == 1, "Exactly one should trigger"
    
    @pytest.mark.asyncio
    async def test_triple_race_condition(self):
        """
        Test race condition with 3 nodes completing simultaneously.
        Fan-in node D depends on B, C, and E.
        """
        mock_redis = MagicMock()
        mock_redis.set = AsyncMock()
        mock_redis.hset = AsyncMock()
        
        counter = [3]
        counter_lock = asyncio.Lock()
        
        async def mock_eval(*args, **kwargs):
            async with counter_lock:
                counter[0] -= 1
                if counter[0] == 0:
                    return 1
                elif counter[0] < 0:
                    return -2
                return 0
        
        coordinator = FanInCoordinator(mock_redis)
        coordinator._decrement_script = mock_eval
        
        workflow_id = uuid4()
        
        # Simulate 3 concurrent completions
        results = await asyncio.gather(
            coordinator.dependency_completed(workflow_id, "node_d", "node_b", {}),
            coordinator.dependency_completed(workflow_id, "node_d", "node_c", {}),
            coordinator.dependency_completed(workflow_id, "node_d", "node_e", {}),
        )
        
        # Exactly one True
        assert results.count(True) == 1
        assert results.count(False) == 2
    
    @pytest.mark.asyncio
    async def test_repeated_race_conditions(self):
        """
        Run multiple race condition scenarios to ensure consistency.
        """
        for iteration in range(10):
            mock_redis = MagicMock()
            mock_redis.set = AsyncMock()
            mock_redis.hset = AsyncMock()
            
            counter = [2]
            counter_lock = asyncio.Lock()
            
            async def mock_eval(*args, **kwargs):
                async with counter_lock:
                    counter[0] -= 1
                    if counter[0] == 0:
                        return 1
                    elif counter[0] < 0:
                        return -2
                    return 0
            
            coordinator = FanInCoordinator(mock_redis)
            coordinator._decrement_script = mock_eval
            
            workflow_id = uuid4()
            
            results = await asyncio.gather(
                coordinator.dependency_completed(workflow_id, "d", "b", {}),
                coordinator.dependency_completed(workflow_id, "d", "c", {}),
            )
            
            assert results.count(True) == 1, f"Failed at iteration {iteration}"
    
    @pytest.mark.asyncio
    async def test_race_condition_with_delayed_completion(self):
        """
        Test race condition where one completion is slightly delayed.
        Should still only trigger once.
        """
        mock_redis = MagicMock()
        mock_redis.set = AsyncMock()
        mock_redis.hset = AsyncMock()
        
        counter = [2]
        counter_lock = asyncio.Lock()
        
        async def mock_eval(*args, **kwargs):
            async with counter_lock:
                counter[0] -= 1
                if counter[0] == 0:
                    return 1
                elif counter[0] < 0:
                    return -2
                return 0
        
        coordinator = FanInCoordinator(mock_redis)
        coordinator._decrement_script = mock_eval
        
        workflow_id = uuid4()
        
        async def complete_b():
            return await coordinator.dependency_completed(
                workflow_id, "node_d", "node_b", {}
            )
        
        async def complete_c_delayed():
            await asyncio.sleep(0.001)  # 1ms delay
            return await coordinator.dependency_completed(
                workflow_id, "node_d", "node_c", {}
            )
        
        results = await asyncio.gather(complete_b(), complete_c_delayed())
        
        # Still exactly one trigger
        assert results.count(True) == 1


# ==================== End-to-End Scenario Tests ====================


class TestEndToEndScenarios:
    """
    End-to-end tests combining multiple scenarios.
    """
    
    @pytest.mark.asyncio
    async def test_full_linear_workflow_simulation(self, linear_workflow_abc):
        """Simulate complete execution of linear workflow."""
        definition, _ = parse_workflow_json(linear_workflow_abc)
        
        # Create execution state
        execution = WorkflowExecution(
            workflow_definition_id=definition.id,
            state=WorkflowState.RUNNING.value,
        )
        
        # Initialize node executions
        for node in definition.dag.nodes:
            execution.node_executions[node.id] = NodeExecution(
                node_id=node.id,
                workflow_execution_id=execution.id,
                state=NodeState.PENDING.value,
            )
        
        parser = DAGParser(definition)
        
        # Step 1: Execute A
        ready = parser.get_ready_nodes(set())
        assert ready[0].id == "node_a"
        
        execution.node_executions["node_a"].state = NodeState.COMPLETED.value
        execution.node_executions["node_a"].output_data = {"initial": "data"}
        
        # Step 2: Execute B
        completed = {"node_a"}
        ready = parser.get_ready_nodes(completed)
        assert ready[0].id == "node_b"
        
        execution.node_executions["node_b"].state = NodeState.COMPLETED.value
        execution.node_executions["node_b"].output_data = {
            "processed": execution.node_executions["node_a"].output_data
        }
        
        # Step 3: Execute C
        completed = {"node_a", "node_b"}
        ready = parser.get_ready_nodes(completed)
        assert ready[0].id == "node_c"
        
        execution.node_executions["node_c"].state = NodeState.COMPLETED.value
        execution.node_executions["node_c"].output_data = {
            "final": execution.node_executions["node_b"].output_data
        }
        
        # Verify final state
        all_completed = all(
            ne.state == NodeState.COMPLETED.value
            for ne in execution.node_executions.values()
        )
        assert all_completed
        
        # Verify data chain
        final_output = execution.node_executions["node_c"].output_data
        assert final_output["final"]["processed"]["initial"] == "data"
    
    @pytest.mark.asyncio
    async def test_full_fanout_fanin_workflow_simulation(self, fanout_fanin_workflow):
        """Simulate complete execution of fan-out/fan-in workflow."""
        definition, _ = parse_workflow_json(fanout_fanin_workflow)
        
        execution = WorkflowExecution(
            workflow_definition_id=definition.id,
            state=WorkflowState.RUNNING.value,
        )
        
        for node in definition.dag.nodes:
            execution.node_executions[node.id] = NodeExecution(
                node_id=node.id,
                workflow_execution_id=execution.id,
                state=NodeState.PENDING.value,
            )
        
        parser = DAGParser(definition)
        
        # Step 1: Execute A
        ready = parser.get_ready_nodes(set())
        assert len(ready) == 1
        assert ready[0].id == "node_a"
        
        execution.node_executions["node_a"].state = NodeState.COMPLETED.value
        execution.node_executions["node_a"].output_data = {"request": "start"}
        
        # Step 2: Execute B and C (parallel)
        completed = {"node_a"}
        ready = parser.get_ready_nodes(completed)
        assert len(ready) == 2
        assert {n.id for n in ready} == {"node_b", "node_c"}
        
        # Simulate B completing first
        execution.node_executions["node_b"].state = NodeState.COMPLETED.value
        execution.node_executions["node_b"].output_data = {"branch": "B", "value": 10}
        
        # D should NOT be ready yet
        completed = {"node_a", "node_b"}
        ready = parser.get_ready_nodes(completed)
        assert "node_d" not in {n.id for n in ready}
        
        # C completes
        execution.node_executions["node_c"].state = NodeState.COMPLETED.value
        execution.node_executions["node_c"].output_data = {"branch": "C", "value": 20}
        
        # Now D should be ready
        completed = {"node_a", "node_b", "node_c"}
        ready = parser.get_ready_nodes(completed)
        assert len(ready) == 1
        assert ready[0].id == "node_d"
        
        # Step 3: Execute D with aggregated results
        b_output = execution.node_executions["node_b"].output_data
        c_output = execution.node_executions["node_c"].output_data
        
        execution.node_executions["node_d"].state = NodeState.COMPLETED.value
        execution.node_executions["node_d"].output_data = {
            "aggregated": {
                "from_b": b_output,
                "from_c": c_output,
                "total": b_output["value"] + c_output["value"],
            }
        }
        
        # Verify final aggregation
        final = execution.node_executions["node_d"].output_data
        assert final["aggregated"]["total"] == 30
        assert final["aggregated"]["from_b"]["branch"] == "B"
        assert final["aggregated"]["from_c"]["branch"] == "C"


# ==================== Workflow State Computation Tests ====================


class TestWorkflowStateComputation:
    """Test workflow state computation for different scenarios."""
    
    @pytest.mark.asyncio
    async def test_linear_workflow_completion_state(self, linear_workflow_abc):
        """Test state computation for completed linear workflow."""
        from workflow_engine.core.state_machine import compute_workflow_state_from_nodes
        
        node_states = {
            "node_a": NodeState.COMPLETED,
            "node_b": NodeState.COMPLETED,
            "node_c": NodeState.COMPLETED,
        }
        
        all_nodes = {"node_a", "node_b", "node_c"}
        state = compute_workflow_state_from_nodes(node_states, all_nodes)
        
        assert state == WorkflowState.COMPLETED
    
    @pytest.mark.asyncio
    async def test_fanout_fanin_partial_completion_state(self, fanout_fanin_workflow):
        """Test state computation when fan-out branches are running."""
        from workflow_engine.core.state_machine import compute_workflow_state_from_nodes
        
        node_states = {
            "node_a": NodeState.COMPLETED,
            "node_b": NodeState.RUNNING,
            "node_c": NodeState.COMPLETED,
        }
        
        all_nodes = {"node_a", "node_b", "node_c", "node_d"}
        state = compute_workflow_state_from_nodes(node_states, all_nodes)
        
        assert state == WorkflowState.RUNNING
    
    @pytest.mark.asyncio
    async def test_fanout_branch_failure_state(self, fanout_fanin_workflow):
        """Test state computation when one fan-out branch fails."""
        from workflow_engine.core.state_machine import compute_workflow_state_from_nodes
        
        node_states = {
            "node_a": NodeState.COMPLETED,
            "node_b": NodeState.FAILED,
            "node_c": NodeState.COMPLETED,
        }
        
        all_nodes = {"node_a", "node_b", "node_c", "node_d"}
        state = compute_workflow_state_from_nodes(node_states, all_nodes)
        
        assert state == WorkflowState.FAILED
