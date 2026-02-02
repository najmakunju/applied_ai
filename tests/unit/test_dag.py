"""
Unit tests for DAG validation and parsing.
"""

import pytest

from workflow_engine.core.dag import DAGParser, DAGValidator, parse_workflow_json
from workflow_engine.core.models import DAGDefinition, NodeDefinition, WorkflowDefinition


class TestDAGValidator:
    """Tests for DAG validation logic."""
    
    def test_valid_linear_dag(self, sample_linear_workflow):
        """Test validation of a valid linear DAG."""
        definition, result = parse_workflow_json(sample_linear_workflow)
        
        assert result.is_valid
        assert len(result.errors) == 0
        assert result.topological_order == ["input", "process", "output"]
    
    def test_valid_fanout_fanin_dag(self, sample_fanout_fanin_workflow):
        """Test validation of a valid fan-out/fan-in DAG."""
        definition, result = parse_workflow_json(sample_fanout_fanin_workflow)
        
        assert result.is_valid
        assert len(result.errors) == 0
        
        # Input should be first, output should be last
        assert result.topological_order[0] == "input"
        assert result.topological_order[-1] == "output"
        
        # Parallel nodes should have same level
        assert result.levels["get_user"] == result.levels["get_posts"]
        assert result.levels["get_user"] == result.levels["get_comments"]
    
    def test_cycle_detection(self, sample_cyclic_workflow):
        """Test that cycles are detected and rejected."""
        definition, result = parse_workflow_json(sample_cyclic_workflow)
        
        assert not result.is_valid
        assert any(e.code == "CYCLE_DETECTED" for e in result.errors)
    
    def test_self_loop_detection(self):
        """Test detection of self-referential dependencies."""
        workflow = {
            "name": "Self Loop Test",
            "dag": {
                "nodes": [
                    {"id": "a", "handler": "input", "dependencies": ["a"]},
                ]
            },
        }
        
        definition, result = parse_workflow_json(workflow)
        
        assert not result.is_valid
        assert any(e.code == "SELF_LOOP" for e in result.errors)
    
    def test_invalid_dependency_reference(self):
        """Test detection of references to non-existent nodes."""
        workflow = {
            "name": "Invalid Reference Test",
            "dag": {
                "nodes": [
                    {"id": "a", "handler": "input", "dependencies": []},
                    {"id": "b", "handler": "output", "dependencies": ["nonexistent"]},
                ]
            },
        }
        
        definition, result = parse_workflow_json(workflow)
        
        assert not result.is_valid
        assert any(e.code == "INVALID_DEPENDENCY" for e in result.errors)
    
    def test_duplicate_node_ids(self):
        """Test detection of duplicate node IDs."""
        workflow = {
            "name": "Duplicate ID Test",
            "dag": {
                "nodes": [
                    {"id": "a", "handler": "input", "dependencies": []},
                    {"id": "a", "handler": "output", "dependencies": []},
                ]
            },
        }
        
        with pytest.raises(ValueError) as exc_info:
            parse_workflow_json(workflow)
        
        assert "Duplicate node IDs" in str(exc_info.value)
    
    def test_levels_calculation(self):
        """Test that node levels are calculated correctly."""
        workflow = {
            "name": "Levels Test",
            "dag": {
                "nodes": [
                    {"id": "a", "handler": "input", "dependencies": []},
                    {"id": "b", "handler": "call_external_service", "dependencies": ["a"], "config": {"url": "http://test"}},
                    {"id": "c", "handler": "call_external_service", "dependencies": ["a"], "config": {"url": "http://test"}},
                    {"id": "d", "handler": "output", "dependencies": ["b", "c"]},
                ]
            },
        }
        
        definition, result = parse_workflow_json(workflow)
        
        assert result.is_valid
        assert result.levels["a"] == 0
        assert result.levels["b"] == 1
        assert result.levels["c"] == 1
        assert result.levels["d"] == 2
    
    def test_example_payload(self, example_payload_workflow):
        """Test the exact example payload from task description."""
        definition, result = parse_workflow_json(example_payload_workflow)
        
        assert result.is_valid
        assert definition.name == "Parallel API Fetcher"
        assert len(definition.dag.nodes) == 5
        
        # Check parallel execution levels
        assert result.levels["input"] == 0
        assert result.levels["get_user"] == 1
        assert result.levels["get_posts"] == 1
        assert result.levels["get_comments"] == 1
        assert result.levels["output"] == 2
    
    # Edge cases
    def test_single_node_workflow(self):
        """Test DAG with single node (minimum valid workflow)."""
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
        assert result.topological_order == ["only"]
        assert result.levels["only"] == 0
    
    def test_diamond_pattern_dag(self):
        """Test diamond pattern: A -> (B, C) -> D."""
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
        
        definition, result = parse_workflow_json(workflow)
        
        assert result.is_valid
        assert result.topological_order[0] == "a"
        assert result.topological_order[-1] == "d"
        assert result.levels["b"] == result.levels["c"]
    
    def test_complex_cycle_detection(self):
        """Test detection of complex indirect cycles: A -> B -> C -> D -> B."""
        workflow = {
            "name": "Complex Cycle",
            "dag": {
                "nodes": [
                    {"id": "a", "handler": "input", "dependencies": []},
                    {"id": "b", "handler": "call_external_service", "dependencies": ["a", "d"], "config": {"url": "http://test"}},
                    {"id": "c", "handler": "call_external_service", "dependencies": ["b"], "config": {"url": "http://test"}},
                    {"id": "d", "handler": "output", "dependencies": ["c"]},
                ]
            },
        }
        
        definition, result = parse_workflow_json(workflow)
        
        assert not result.is_valid
        assert any(e.code == "CYCLE_DETECTED" for e in result.errors)
    
    def test_multiple_root_nodes(self):
        """Test DAG with multiple independent root nodes."""
        workflow = {
            "name": "Multiple Roots",
            "dag": {
                "nodes": [
                    {"id": "root1", "handler": "input", "dependencies": []},
                    {"id": "root2", "handler": "input", "dependencies": []},
                    {"id": "root3", "handler": "input", "dependencies": []},
                    {"id": "merge", "handler": "output", "dependencies": ["root1", "root2", "root3"]},
                ]
            },
        }
        
        definition, result = parse_workflow_json(workflow)
        
        assert result.is_valid
        root_levels = [result.levels[f"root{i}"] for i in range(1, 4)]
        assert all(level == 0 for level in root_levels)
        assert result.levels["merge"] == 1
    
    def test_multiple_leaf_nodes(self):
        """Test DAG with multiple independent leaf nodes."""
        workflow = {
            "name": "Multiple Leaves",
            "dag": {
                "nodes": [
                    {"id": "start", "handler": "input", "dependencies": []},
                    {"id": "leaf1", "handler": "output", "dependencies": ["start"]},
                    {"id": "leaf2", "handler": "output", "dependencies": ["start"]},
                    {"id": "leaf3", "handler": "output", "dependencies": ["start"]},
                ]
            },
        }
        
        definition, result = parse_workflow_json(workflow)
        
        assert result.is_valid
        leaf_levels = [result.levels[f"leaf{i}"] for i in range(1, 4)]
        assert all(level == 1 for level in leaf_levels)
    
    def test_wide_fan_in(self):
        """Test node with many dependencies (wide fan-in)."""
        nodes = [{"id": f"source_{i}", "handler": "input", "dependencies": []} for i in range(10)]
        nodes.append({
            "id": "aggregate",
            "handler": "output",
            "dependencies": [f"source_{i}" for i in range(10)],
        })
        
        workflow = {"name": "Wide Fan-In", "dag": {"nodes": nodes}}
        
        definition, result = parse_workflow_json(workflow)
        
        assert result.is_valid
        assert result.levels["aggregate"] == 1
    
    def test_wide_fan_out(self):
        """Test node with many dependents (wide fan-out)."""
        nodes = [{"id": "source", "handler": "input", "dependencies": []}]
        nodes.extend([
            {"id": f"target_{i}", "handler": "output", "dependencies": ["source"]}
            for i in range(10)
        ])
        
        workflow = {"name": "Wide Fan-Out", "dag": {"nodes": nodes}}
        
        definition, result = parse_workflow_json(workflow)
        
        assert result.is_valid
        assert all(result.levels[f"target_{i}"] == 1 for i in range(10))
    
    def test_deep_chain(self):
        """Test deeply nested linear chain."""
        nodes = [{"id": "node_0", "handler": "input", "dependencies": []}]
        for i in range(1, 20):
            nodes.append({
                "id": f"node_{i}",
                "handler": "call_external_service" if i < 19 else "output",
                "dependencies": [f"node_{i-1}"],
                "config": {"url": "http://test"} if i < 19 else {},
            })
        
        workflow = {"name": "Deep Chain", "dag": {"nodes": nodes}}
        
        definition, result = parse_workflow_json(workflow)
        
        assert result.is_valid
        assert result.levels["node_0"] == 0
        assert result.levels["node_19"] == 19
    
    def test_no_input_handler_warning(self):
        """Test warning when no input handler node exists."""
        workflow = {
            "name": "No Input Handler",
            "dag": {
                "nodes": [
                    {"id": "a", "handler": "call_external_service", "dependencies": [], "config": {"url": "http://test"}},
                    {"id": "b", "handler": "output", "dependencies": ["a"]},
                ]
            },
        }
        
        definition, result = parse_workflow_json(workflow)
        
        assert result.is_valid  # Still valid, just has warning
        assert any(w.code == "NO_INPUT_NODE" for w in result.warnings)
    
    def test_no_output_handler_warning(self):
        """Test warning when no output handler node exists."""
        workflow = {
            "name": "No Output Handler",
            "dag": {
                "nodes": [
                    {"id": "a", "handler": "input", "dependencies": []},
                    {"id": "b", "handler": "call_external_service", "dependencies": ["a"], "config": {"url": "http://test"}},
                ]
            },
        }
        
        definition, result = parse_workflow_json(workflow)
        
        assert result.is_valid  # Still valid, just has warning
        assert any(w.code == "NO_OUTPUT_NODE" for w in result.warnings)
    
    def test_multiple_invalid_dependencies(self):
        """Test node with multiple invalid dependency references."""
        workflow = {
            "name": "Multiple Invalid Deps",
            "dag": {
                "nodes": [
                    {"id": "a", "handler": "input", "dependencies": []},
                    {"id": "b", "handler": "output", "dependencies": ["missing1", "missing2", "a"]},
                ]
            },
        }
        
        definition, result = parse_workflow_json(workflow)
        
        assert not result.is_valid
        invalid_dep_errors = [e for e in result.errors if e.code == "INVALID_DEPENDENCY"]
        assert len(invalid_dep_errors) == 2
    
    def test_complex_graph_with_multiple_paths(self):
        """Test complex graph where multiple paths lead to same node."""
        workflow = {
            "name": "Complex Multi-Path",
            "dag": {
                "nodes": [
                    {"id": "input", "handler": "input", "dependencies": []},
                    {"id": "a", "handler": "call_external_service", "dependencies": ["input"], "config": {"url": "http://test"}},
                    {"id": "b", "handler": "call_external_service", "dependencies": ["input"], "config": {"url": "http://test"}},
                    {"id": "c", "handler": "call_external_service", "dependencies": ["a"], "config": {"url": "http://test"}},
                    {"id": "d", "handler": "call_external_service", "dependencies": ["a", "b"], "config": {"url": "http://test"}},
                    {"id": "output", "handler": "output", "dependencies": ["c", "d"]},
                ]
            },
        }
        
        definition, result = parse_workflow_json(workflow)
        
        assert result.is_valid
        # c depends only on a (level 1), so c is level 2
        # d depends on a (level 1) and b (level 1), so d is level 2
        # output depends on c (level 2) and d (level 2), so output is level 3
        assert result.levels["c"] == 2
        assert result.levels["d"] == 2
        assert result.levels["output"] == 3


class TestDAGParser:
    """Tests for DAG parsing utilities."""
    
    def test_get_ready_nodes_initial(self, sample_linear_workflow):
        """Test getting ready nodes at start of execution."""
        definition, _ = parse_workflow_json(sample_linear_workflow)
        parser = DAGParser(definition)
        
        ready = parser.get_ready_nodes(completed_nodes=set())
        
        assert len(ready) == 1
        assert ready[0].id == "input"
    
    def test_get_ready_nodes_after_completion(self, sample_linear_workflow):
        """Test getting ready nodes after some complete."""
        definition, _ = parse_workflow_json(sample_linear_workflow)
        parser = DAGParser(definition)
        
        ready = parser.get_ready_nodes(completed_nodes={"input"})
        
        assert len(ready) == 1
        assert ready[0].id == "process"
    
    def test_get_ready_nodes_fanout(self, sample_fanout_fanin_workflow):
        """Test that fan-out produces multiple ready nodes."""
        definition, _ = parse_workflow_json(sample_fanout_fanin_workflow)
        parser = DAGParser(definition)
        
        ready = parser.get_ready_nodes(completed_nodes={"input"})
        
        assert len(ready) == 3
        ready_ids = {n.id for n in ready}
        assert ready_ids == {"get_user", "get_posts", "get_comments"}
    
    def test_get_ready_nodes_fanin(self, sample_fanout_fanin_workflow):
        """Test that fan-in waits for all dependencies."""
        definition, _ = parse_workflow_json(sample_fanout_fanin_workflow)
        parser = DAGParser(definition)
        
        # Only some parallel branches complete
        ready = parser.get_ready_nodes(
            completed_nodes={"input", "get_user", "get_posts"}
        )
        
        # Output should not be ready yet
        assert not any(n.id == "output" for n in ready)
        
        # Now all complete
        ready = parser.get_ready_nodes(
            completed_nodes={"input", "get_user", "get_posts", "get_comments"}
        )
        
        assert len(ready) == 1
        assert ready[0].id == "output"
    
    def test_get_parallel_batches(self, sample_fanout_fanin_workflow):
        """Test parallel batch generation."""
        definition, _ = parse_workflow_json(sample_fanout_fanin_workflow)
        parser = DAGParser(definition)
        
        batches = parser.get_parallel_batches()
        
        assert len(batches) == 3
        assert batches[0] == ["input"]
        assert set(batches[1]) == {"get_user", "get_posts", "get_comments"}
        assert batches[2] == ["output"]
    
    # Edge cases
    def test_get_ready_nodes_all_completed(self, sample_linear_workflow):
        """Test get_ready_nodes when all nodes are completed."""
        definition, _ = parse_workflow_json(sample_linear_workflow)
        parser = DAGParser(definition)
        
        ready = parser.get_ready_nodes(completed_nodes={"input", "process", "output"})
        
        assert len(ready) == 0
    
    def test_get_ready_nodes_empty_completed_set(self, sample_fanout_fanin_workflow):
        """Test get_ready_nodes with empty completed set."""
        definition, _ = parse_workflow_json(sample_fanout_fanin_workflow)
        parser = DAGParser(definition)
        
        ready = parser.get_ready_nodes(completed_nodes=set())
        
        # Only root nodes should be ready
        assert len(ready) == 1
        assert ready[0].id == "input"
    
    def test_get_ready_nodes_single_node_workflow(self):
        """Test get_ready_nodes for single node workflow."""
        workflow = {
            "name": "Single Node",
            "dag": {"nodes": [{"id": "only", "handler": "input", "dependencies": []}]},
        }
        
        definition, _ = parse_workflow_json(workflow)
        parser = DAGParser(definition)
        
        # Initially ready
        ready = parser.get_ready_nodes(completed_nodes=set())
        assert len(ready) == 1
        assert ready[0].id == "only"
        
        # After completion, nothing ready
        ready = parser.get_ready_nodes(completed_nodes={"only"})
        assert len(ready) == 0
    
    def test_get_parallel_batches_single_node(self):
        """Test parallel batches for single node workflow."""
        workflow = {
            "name": "Single Node",
            "dag": {"nodes": [{"id": "only", "handler": "input", "dependencies": []}]},
        }
        
        definition, _ = parse_workflow_json(workflow)
        parser = DAGParser(definition)
        
        batches = parser.get_parallel_batches()
        
        assert len(batches) == 1
        assert batches[0] == ["only"]
    
    def test_get_parallel_batches_deep_chain(self):
        """Test parallel batches for deep linear chain."""
        nodes = [{"id": "n0", "handler": "input", "dependencies": []}]
        for i in range(1, 5):
            nodes.append({
                "id": f"n{i}",
                "handler": "output" if i == 4 else "call_external_service",
                "dependencies": [f"n{i-1}"],
                "config": {"url": "http://test"} if i < 4 else {},
            })
        
        workflow = {"name": "Chain", "dag": {"nodes": nodes}}
        definition, _ = parse_workflow_json(workflow)
        parser = DAGParser(definition)
        
        batches = parser.get_parallel_batches()
        
        # Each node is in its own batch (no parallelism)
        assert len(batches) == 5
        for i, batch in enumerate(batches):
            assert batch == [f"n{i}"]
    
    def test_get_node_returns_none_for_nonexistent(self):
        """Test get_node returns None for non-existent node."""
        workflow = {
            "name": "Test",
            "dag": {"nodes": [{"id": "a", "handler": "input", "dependencies": []}]},
        }
        
        definition, _ = parse_workflow_json(workflow)
        parser = DAGParser(definition)
        
        assert parser.get_node("nonexistent") is None
    
    def test_get_node_effective_retry_config_uses_node_specific(self):
        """Test effective retry config uses node-specific when available."""
        from workflow_engine.core.models import RetryConfig
        
        workflow = {
            "name": "Test",
            "dag": {
                "nodes": [
                    {
                        "id": "a",
                        "handler": "input",
                        "dependencies": [],
                        "retry_config": {"max_retries": 10, "initial_delay": 5.0},
                    }
                ]
            },
        }
        
        definition, _ = parse_workflow_json(workflow)
        parser = DAGParser(definition)
        
        retry_config = parser.get_node_effective_retry_config("a")
        
        assert retry_config.max_retries == 10
        assert retry_config.initial_delay == 5.0
    
    def test_get_node_effective_retry_config_uses_default(self):
        """Test effective retry config uses default when no node-specific."""
        workflow = {
            "name": "Test",
            "default_retry_config": {"max_retries": 7},
            "dag": {
                "nodes": [{"id": "a", "handler": "input", "dependencies": []}]
            },
        }
        
        definition, _ = parse_workflow_json(workflow)
        parser = DAGParser(definition)
        
        retry_config = parser.get_node_effective_retry_config("a")
        
        assert retry_config.max_retries == 7
    
    def test_is_valid_property(self, sample_linear_workflow, sample_cyclic_workflow):
        """Test is_valid property reflects validation state."""
        valid_def, _ = parse_workflow_json(sample_linear_workflow)
        valid_parser = DAGParser(valid_def)
        
        invalid_def, _ = parse_workflow_json(sample_cyclic_workflow)
        invalid_parser = DAGParser(invalid_def)
        
        assert valid_parser.is_valid is True
        assert invalid_parser.is_valid is False
    
    def test_get_ready_nodes_with_superset_completed(self, sample_linear_workflow):
        """Test get_ready_nodes handles superset of completed nodes gracefully."""
        definition, _ = parse_workflow_json(sample_linear_workflow)
        parser = DAGParser(definition)
        
        # Pass completed set that includes nodes not in DAG
        ready = parser.get_ready_nodes(
            completed_nodes={"input", "process", "output", "extra_node"}
        )
        
        assert len(ready) == 0
