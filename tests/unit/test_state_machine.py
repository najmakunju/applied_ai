"""
Unit tests for state machine transitions.
"""

import pytest

from workflow_engine.core.state_machine import (
    InvalidStateTransitionError,
    NodeState,
    NodeStateMachine,
    WorkflowState,
    WorkflowStateMachine,
    compute_workflow_state_from_nodes,
)


class TestNodeStateMachine:
    """Tests for node state machine."""
    
    def test_initial_state(self):
        """Test default initial state is PENDING."""
        sm = NodeStateMachine()
        assert sm.state == NodeState.PENDING
    
    def test_valid_transition_pending_to_queued(self):
        """Test valid transition from PENDING to QUEUED."""
        sm = NodeStateMachine()
        
        transition = sm.transition(NodeState.QUEUED, reason="Dependencies met")
        
        assert sm.state == NodeState.QUEUED
        assert transition.from_state == "PENDING"
        assert transition.to_state == "QUEUED"
        assert transition.reason == "Dependencies met"
    
    def test_valid_transition_queued_to_running(self):
        """Test valid transition from QUEUED to RUNNING."""
        sm = NodeStateMachine(NodeState.QUEUED)
        
        sm.transition(NodeState.RUNNING)
        
        assert sm.state == NodeState.RUNNING
    
    def test_valid_transition_running_to_completed(self):
        """Test valid transition from RUNNING to COMPLETED."""
        sm = NodeStateMachine(NodeState.RUNNING)
        
        sm.transition(NodeState.COMPLETED)
        
        assert sm.state == NodeState.COMPLETED
        assert sm.is_terminal
        assert sm.is_success
    
    def test_valid_transition_running_to_failed(self):
        """Test valid transition from RUNNING to FAILED."""
        sm = NodeStateMachine(NodeState.RUNNING)
        
        sm.transition(NodeState.FAILED, reason="Task error")
        
        assert sm.state == NodeState.FAILED
        assert sm.is_terminal
        assert sm.is_failure
    
    def test_invalid_transition_from_terminal(self):
        """Test that transitions from terminal states are rejected."""
        sm = NodeStateMachine(NodeState.COMPLETED)
        
        with pytest.raises(InvalidStateTransitionError):
            sm.transition(NodeState.RUNNING)
    
    def test_invalid_direct_transition(self):
        """Test that invalid direct transitions are rejected."""
        sm = NodeStateMachine(NodeState.PENDING)
        
        # Cannot go directly from PENDING to RUNNING (must go through QUEUED)
        with pytest.raises(InvalidStateTransitionError):
            sm.transition(NodeState.RUNNING)
    
    def test_transition_with_guard(self):
        """Test transition with guard condition."""
        sm = NodeStateMachine()
        
        # Guard that always fails
        with pytest.raises(InvalidStateTransitionError) as exc_info:
            sm.transition(
                NodeState.QUEUED,
                guard=lambda: False,
            )
        
        assert "Guard condition failed" in str(exc_info.value)
        assert sm.state == NodeState.PENDING  # State unchanged
    
    def test_cancellation_from_any_state(self):
        """Test that CANCELLED can be reached from most states."""
        for initial_state in [NodeState.PENDING, NodeState.QUEUED, NodeState.RUNNING]:
            sm = NodeStateMachine(initial_state)
            sm.transition(NodeState.CANCELLED)
            assert sm.state == NodeState.CANCELLED
    
    def test_history_tracking(self):
        """Test that transition history is tracked."""
        sm = NodeStateMachine()
        
        sm.transition(NodeState.QUEUED)
        sm.transition(NodeState.RUNNING)
        sm.transition(NodeState.COMPLETED)
        
        history = sm.history
        assert len(history) == 3
        assert history[0].to_state == "QUEUED"
        assert history[1].to_state == "RUNNING"
        assert history[2].to_state == "COMPLETED"
    
    def test_get_valid_transitions(self):
        """Test getting valid transitions from a state."""
        sm = NodeStateMachine(NodeState.RUNNING)
        
        valid = sm.get_valid_transitions()
        
        assert NodeState.COMPLETED in valid
        assert NodeState.FAILED in valid
        assert NodeState.CANCELLED in valid
        assert NodeState.PENDING not in valid
    
    # Edge cases
    def test_paused_state_transitions(self):
        """Test transitions involving PAUSED state."""
        sm = NodeStateMachine(NodeState.RUNNING)
        
        # Can pause from RUNNING
        sm.transition(NodeState.PAUSED)
        assert sm.state == NodeState.PAUSED
        
        # Can resume to RUNNING from PAUSED
        sm.transition(NodeState.RUNNING)
        assert sm.state == NodeState.RUNNING
    
    def test_paused_to_queued(self):
        """Test transition from PAUSED to QUEUED (re-queue)."""
        sm = NodeStateMachine(NodeState.PAUSED)
        
        sm.transition(NodeState.QUEUED)
        assert sm.state == NodeState.QUEUED
    
    def test_paused_to_cancelled(self):
        """Test transition from PAUSED to CANCELLED."""
        sm = NodeStateMachine(NodeState.PAUSED)
        
        sm.transition(NodeState.CANCELLED)
        assert sm.state == NodeState.CANCELLED
        assert sm.is_terminal
    
    def test_cannot_transition_from_cancelled(self):
        """Test that no transitions are allowed from CANCELLED."""
        sm = NodeStateMachine(NodeState.CANCELLED)
        
        for target in NodeState:
            if target != NodeState.CANCELLED:
                with pytest.raises(InvalidStateTransitionError):
                    sm.transition(target)
    
    def test_cannot_transition_from_failed(self):
        """Test that no transitions are allowed from FAILED."""
        sm = NodeStateMachine(NodeState.FAILED)
        
        for target in NodeState:
            if target != NodeState.FAILED:
                with pytest.raises(InvalidStateTransitionError):
                    sm.transition(target)
    
    def test_queued_to_failed_directly(self):
        """Test transition from QUEUED directly to FAILED (e.g., queue timeout)."""
        sm = NodeStateMachine(NodeState.QUEUED)
        
        sm.transition(NodeState.FAILED, reason="Queue timeout")
        assert sm.state == NodeState.FAILED
    
    def test_guard_that_passes(self):
        """Test transition with guard that passes."""
        sm = NodeStateMachine()
        
        transition = sm.transition(
            NodeState.QUEUED,
            guard=lambda: True,
        )
        
        assert sm.state == NodeState.QUEUED
        assert transition.to_state == "QUEUED"
    
    def test_transition_metadata(self):
        """Test transition with metadata."""
        sm = NodeStateMachine()
        
        metadata = {"worker_id": "worker-1", "attempt": 1}
        transition = sm.transition(
            NodeState.QUEUED,
            metadata=metadata,
        )
        
        assert transition.metadata == metadata
    
    def test_transition_triggered_by(self):
        """Test transition with triggered_by information."""
        sm = NodeStateMachine()
        
        transition = sm.transition(
            NodeState.QUEUED,
            triggered_by="orchestrator",
        )
        
        assert transition.triggered_by == "orchestrator"
    
    def test_history_is_immutable_copy(self):
        """Test that history property returns a copy."""
        sm = NodeStateMachine()
        sm.transition(NodeState.QUEUED)
        
        history1 = sm.history
        history2 = sm.history
        
        # Should be equal but not the same object
        assert history1 == history2
        assert history1 is not history2
    
    def test_can_transition_to_method(self):
        """Test can_transition_to method."""
        sm = NodeStateMachine(NodeState.PENDING)
        
        assert sm.can_transition_to(NodeState.QUEUED) is True
        assert sm.can_transition_to(NodeState.CANCELLED) is True
        assert sm.can_transition_to(NodeState.RUNNING) is False
        assert sm.can_transition_to(NodeState.COMPLETED) is False
    
    def test_complete_workflow_path(self):
        """Test complete successful workflow path."""
        sm = NodeStateMachine()
        
        # PENDING -> QUEUED -> RUNNING -> COMPLETED
        sm.transition(NodeState.QUEUED)
        sm.transition(NodeState.RUNNING)
        sm.transition(NodeState.COMPLETED)
        
        assert sm.is_terminal
        assert sm.is_success
        assert not sm.is_failure
        assert len(sm.history) == 3
    
    def test_failed_workflow_path(self):
        """Test failed workflow path."""
        sm = NodeStateMachine()
        
        # PENDING -> QUEUED -> RUNNING -> FAILED
        sm.transition(NodeState.QUEUED)
        sm.transition(NodeState.RUNNING)
        sm.transition(NodeState.FAILED, reason="Task execution error")
        
        assert sm.is_terminal
        assert sm.is_failure
        assert not sm.is_success
    
    def test_pause_resume_complete_path(self):
        """Test path with pause and resume."""
        sm = NodeStateMachine()
        
        sm.transition(NodeState.QUEUED)
        sm.transition(NodeState.RUNNING)
        sm.transition(NodeState.PAUSED, reason="User requested pause")
        
        assert not sm.is_terminal
        
        sm.transition(NodeState.RUNNING, reason="Resumed")
        sm.transition(NodeState.COMPLETED)
        
        assert sm.is_terminal
        assert sm.is_success
        assert len(sm.history) == 5


class TestWorkflowStateMachine:
    """Tests for workflow state machine."""
    
    def test_initial_state(self):
        """Test default initial state is PENDING."""
        sm = WorkflowStateMachine()
        assert sm.state == WorkflowState.PENDING
    
    def test_valid_workflow_lifecycle(self):
        """Test complete valid workflow lifecycle."""
        sm = WorkflowStateMachine()
        
        sm.transition(WorkflowState.RUNNING, reason="Triggered")
        assert sm.is_active
        
        sm.transition(WorkflowState.COMPLETED, reason="All nodes completed")
        assert sm.is_terminal
        assert sm.is_success
    
    def test_workflow_failure(self):
        """Test workflow failure transition."""
        sm = WorkflowStateMachine(WorkflowState.RUNNING)
        
        sm.transition(WorkflowState.FAILED, reason="Node failed")
        
        assert sm.is_terminal
        assert sm.is_failure
    
    # Edge cases
    def test_workflow_paused_state(self):
        """Test workflow can be paused."""
        sm = WorkflowStateMachine(WorkflowState.RUNNING)
        
        sm.transition(WorkflowState.PAUSED, reason="User requested pause")
        
        assert sm.state == WorkflowState.PAUSED
        assert not sm.is_terminal
        assert not sm.is_active
    
    def test_workflow_resume_from_paused(self):
        """Test workflow can resume from paused."""
        sm = WorkflowStateMachine(WorkflowState.PAUSED)
        
        sm.transition(WorkflowState.RUNNING, reason="Resumed")
        
        assert sm.state == WorkflowState.RUNNING
        assert sm.is_active
    
    def test_workflow_cancel_from_paused(self):
        """Test workflow can be cancelled from paused."""
        sm = WorkflowStateMachine(WorkflowState.PAUSED)
        
        sm.transition(WorkflowState.CANCELLED)
        
        assert sm.is_terminal
        assert sm.is_failure
    
    def test_workflow_cancel_from_pending(self):
        """Test workflow can be cancelled from pending."""
        sm = WorkflowStateMachine(WorkflowState.PENDING)
        
        sm.transition(WorkflowState.CANCELLED)
        
        assert sm.is_terminal
    
    def test_workflow_cancel_from_running(self):
        """Test workflow can be cancelled from running."""
        sm = WorkflowStateMachine(WorkflowState.RUNNING)
        
        sm.transition(WorkflowState.CANCELLED)
        
        assert sm.is_terminal
    
    def test_invalid_transition_pending_to_completed(self):
        """Test cannot go directly from PENDING to COMPLETED."""
        sm = WorkflowStateMachine(WorkflowState.PENDING)
        
        with pytest.raises(InvalidStateTransitionError):
            sm.transition(WorkflowState.COMPLETED)
    
    def test_invalid_transition_pending_to_failed(self):
        """Test cannot go directly from PENDING to FAILED."""
        sm = WorkflowStateMachine(WorkflowState.PENDING)
        
        with pytest.raises(InvalidStateTransitionError):
            sm.transition(WorkflowState.FAILED)
    
    def test_invalid_transition_pending_to_paused(self):
        """Test cannot go directly from PENDING to PAUSED."""
        sm = WorkflowStateMachine(WorkflowState.PENDING)
        
        with pytest.raises(InvalidStateTransitionError):
            sm.transition(WorkflowState.PAUSED)
    
    def test_no_transitions_from_completed(self):
        """Test no transitions allowed from COMPLETED."""
        sm = WorkflowStateMachine(WorkflowState.COMPLETED)
        
        for target in WorkflowState:
            if target != WorkflowState.COMPLETED:
                with pytest.raises(InvalidStateTransitionError):
                    sm.transition(target)
    
    def test_no_transitions_from_failed(self):
        """Test no transitions allowed from FAILED."""
        sm = WorkflowStateMachine(WorkflowState.FAILED)
        
        for target in WorkflowState:
            if target != WorkflowState.FAILED:
                with pytest.raises(InvalidStateTransitionError):
                    sm.transition(target)
    
    def test_get_valid_transitions_from_pending(self):
        """Test valid transitions from PENDING."""
        sm = WorkflowStateMachine(WorkflowState.PENDING)
        
        valid = sm.get_valid_transitions()
        
        assert WorkflowState.RUNNING in valid
        assert WorkflowState.CANCELLED in valid
        assert len(valid) == 2
    
    def test_get_valid_transitions_from_running(self):
        """Test valid transitions from RUNNING."""
        sm = WorkflowStateMachine(WorkflowState.RUNNING)
        
        valid = sm.get_valid_transitions()
        
        assert WorkflowState.COMPLETED in valid
        assert WorkflowState.FAILED in valid
        assert WorkflowState.CANCELLED in valid
        assert WorkflowState.PAUSED in valid
        assert len(valid) == 4
    
    def test_workflow_history_tracking(self):
        """Test workflow transition history is tracked."""
        sm = WorkflowStateMachine()
        
        sm.transition(WorkflowState.RUNNING)
        sm.transition(WorkflowState.PAUSED)
        sm.transition(WorkflowState.RUNNING)
        sm.transition(WorkflowState.COMPLETED)
        
        history = sm.history
        assert len(history) == 4
        assert [h.to_state for h in history] == ["RUNNING", "PAUSED", "RUNNING", "COMPLETED"]
    
    def test_is_active_only_when_running(self):
        """Test is_active is True only when RUNNING."""
        for state in WorkflowState:
            sm = WorkflowStateMachine(state)
            if state == WorkflowState.RUNNING:
                assert sm.is_active
            else:
                assert not sm.is_active


class TestComputeWorkflowState:
    """Tests for computing workflow state from node states."""
    
    def test_empty_returns_pending(self):
        """Test empty node states returns PENDING."""
        state = compute_workflow_state_from_nodes({}, {"a", "b"})
        assert state == WorkflowState.PENDING
    
    def test_all_completed(self):
        """Test all nodes completed returns COMPLETED."""
        node_states = {
            "a": NodeState.COMPLETED,
            "b": NodeState.COMPLETED,
            "c": NodeState.COMPLETED,
        }
        
        state = compute_workflow_state_from_nodes(node_states, {"a", "b", "c"})
        assert state == WorkflowState.COMPLETED
    
    def test_any_failed_returns_failed(self):
        """Test any node failed returns FAILED."""
        node_states = {
            "a": NodeState.COMPLETED,
            "b": NodeState.FAILED,
            "c": NodeState.PENDING,
        }
        
        state = compute_workflow_state_from_nodes(node_states, {"a", "b", "c"})
        assert state == WorkflowState.FAILED
    
    def test_running_if_any_active(self):
        """Test workflow is RUNNING if any node is active."""
        node_states = {
            "a": NodeState.COMPLETED,
            "b": NodeState.RUNNING,
            "c": NodeState.PENDING,
        }
        
        state = compute_workflow_state_from_nodes(node_states, {"a", "b", "c"})
        assert state == WorkflowState.RUNNING
    
    def test_failed_propagates(self):
        """Test FAILED propagates to workflow."""
        node_states = {
            "a": NodeState.COMPLETED,
            "b": NodeState.FAILED,
        }
        
        state = compute_workflow_state_from_nodes(node_states, {"a", "b"})
        assert state == WorkflowState.FAILED
    
    # Edge cases
    def test_cancelled_propagates(self):
        """Test CANCELLED propagates to workflow."""
        node_states = {
            "a": NodeState.COMPLETED,
            "b": NodeState.CANCELLED,
        }
        
        state = compute_workflow_state_from_nodes(node_states, {"a", "b"})
        assert state == WorkflowState.CANCELLED
    
    def test_failed_takes_precedence_over_cancelled(self):
        """Test FAILED takes precedence over CANCELLED."""
        node_states = {
            "a": NodeState.FAILED,
            "b": NodeState.CANCELLED,
            "c": NodeState.COMPLETED,
        }
        
        state = compute_workflow_state_from_nodes(node_states, {"a", "b", "c"})
        assert state == WorkflowState.FAILED
    
    def test_all_pending(self):
        """Test all nodes PENDING returns RUNNING (workflow started)."""
        node_states = {
            "a": NodeState.PENDING,
            "b": NodeState.PENDING,
        }
        
        state = compute_workflow_state_from_nodes(node_states, {"a", "b"})
        assert state == WorkflowState.RUNNING
    
    def test_all_queued(self):
        """Test all nodes QUEUED returns RUNNING."""
        node_states = {
            "a": NodeState.QUEUED,
            "b": NodeState.QUEUED,
        }
        
        state = compute_workflow_state_from_nodes(node_states, {"a", "b"})
        assert state == WorkflowState.RUNNING
    
    def test_mix_of_queued_and_running(self):
        """Test mix of QUEUED and RUNNING returns RUNNING."""
        node_states = {
            "a": NodeState.QUEUED,
            "b": NodeState.RUNNING,
            "c": NodeState.PENDING,
        }
        
        state = compute_workflow_state_from_nodes(node_states, {"a", "b", "c"})
        assert state == WorkflowState.RUNNING
    
    def test_all_paused(self):
        """Test all nodes PAUSED returns PAUSED."""
        node_states = {
            "a": NodeState.PAUSED,
            "b": NodeState.PAUSED,
        }
        
        state = compute_workflow_state_from_nodes(node_states, {"a", "b"})
        assert state == WorkflowState.PAUSED
    
    def test_mix_of_completed_and_paused(self):
        """Test mix of COMPLETED and PAUSED returns PAUSED."""
        node_states = {
            "a": NodeState.COMPLETED,
            "b": NodeState.PAUSED,
        }
        
        state = compute_workflow_state_from_nodes(node_states, {"a", "b"})
        assert state == WorkflowState.PAUSED
    
    def test_partial_completion_returns_running(self):
        """Test partial completion (some completed, some pending) returns RUNNING."""
        node_states = {
            "a": NodeState.COMPLETED,
            "b": NodeState.COMPLETED,
        }
        
        # Only 2 of 3 nodes have states
        state = compute_workflow_state_from_nodes(node_states, {"a", "b", "c"})
        assert state == WorkflowState.RUNNING
    
    def test_single_node_completed(self):
        """Test single node workflow completed."""
        node_states = {"only": NodeState.COMPLETED}
        
        state = compute_workflow_state_from_nodes(node_states, {"only"})
        assert state == WorkflowState.COMPLETED
    
    def test_single_node_failed(self):
        """Test single node workflow failed."""
        node_states = {"only": NodeState.FAILED}
        
        state = compute_workflow_state_from_nodes(node_states, {"only"})
        assert state == WorkflowState.FAILED
    
    def test_single_node_running(self):
        """Test single node workflow running."""
        node_states = {"only": NodeState.RUNNING}
        
        state = compute_workflow_state_from_nodes(node_states, {"only"})
        assert state == WorkflowState.RUNNING
    
    def test_nodes_not_yet_tracked(self):
        """Test when some nodes haven't started (not in node_states)."""
        node_states = {
            "a": NodeState.COMPLETED,
        }
        
        # Node "b" exists in workflow but hasn't been tracked yet
        state = compute_workflow_state_from_nodes(node_states, {"a", "b"})
        assert state == WorkflowState.RUNNING
    
    def test_empty_all_nodes_set(self):
        """Test with empty all_nodes set."""
        node_states = {}
        
        state = compute_workflow_state_from_nodes(node_states, set())
        assert state == WorkflowState.PENDING
    
    def test_large_workflow_all_completed(self):
        """Test large workflow with all nodes completed."""
        nodes = {f"node_{i}": NodeState.COMPLETED for i in range(50)}
        all_nodes = {f"node_{i}" for i in range(50)}
        
        state = compute_workflow_state_from_nodes(nodes, all_nodes)
        assert state == WorkflowState.COMPLETED
    
    def test_large_workflow_one_failed(self):
        """Test large workflow with one node failed."""
        nodes = {f"node_{i}": NodeState.COMPLETED for i in range(49)}
        nodes["node_49"] = NodeState.FAILED
        all_nodes = {f"node_{i}" for i in range(50)}
        
        state = compute_workflow_state_from_nodes(nodes, all_nodes)
        assert state == WorkflowState.FAILED
    
    def test_running_paused_pending_mix(self):
        """Test complex mix of active states."""
        node_states = {
            "a": NodeState.COMPLETED,
            "b": NodeState.RUNNING,
            "c": NodeState.PAUSED,
            "d": NodeState.PENDING,
        }
        
        state = compute_workflow_state_from_nodes(node_states, {"a", "b", "c", "d"})
        # RUNNING should take precedence over PAUSED when there are active nodes
        assert state == WorkflowState.RUNNING
