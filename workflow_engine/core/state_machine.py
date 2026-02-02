"""
State machine definitions for workflow and node states.

Implements explicit state transitions with guards and validation.
"""

from datetime import datetime
from enum import Enum
from typing import Callable, Optional

from pydantic import BaseModel, Field


class NodeState(str, Enum):
    """
    Possible states for a node execution.
    
    State transitions:
    - PENDING -> QUEUED -> RUNNING -> COMPLETED
    - PENDING -> QUEUED -> RUNNING -> FAILED
    - Any state -> CANCELLED
    - RUNNING -> PAUSED -> RUNNING (resume)
    """
    
    PENDING = "PENDING"      # Initial state, waiting for dependencies
    QUEUED = "QUEUED"        # Dependencies met, waiting in task queue
    RUNNING = "RUNNING"      # Currently being executed by a worker
    COMPLETED = "COMPLETED"  # Successfully completed
    FAILED = "FAILED"        # Failed after all retries exhausted (includes timeouts)
    CANCELLED = "CANCELLED"  # Manually cancelled
    PAUSED = "PAUSED"        # Temporarily paused


class WorkflowState(str, Enum):
    """
    Possible states for a workflow execution.
    
    State transitions:
    - PENDING -> RUNNING -> COMPLETED
    - PENDING -> RUNNING -> FAILED
    - Any state -> CANCELLED
    - RUNNING -> PAUSED -> RUNNING (resume)
    """
    
    PENDING = "PENDING"      # Created but not started
    RUNNING = "RUNNING"      # Active execution
    COMPLETED = "COMPLETED"  # All nodes completed successfully
    FAILED = "FAILED"        # One or more nodes failed (includes timeouts)
    CANCELLED = "CANCELLED"  # Manually cancelled
    PAUSED = "PAUSED"        # Temporarily paused


class StateTransition(BaseModel):
    """Represents a state transition event."""
    
    from_state: str
    to_state: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    reason: Optional[str] = None
    triggered_by: Optional[str] = None  # User, system, worker, etc.
    metadata: dict = Field(default_factory=dict)


class InvalidStateTransitionError(Exception):
    """Raised when an invalid state transition is attempted."""
    
    def __init__(self, from_state: str, to_state: str, message: str = ""):
        self.from_state = from_state
        self.to_state = to_state
        super().__init__(
            f"Invalid state transition from {from_state} to {to_state}"
            + (f": {message}" if message else "")
        )


# Type alias for transition guards
TransitionGuard = Callable[[], bool]


class NodeStateMachine:
    """
    State machine for node execution states.
    
    Defines valid transitions and provides transition guards.
    """
    
    # Valid state transitions: from_state -> [valid_to_states]
    VALID_TRANSITIONS: dict[NodeState, set[NodeState]] = {
        NodeState.PENDING: {NodeState.QUEUED, NodeState.CANCELLED},
        NodeState.QUEUED: {NodeState.RUNNING, NodeState.CANCELLED, NodeState.FAILED},
        NodeState.RUNNING: {
            NodeState.COMPLETED,
            NodeState.FAILED,
            NodeState.CANCELLED,
            NodeState.PAUSED,
        },
        NodeState.PAUSED: {NodeState.RUNNING, NodeState.QUEUED, NodeState.CANCELLED},
        NodeState.COMPLETED: set(),  # Terminal state
        NodeState.FAILED: set(),     # Terminal state
        NodeState.CANCELLED: set(),  # Terminal state
    }
    
    # Terminal states - no further transitions allowed
    TERMINAL_STATES: set[NodeState] = {
        NodeState.COMPLETED,
        NodeState.FAILED,
        NodeState.CANCELLED,
    }
    
    # States that indicate successful completion
    SUCCESS_STATES: set[NodeState] = {NodeState.COMPLETED}
    
    # States that indicate failure
    FAILURE_STATES: set[NodeState] = {
        NodeState.FAILED,
        NodeState.CANCELLED,
    }
    
    def __init__(self, initial_state: NodeState = NodeState.PENDING):
        self._state = initial_state
        self._history: list[StateTransition] = []
    
    @property
    def state(self) -> NodeState:
        """Get current state."""
        return self._state
    
    @property
    def history(self) -> list[StateTransition]:
        """Get state transition history."""
        return self._history.copy()
    
    @property
    def is_terminal(self) -> bool:
        """Check if current state is terminal."""
        return self._state in self.TERMINAL_STATES
    
    @property
    def is_success(self) -> bool:
        """Check if node completed successfully."""
        return self._state in self.SUCCESS_STATES
    
    @property
    def is_failure(self) -> bool:
        """Check if node failed."""
        return self._state in self.FAILURE_STATES
    
    def can_transition_to(self, to_state: NodeState) -> bool:
        """Check if transition to given state is valid."""
        return to_state in self.VALID_TRANSITIONS.get(self._state, set())
    
    def get_valid_transitions(self) -> set[NodeState]:
        """Get all valid transitions from current state."""
        return self.VALID_TRANSITIONS.get(self._state, set()).copy()
    
    def transition(
        self,
        to_state: NodeState,
        reason: Optional[str] = None,
        triggered_by: Optional[str] = None,
        guard: Optional[TransitionGuard] = None,
        metadata: Optional[dict] = None,
    ) -> StateTransition:
        """
        Transition to a new state.
        
        Args:
            to_state: Target state
            reason: Reason for transition
            triggered_by: Who/what triggered the transition
            guard: Optional guard function that must return True
            metadata: Additional metadata for the transition
            
        Returns:
            StateTransition record
            
        Raises:
            InvalidStateTransitionError: If transition is not valid
        """
        if not self.can_transition_to(to_state):
            raise InvalidStateTransitionError(
                self._state.value,
                to_state.value,
                f"Valid transitions: {self.get_valid_transitions()}"
            )
        
        # Check guard condition if provided
        if guard is not None and not guard():
            raise InvalidStateTransitionError(
                self._state.value,
                to_state.value,
                "Guard condition failed"
            )
        
        # Record transition
        transition = StateTransition(
            from_state=self._state.value,
            to_state=to_state.value,
            reason=reason,
            triggered_by=triggered_by,
            metadata=metadata or {},
        )
        
        self._history.append(transition)
        self._state = to_state
        
        return transition


class WorkflowStateMachine:
    """
    State machine for workflow execution states.
    
    Defines valid transitions and provides transition guards.
    """
    
    # Valid state transitions: from_state -> [valid_to_states]
    VALID_TRANSITIONS: dict[WorkflowState, set[WorkflowState]] = {
        WorkflowState.PENDING: {WorkflowState.RUNNING, WorkflowState.CANCELLED},
        WorkflowState.RUNNING: {
            WorkflowState.COMPLETED,
            WorkflowState.FAILED,
            WorkflowState.CANCELLED,
            WorkflowState.PAUSED,
        },
        WorkflowState.PAUSED: {WorkflowState.RUNNING, WorkflowState.CANCELLED},
        WorkflowState.COMPLETED: set(),  # Terminal state
        WorkflowState.FAILED: set(),     # Terminal state
        WorkflowState.CANCELLED: set(),  # Terminal state
    }
    
    # Terminal states - no further transitions allowed
    TERMINAL_STATES: set[WorkflowState] = {
        WorkflowState.COMPLETED,
        WorkflowState.FAILED,
        WorkflowState.CANCELLED,
    }
    
    # States that indicate successful completion
    SUCCESS_STATES: set[WorkflowState] = {WorkflowState.COMPLETED}
    
    # States that indicate failure
    FAILURE_STATES: set[WorkflowState] = {
        WorkflowState.FAILED,
        WorkflowState.CANCELLED,
    }
    
    def __init__(self, initial_state: WorkflowState = WorkflowState.PENDING):
        self._state = initial_state
        self._history: list[StateTransition] = []
    
    @property
    def state(self) -> WorkflowState:
        """Get current state."""
        return self._state
    
    @property
    def history(self) -> list[StateTransition]:
        """Get state transition history."""
        return self._history.copy()
    
    @property
    def is_terminal(self) -> bool:
        """Check if current state is terminal."""
        return self._state in self.TERMINAL_STATES
    
    @property
    def is_success(self) -> bool:
        """Check if workflow completed successfully."""
        return self._state in self.SUCCESS_STATES
    
    @property
    def is_failure(self) -> bool:
        """Check if workflow failed."""
        return self._state in self.FAILURE_STATES
    
    @property
    def is_active(self) -> bool:
        """Check if workflow is actively processing."""
        return self._state == WorkflowState.RUNNING
    
    def can_transition_to(self, to_state: WorkflowState) -> bool:
        """Check if transition to given state is valid."""
        return to_state in self.VALID_TRANSITIONS.get(self._state, set())
    
    def get_valid_transitions(self) -> set[WorkflowState]:
        """Get all valid transitions from current state."""
        return self.VALID_TRANSITIONS.get(self._state, set()).copy()
    
    def transition(
        self,
        to_state: WorkflowState,
        reason: Optional[str] = None,
        triggered_by: Optional[str] = None,
        guard: Optional[TransitionGuard] = None,
        metadata: Optional[dict] = None,
    ) -> StateTransition:
        """
        Transition to a new state.
        
        Args:
            to_state: Target state
            reason: Reason for transition
            triggered_by: Who/what triggered the transition
            guard: Optional guard function that must return True
            metadata: Additional metadata for the transition
            
        Returns:
            StateTransition record
            
        Raises:
            InvalidStateTransitionError: If transition is not valid
        """
        if not self.can_transition_to(to_state):
            raise InvalidStateTransitionError(
                self._state.value,
                to_state.value,
                f"Valid transitions: {self.get_valid_transitions()}"
            )
        
        # Check guard condition if provided
        if guard is not None and not guard():
            raise InvalidStateTransitionError(
                self._state.value,
                to_state.value,
                "Guard condition failed"
            )
        
        # Record transition
        transition = StateTransition(
            from_state=self._state.value,
            to_state=to_state.value,
            reason=reason,
            triggered_by=triggered_by,
            metadata=metadata or {},
        )
        
        self._history.append(transition)
        self._state = to_state
        
        return transition


def compute_workflow_state_from_nodes(
    node_states: dict[str, NodeState],
    all_nodes: set[str],
) -> WorkflowState:
    """
    Compute the overall workflow state based on individual node states.
    
    Args:
        node_states: Mapping of node_id to its current state
        all_nodes: Set of all node IDs in the workflow
        
    Returns:
        Computed WorkflowState
    """
    if not node_states:
        return WorkflowState.PENDING
    
    states = set(node_states.values())
    
    # If any node has failed/cancelled, workflow is in that state
    if NodeState.FAILED in states:
        return WorkflowState.FAILED
    if NodeState.CANCELLED in states:
        return WorkflowState.CANCELLED
    
    # If all nodes are paused or a mix of completed and paused
    active_states = {NodeState.PENDING, NodeState.QUEUED, NodeState.RUNNING}
    if states.issubset({NodeState.PAUSED, NodeState.COMPLETED}):
        if NodeState.PAUSED in states:
            return WorkflowState.PAUSED
    
    # If all nodes are completed, workflow is completed
    if states == {NodeState.COMPLETED} and len(node_states) == len(all_nodes):
        return WorkflowState.COMPLETED
    
    # If any node is running, queued, or pending, workflow is running
    if states & active_states:
        return WorkflowState.RUNNING
    
    # Default to running if some nodes complete but not all
    return WorkflowState.RUNNING
