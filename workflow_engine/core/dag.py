"""
DAG (Directed Acyclic Graph) validation and parsing.

Implements cycle detection using Kahn's algorithm and structure validation.
"""

from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any, Optional

from workflow_engine.core.models import DAGDefinition, NodeDefinition, WorkflowDefinition


@dataclass
class ValidationError:
    """Represents a single validation error."""
    
    code: str
    message: str
    node_id: Optional[str] = None
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class ValidationResult:
    """Result of DAG validation."""
    
    is_valid: bool
    errors: list[ValidationError] = field(default_factory=list)
    warnings: list[ValidationError] = field(default_factory=list)
    
    # Computed graph properties (populated on successful validation)
    topological_order: list[str] = field(default_factory=list)
    levels: dict[str, int] = field(default_factory=dict)  # node_id -> level in DAG
    critical_path: list[str] = field(default_factory=list)
    
    def add_error(
        self,
        code: str,
        message: str,
        node_id: Optional[str] = None,
        **details: Any,
    ) -> None:
        """Add a validation error."""
        self.errors.append(ValidationError(code, message, node_id, details))
        self.is_valid = False
    
    def add_warning(
        self,
        code: str,
        message: str,
        node_id: Optional[str] = None,
        **details: Any,
    ) -> None:
        """Add a validation warning."""
        self.warnings.append(ValidationError(code, message, node_id, details))


class DAGValidator:
    """
    Validates DAG structure and detects cycles.
    
    Uses Kahn's algorithm for topological sorting and cycle detection.
    """
    
    def __init__(self, dag: DAGDefinition):
        self.dag = dag
        self._adjacency_list: dict[str, list[str]] = defaultdict(list)
        self._reverse_adjacency: dict[str, list[str]] = defaultdict(list)
        self._in_degree: dict[str, int] = {}
        self._node_map: dict[str, NodeDefinition] = {}
        
        self._build_graph()
    
    def _build_graph(self) -> None:
        """Build internal graph representation."""
        # Build node map
        for node in self.dag.nodes:
            self._node_map[node.id] = node
            self._in_degree[node.id] = len(node.dependencies)
        
        # Build adjacency lists
        for node in self.dag.nodes:
            for dep in node.dependencies:
                # dep -> node (forward edge)
                self._adjacency_list[dep].append(node.id)
                # node -> dep (reverse edge for dependency lookup)
                self._reverse_adjacency[node.id].append(dep)
    
    def validate(self) -> ValidationResult:
        """
        Perform full validation of the DAG.
        
        Returns:
            ValidationResult with errors, warnings, and computed properties
        """
        result = ValidationResult(is_valid=True)
        
        # Run all validation checks
        self._validate_node_references(result)
        self._validate_no_self_loops(result)
        self._detect_cycles_and_compute_order(result)
        self._validate_input_output_nodes(result)
        self._compute_levels(result)
        self._check_unreachable_nodes(result)
        
        return result
    
    def _validate_node_references(self, result: ValidationResult) -> None:
        """Validate that all dependency references point to existing nodes."""
        node_ids = set(self._node_map.keys())
        
        for node in self.dag.nodes:
            for dep in node.dependencies:
                if dep not in node_ids:
                    result.add_error(
                        code="INVALID_DEPENDENCY",
                        message=f"Node '{node.id}' references non-existent dependency '{dep}'",
                        node_id=node.id,
                        dependency=dep,
                    )
    
    def _validate_no_self_loops(self, result: ValidationResult) -> None:
        """Check for self-referential dependencies."""
        for node in self.dag.nodes:
            if node.id in node.dependencies:
                result.add_error(
                    code="SELF_LOOP",
                    message=f"Node '{node.id}' has a self-referential dependency",
                    node_id=node.id,
                )
    
    def _detect_cycles_and_compute_order(self, result: ValidationResult) -> None:
        """
        Detect cycles using Kahn's algorithm and compute topological order.
        
        Kahn's Algorithm:
        1. Find all nodes with in-degree 0
        2. Remove them and their outgoing edges
        3. Repeat until no nodes left
        4. If nodes remain, there's a cycle
        """
        # Skip if there are already errors
        if not result.is_valid:
            return
        
        # Create working copy of in-degrees
        in_degree = self._in_degree.copy()
        
        # Find all nodes with no incoming edges (in-degree 0)
        queue = deque([
            node_id for node_id, degree in in_degree.items()
            if degree == 0
        ])
        
        topological_order: list[str] = []
        
        while queue:
            # Process node with in-degree 0
            node_id = queue.popleft()
            topological_order.append(node_id)
            
            # "Remove" this node by decrementing in-degrees of neighbors
            for neighbor in self._adjacency_list[node_id]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)
        
        # If not all nodes are processed, there's a cycle
        if len(topological_order) != len(self._node_map):
            # Find nodes involved in cycles
            remaining = set(self._node_map.keys()) - set(topological_order)
            cycle_nodes = self._find_cycle_nodes(remaining)
            
            result.add_error(
                code="CYCLE_DETECTED",
                message=f"Workflow contains circular dependencies involving nodes: {cycle_nodes}",
                cycle_nodes=list(cycle_nodes),
            )
        else:
            result.topological_order = topological_order
    
    def _find_cycle_nodes(self, candidates: set[str]) -> list[str]:
        """Find nodes that are part of a cycle using DFS."""
        # Use DFS to find a cycle
        visited = set()
        rec_stack = set()
        cycle_path: list[str] = []
        
        def dfs(node: str, path: list[str]) -> bool:
            visited.add(node)
            rec_stack.add(node)
            path.append(node)
            
            for neighbor in self._adjacency_list.get(node, []):
                if neighbor not in visited:
                    if dfs(neighbor, path):
                        return True
                elif neighbor in rec_stack:
                    # Found cycle - extract cycle portion
                    cycle_start = path.index(neighbor)
                    cycle_path.extend(path[cycle_start:])
                    return True
            
            path.pop()
            rec_stack.remove(node)
            return False
        
        for node in candidates:
            if node not in visited:
                if dfs(node, []):
                    break
        
        return cycle_path if cycle_path else list(candidates)
    
    def _validate_input_output_nodes(self, result: ValidationResult) -> None:
        """Validate that workflow has proper entry and exit points."""
        root_nodes = self.dag.get_root_nodes()
        leaf_nodes = self.dag.get_leaf_nodes()
        
        # Check for input nodes
        input_nodes = [n for n in root_nodes if n.handler.value == "input"]
        if not input_nodes and root_nodes:
            result.add_warning(
                code="NO_INPUT_NODE",
                message="Workflow has no explicit 'input' handler node. "
                        f"Entry points: {[n.id for n in root_nodes]}",
            )
        
        # Check for output nodes
        output_nodes = [n for n in leaf_nodes if n.handler.value == "output"]
        if not output_nodes and leaf_nodes:
            result.add_warning(
                code="NO_OUTPUT_NODE",
                message="Workflow has no explicit 'output' handler node. "
                        f"Exit points: {[n.id for n in leaf_nodes]}",
            )
    
    def _compute_levels(self, result: ValidationResult) -> None:
        """Compute the level (depth) of each node in the DAG."""
        if not result.topological_order:
            return
        
        levels: dict[str, int] = {}
        
        for node_id in result.topological_order:
            node = self._node_map[node_id]
            if not node.dependencies:
                levels[node_id] = 0
            else:
                # Level is max level of dependencies + 1
                max_dep_level = max(
                    levels.get(dep, 0) for dep in node.dependencies
                )
                levels[node_id] = max_dep_level + 1
        
        result.levels = levels
    
    def _check_unreachable_nodes(self, result: ValidationResult) -> None:
        """Check for nodes that cannot be reached from any root node."""
        if not result.topological_order:
            return
        
        # BFS from all root nodes
        root_nodes = [n.id for n in self.dag.get_root_nodes()]
        reachable = set(root_nodes)
        queue = deque(root_nodes)
        
        while queue:
            node_id = queue.popleft()
            for neighbor in self._adjacency_list[node_id]:
                if neighbor not in reachable:
                    reachable.add(neighbor)
                    queue.append(neighbor)
        
        unreachable = set(self._node_map.keys()) - reachable
        if unreachable:
            result.add_warning(
                code="UNREACHABLE_NODES",
                message=f"Nodes {list(unreachable)} are not reachable from any root node",
                unreachable_nodes=list(unreachable),
            )


class DAGParser:
    """
    Parses and validates workflow definitions.
    
    Provides utilities for working with validated DAGs.
    """
    
    def __init__(self, definition: WorkflowDefinition):
        self.definition = definition
        self.validator = DAGValidator(definition.dag)
        self._validation_result: Optional[ValidationResult] = None
    
    @property
    def validation_result(self) -> ValidationResult:
        """Get validation result, running validation if needed."""
        if self._validation_result is None:
            self._validation_result = self.validator.validate()
        return self._validation_result
    
    @property
    def is_valid(self) -> bool:
        """Check if the workflow is valid."""
        return self.validation_result.is_valid
    
    def get_ready_nodes(self, completed_nodes: set[str]) -> list[NodeDefinition]:
        """
        Get nodes that are ready to execute.
        
        A node is ready if all its dependencies are in the completed set.
        
        Args:
            completed_nodes: Set of node IDs that have completed
            
        Returns:
            List of nodes ready for execution
        """
        ready = []
        for node in self.definition.dag.nodes:
            if node.id in completed_nodes:
                continue
            if all(dep in completed_nodes for dep in node.dependencies):
                ready.append(node)
        return ready
    
    def get_parallel_batches(self) -> list[list[str]]:
        """
        Get nodes grouped by level for parallel execution.
        
        Nodes in the same batch can be executed in parallel.
        
        Returns:
            List of batches, where each batch contains node IDs
        """
        if not self.validation_result.levels:
            return []
        
        # Group nodes by level
        level_groups: dict[int, list[str]] = defaultdict(list)
        for node_id, level in self.validation_result.levels.items():
            level_groups[level].append(node_id)
        
        # Return as ordered list of batches
        max_level = max(level_groups.keys()) if level_groups else 0
        return [level_groups[level] for level in range(max_level + 1)]
    
    def get_node(self, node_id: str) -> Optional[NodeDefinition]:
        """Get node definition by ID."""
        return self.definition.dag.get_node(node_id)
    
    def get_node_effective_retry_config(self, node_id: str):
        """Get effective retry config for a node (node-specific or default)."""
        node = self.get_node(node_id)
        if node and node.retry_config:
            return node.retry_config
        return self.definition.default_retry_config


def parse_workflow_json(workflow_json: dict) -> tuple[WorkflowDefinition, ValidationResult]:
    """
    Parse and validate a workflow JSON payload.
    
    Args:
        workflow_json: Raw workflow JSON
        
    Returns:
        Tuple of (WorkflowDefinition, ValidationResult)
        
    Raises:
        ValueError: If JSON cannot be parsed into WorkflowDefinition
    """
    # Parse JSON into WorkflowDefinition
    definition = WorkflowDefinition(**workflow_json)
    
    # Validate DAG
    parser = DAGParser(definition)
    validation_result = parser.validation_result
    
    return definition, validation_result
