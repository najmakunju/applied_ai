"""
Sandboxed template resolution engine.

Resolves {{ node.output }} syntax safely without eval/exec.
"""

import re
from dataclasses import dataclass
from typing import Any, Optional


class TemplateValidationError(Exception):
    """Raised when template validation fails."""
    
    def __init__(self, message: str, template: str, position: Optional[int] = None):
        self.template = template
        self.position = position
        super().__init__(message)


@dataclass
class TemplateReference:
    """Represents a parsed template reference."""
    
    full_match: str
    node_id: str
    path: list[str]  # Path to the value (e.g., ["output", "data", "id"])
    start_pos: int
    end_pos: int


class TemplateResolver:
    """
    Resolves template expressions safely.
    
    Supports:
    - {{ node_id.output }} - Get entire output of a node
    - {{ node_id.output.key }} - Get specific key from output
    - {{ node_id.output.nested.path }} - Access nested values
    - {{ input.param_name }} - Access workflow input parameters
    
    Security:
    - No eval/exec
    - Restricted to node outputs and input parameters
    - Path traversal only through dict keys and list indices
    """
    
    # Pattern to match {{ reference }}
    TEMPLATE_PATTERN = re.compile(r"\{\{\s*([^}]+?)\s*\}\}")
    
    # Pattern to validate reference format
    REFERENCE_PATTERN = re.compile(r"^([a-zA-Z_][a-zA-Z0-9_\-]*)(?:\.([a-zA-Z_][a-zA-Z0-9_.\[\]]*)?)?$")
    
    # Allowed root references
    ALLOWED_ROOTS = {"input", "env"}
    
    def __init__(
        self,
        node_outputs: dict[str, dict[str, Any]],
        input_params: Optional[dict[str, Any]] = None,
        env_vars: Optional[dict[str, str]] = None,
    ):
        """
        Initialize resolver.
        
        Args:
            node_outputs: Mapping of node_id to their outputs
            input_params: Workflow input parameters
            env_vars: Safe environment variables (whitelist only)
        """
        self.node_outputs = node_outputs
        self.input_params = input_params or {}
        self.env_vars = env_vars or {}
    
    def resolve(self, template: str | dict | list) -> Any:
        """
        Resolve all template expressions in a value.
        
        Handles strings, dicts, and lists recursively.
        """
        if isinstance(template, str):
            return self._resolve_string(template)
        elif isinstance(template, dict):
            return {k: self.resolve(v) for k, v in template.items()}
        elif isinstance(template, list):
            return [self.resolve(v) for v in template]
        else:
            return template
    
    def _resolve_string(self, template: str) -> Any:
        """Resolve template expressions in a string."""
        # Find all template references
        references = self.find_references(template)
        
        if not references:
            return template
        
        # If the entire string is a single reference, return the actual value
        # This preserves types (dict, list, etc.) instead of converting to string
        if len(references) == 1 and references[0].full_match == template.strip():
            return self._resolve_reference(references[0])
        
        # Otherwise, replace references in string
        result = template
        for ref in reversed(references):  # Reverse to maintain positions
            value = self._resolve_reference(ref)
            # Convert value to string for embedding
            str_value = str(value) if value is not None else ""
            result = result[:ref.start_pos] + str_value + result[ref.end_pos:]
        
        return result
    
    def find_references(self, template: str) -> list[TemplateReference]:
        """Find all template references in a string."""
        references = []
        
        for match in self.TEMPLATE_PATTERN.finditer(template):
            full_match = match.group(0)
            reference = match.group(1).strip()
            
            # Parse the reference
            parsed = self._parse_reference(reference)
            if parsed:
                references.append(TemplateReference(
                    full_match=full_match,
                    node_id=parsed[0],
                    path=parsed[1],
                    start_pos=match.start(),
                    end_pos=match.end(),
                ))
        
        return references
    
    def _parse_reference(self, reference: str) -> Optional[tuple[str, list[str]]]:
        """
        Parse a reference string into (node_id, path).
        
        Examples:
            "node1.output" -> ("node1", ["output"])
            "node1.output.data.id" -> ("node1", ["output", "data", "id"])
            "input.user_id" -> ("input", ["user_id"])
        """
        match = self.REFERENCE_PATTERN.match(reference)
        if not match:
            return None
        
        node_id = match.group(1)
        path_str = match.group(2) or ""
        
        # Parse path, handling array indices
        path = []
        if path_str:
            # Split by dots but handle array indices
            parts = re.split(r"\.(?![^\[]*\])", path_str)
            for part in parts:
                # Check for array index
                array_match = re.match(r"([a-zA-Z_][a-zA-Z0-9_]*)\[(\d+)\]", part)
                if array_match:
                    path.append(array_match.group(1))
                    path.append(int(array_match.group(2)))
                else:
                    path.append(part)
        
        return (node_id, path)
    
    def _resolve_reference(self, ref: TemplateReference) -> Any:
        """Resolve a single reference to its value."""
        node_id = ref.node_id
        path = ref.path
        
        # Get root value
        if node_id == "input":
            root = self.input_params
        elif node_id == "env":
            root = self.env_vars
        elif node_id in self.node_outputs:
            root = self.node_outputs[node_id]
        else:
            # Node output not available yet
            return None
        
        # Navigate path
        return self._navigate_path(root, path)
    
    def _navigate_path(self, value: Any, path: list) -> Any:
        """
        Navigate a path through nested data structures.
        
        Security: Only dict-based navigation and list indexing are allowed.
        No attribute access (getattr) to prevent potential security issues
        like accessing object methods or private attributes.
        """
        current = value
        
        for key in path:
            if current is None:
                return None
            
            if isinstance(key, int):
                # Array index
                if isinstance(current, list) and 0 <= key < len(current):
                    current = current[key]
                else:
                    return None
            elif isinstance(current, dict):
                current = current.get(key)
            else:
                # Security: Do NOT use getattr() to avoid accessing object
                # methods or private attributes. Only dict/list navigation
                # is allowed for template resolution.
                return None
        
        return current


def resolve_node_input(
    node_config: dict[str, Any],
    completed_outputs: dict[str, dict[str, Any]],
    input_params: dict[str, Any],
) -> dict[str, Any]:
    """
    Resolve all templates in a node's configuration.
    
    Convenience function for the orchestrator.
    """
    resolver = TemplateResolver(
        node_outputs=completed_outputs,
        input_params=input_params,
    )
    
    return resolver.resolve(node_config)
