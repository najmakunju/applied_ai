"""
Fan-in coordination using Redis Lua scripts.

Handles atomic counter decrements to prevent race conditions
when multiple parallel branches complete simultaneously.

Includes recovery support to reconstruct fan-in state from PostgreSQL
when Redis fails.
"""

import json
import logging
from typing import Any, Optional
from uuid import UUID

import redis.asyncio as redis

logger = logging.getLogger(__name__)


# Lua script for atomic fan-in coordination
# Decrements counter and returns 1 if counter reaches 0 (trigger downstream)
FAN_IN_DECREMENT_SCRIPT = """
local key = KEYS[1]
local current = redis.call("GET", key)

if current == nil then
    return -1  -- Counter not initialized
end

local new_value = redis.call("DECR", key)

if new_value == 0 then
    return 1  -- All dependencies complete, trigger downstream
elseif new_value < 0 then
    return -2  -- Already triggered (race condition handled)
else
    return 0  -- Still waiting for other dependencies
end
"""


class FanInCoordinator:
    """
    Coordinates fan-in scenarios where multiple parallel branches
    converge on a single node.
    
    Uses Redis Lua scripts for atomic operations to prevent race conditions.
    """
    
    COUNTER_PREFIX = "wf:fanin:"
    OUTPUTS_PREFIX = "wf:fanin_outputs:"
    
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        self._decrement_script: Optional[str] = None
    
    async def init(self) -> None:
        """Initialize Lua scripts."""
        self._decrement_script = self.redis.register_script(FAN_IN_DECREMENT_SCRIPT)
    
    def _counter_key(self, workflow_execution_id: UUID, node_id: str) -> str:
        """Get Redis key for fan-in counter."""
        return f"{self.COUNTER_PREFIX}{workflow_execution_id}:{node_id}"
    
    def _outputs_key(self, workflow_execution_id: UUID, node_id: str) -> str:
        """Get Redis key for collecting fan-in outputs."""
        return f"{self.OUTPUTS_PREFIX}{workflow_execution_id}:{node_id}"
    
    async def initialize_fan_in(
        self,
        workflow_execution_id: UUID,
        node_id: str,
        dependency_count: int,
    ) -> None:
        """
        Initialize fan-in counter for a node.
        
        Called when a node with multiple dependencies is identified.
        """
        key = self._counter_key(workflow_execution_id, node_id)
        await self.redis.set(key, dependency_count)
        
        logger.debug(
            f"Initialized fan-in counter for {node_id}: {dependency_count} dependencies"
        )
    
    async def dependency_completed(
        self,
        workflow_execution_id: UUID,
        target_node_id: str,
        completed_node_id: str,
        output_data: dict,
    ) -> bool:
        """
        Signal that a dependency has completed.
        
        Atomically decrements the counter and stores the output.
        
        Args:
            workflow_execution_id: Workflow execution ID
            target_node_id: The node waiting for dependencies
            completed_node_id: The dependency that just completed
            output_data: Output from the completed dependency
            
        Returns:
            True if all dependencies are now complete and target should run
        """
        counter_key = self._counter_key(workflow_execution_id, target_node_id)
        outputs_key = self._outputs_key(workflow_execution_id, target_node_id)
        
        # Store output from completed dependency
        import json
        await self.redis.hset(outputs_key, completed_node_id, json.dumps(output_data))
        
        # Atomically decrement counter
        if self._decrement_script is None:
            await self.init()
        
        result = await self._decrement_script(keys=[counter_key])
        
        if result == 1:
            logger.info(
                f"All dependencies complete for {target_node_id}, triggering execution"
            )
            return True
        elif result == -1:
            logger.warning(f"Fan-in counter not initialized for {target_node_id}")
            return False
        elif result == -2:
            logger.debug(f"Fan-in for {target_node_id} already triggered (race handled)")
            return False
        else:
            logger.debug(
                f"Dependency {completed_node_id} complete for {target_node_id}, "
                f"waiting for {result} more"
            )
            return False
    
    async def get_aggregated_outputs(
        self,
        workflow_execution_id: UUID,
        node_id: str,
    ) -> dict[str, dict]:
        """
        Get all collected outputs for a fan-in node.
        
        Returns mapping of dependency_node_id -> output_data.
        """
        outputs_key = self._outputs_key(workflow_execution_id, node_id)
        
        raw_outputs = await self.redis.hgetall(outputs_key)
        
        import json
        return {
            dep_id: json.loads(output)
            for dep_id, output in raw_outputs.items()
        }
    
    async def cleanup(
        self,
        workflow_execution_id: UUID,
        node_id: str,
    ) -> None:
        """Clean up fan-in state after node execution."""
        counter_key = self._counter_key(workflow_execution_id, node_id)
        outputs_key = self._outputs_key(workflow_execution_id, node_id)
        
        await self.redis.delete(counter_key, outputs_key)
    
    async def get_pending_count(
        self,
        workflow_execution_id: UUID,
        node_id: str,
    ) -> int:
        """Get current pending dependency count."""
        key = self._counter_key(workflow_execution_id, node_id)
        value = await self.redis.get(key)
        return int(value) if value else 0
    
    async def recover_fan_in_state(
        self,
        workflow_execution_id: UUID,
        node_id: str,
        total_dependencies: int,
        completed_dependency_ids: list[str],
        completed_outputs: dict[str, dict[str, Any]],
    ) -> int:
        """
        Recover fan-in state from persisted node states.
        
        Called during crash recovery to reconstruct Redis state from PostgreSQL.
        
        Args:
            workflow_execution_id: Workflow execution ID
            node_id: The fan-in node ID
            total_dependencies: Total number of dependencies for this node
            completed_dependency_ids: List of dependency node IDs that completed
            completed_outputs: Map of completed_node_id -> output_data
            
        Returns:
            The remaining count (0 means ready to trigger)
        """
        remaining = total_dependencies - len(completed_dependency_ids)
        
        if remaining < 0:
            logger.warning(
                f"Recovery: More completed deps ({len(completed_dependency_ids)}) "
                f"than total ({total_dependencies}) for node {node_id}"
            )
            remaining = 0
        
        counter_key = self._counter_key(workflow_execution_id, node_id)
        outputs_key = self._outputs_key(workflow_execution_id, node_id)
        
        # Use pipeline for atomic restoration
        async with self.redis.pipeline(transaction=True) as pipe:
            # Set the counter to remaining count
            pipe.set(counter_key, remaining)
            
            # Restore completed outputs
            for dep_id, output in completed_outputs.items():
                pipe.hset(outputs_key, dep_id, json.dumps(output))
            
            await pipe.execute()
        
        logger.info(
            f"Recovery: Restored fan-in state for {node_id}: "
            f"{len(completed_dependency_ids)}/{total_dependencies} complete, "
            f"{remaining} remaining"
        )
        
        return remaining
    
    async def check_and_recover_counter(
        self,
        workflow_execution_id: UUID,
        node_id: str,
        total_dependencies: int,
        completed_dependency_ids: list[str],
        completed_outputs: dict[str, dict[str, Any]],
    ) -> tuple[bool, int]:
        """
        Check if counter exists, recover if missing.
        
        This is a safe method to call before dependency_completed() to ensure
        the counter is properly initialized.
        
        Args:
            workflow_execution_id: Workflow execution ID
            node_id: The fan-in node ID
            total_dependencies: Total number of dependencies
            completed_dependency_ids: Already completed dependency node IDs
            completed_outputs: Outputs from completed dependencies
            
        Returns:
            Tuple of (was_recovered, remaining_count)
        """
        counter_key = self._counter_key(workflow_execution_id, node_id)
        
        # Check if counter exists
        exists = await self.redis.exists(counter_key)
        
        if exists:
            value = await self.redis.get(counter_key)
            return False, int(value) if value else 0
        
        # Counter missing - recover from node states
        logger.warning(
            f"Fan-in counter missing for {node_id}, recovering from persisted state"
        )
        
        remaining = await self.recover_fan_in_state(
            workflow_execution_id,
            node_id,
            total_dependencies,
            completed_dependency_ids,
            completed_outputs,
        )
        
        return True, remaining
    
    async def is_counter_initialized(
        self,
        workflow_execution_id: UUID,
        node_id: str,
    ) -> bool:
        """Check if fan-in counter is initialized for a node."""
        key = self._counter_key(workflow_execution_id, node_id)
        return bool(await self.redis.exists(key))
