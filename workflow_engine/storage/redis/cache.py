"""
Redis cache layer for hot operational data.

Redis is used as a cache layer - PostgreSQL is the source of truth.
"""

import json
from datetime import datetime
from typing import Any, Optional
from uuid import UUID

import redis.asyncio as redis

from workflow_engine.config import get_settings


class RedisCache:
    """
    Redis cache for workflow execution state.
    
    Provides fast access to active workflow data.
    """
    
    # Key prefixes
    WORKFLOW_EXEC_PREFIX = "wf:exec:"
    NODE_EXEC_PREFIX = "wf:node:"
    WORKER_PREFIX = "wf:worker:"
    
    def __init__(self, client: redis.Redis):
        self.client = client
        self.settings = get_settings()
    
    # ==================== Workflow Execution Cache ====================
    
    async def cache_workflow_execution(
        self,
        execution_id: UUID,
        data: dict[str, Any],
        ttl: Optional[int] = None,
    ) -> None:
        """Cache workflow execution state."""
        key = f"{self.WORKFLOW_EXEC_PREFIX}{execution_id}"
        ttl = ttl or 3600  # Default 1 hour TTL
        
        await self.client.setex(
            key,
            ttl,
            json.dumps(data, default=str),
        )
    
    async def get_workflow_execution(
        self,
        execution_id: UUID,
    ) -> Optional[dict[str, Any]]:
        """Get cached workflow execution state."""
        key = f"{self.WORKFLOW_EXEC_PREFIX}{execution_id}"
        data = await self.client.get(key)
        
        if data:
            return json.loads(data)
        return None
    
    async def delete_workflow_execution(
        self,
        execution_id: UUID,
    ) -> None:
        """Remove workflow execution from cache after completion."""
        key = f"{self.WORKFLOW_EXEC_PREFIX}{execution_id}"
        await self.client.delete(key)
    
    async def is_node_completed(
        self,
        workflow_execution_id: UUID,
        node_id: str,
    ) -> bool:
        """
        Check if a node is already completed.
        
        Used by workers to skip duplicate tasks after orchestrator recovery.
        Returns False if workflow not in cache (conservative - process the task).
        """
        cached = await self.get_workflow_execution(workflow_execution_id)
        if not cached:
            # Workflow not in cache - can't determine, let task proceed
            return False
        
        node_executions = cached.get("node_executions", {})
        node_data = node_executions.get(node_id)
        
        if not node_data:
            return False
        
        return node_data.get("state") == "COMPLETED"
    
    # ==================== Node Execution Cache ====================
    
    async def cache_node_execution(
        self,
        workflow_execution_id: UUID,
        node_id: str,
        data: dict[str, Any],
        ttl: Optional[int] = None,
    ) -> None:
        """Cache node execution state."""
        key = f"{self.NODE_EXEC_PREFIX}{workflow_execution_id}:{node_id}"
        ttl = ttl or 3600
        
        await self.client.setex(
            key,
            ttl,
            json.dumps(data, default=str),
        )
    
    # ==================== Worker Registry ====================
    
    async def register_worker(
        self,
        worker_id: str,
        capabilities: list[str],
        metadata: Optional[dict[str, Any]] = None,
    ) -> None:
        """Register a worker with heartbeat."""
        key = f"{self.WORKER_PREFIX}{worker_id}"
        data = {
            "worker_id": worker_id,
            "capabilities": capabilities,
            "metadata": metadata or {},
            "registered_at": datetime.utcnow().isoformat(),
            "last_heartbeat": datetime.utcnow().isoformat(),
            "status": "active",
        }
        
        # TTL slightly longer than heartbeat timeout
        ttl = int(self.settings.worker.heartbeat_timeout * 2)
        await self.client.setex(key, ttl, json.dumps(data, default=str))
    
    async def worker_heartbeat(
        self,
        worker_id: str,
    ) -> bool:
        """Update worker heartbeat. Returns False if worker not registered."""
        key = f"{self.WORKER_PREFIX}{worker_id}"
        
        data = await self.client.get(key)
        if not data:
            return False
        
        worker = json.loads(data)
        worker["last_heartbeat"] = datetime.utcnow().isoformat()
        
        ttl = int(self.settings.worker.heartbeat_timeout * 2)
        await self.client.setex(key, ttl, json.dumps(worker, default=str))
        return True
    
    async def deregister_worker(
        self,
        worker_id: str,
    ) -> None:
        """Remove worker registration."""
        key = f"{self.WORKER_PREFIX}{worker_id}"
        await self.client.delete(key)
    
    async def get_active_workers(self) -> list[dict[str, Any]]:
        """Get all active workers."""
        pattern = f"{self.WORKER_PREFIX}*"
        workers = []
        
        async for key in self.client.scan_iter(match=pattern):
            data = await self.client.get(key)
            if data:
                workers.append(json.loads(data))
        
        return workers
