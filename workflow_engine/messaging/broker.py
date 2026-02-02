"""
Message broker implementation using Redis Streams.

Provides reliable message delivery with consumer groups and acknowledgment.
"""

import json
from datetime import datetime
from typing import Any, Optional
from uuid import UUID

import redis.asyncio as redis

from workflow_engine.config import get_settings
from workflow_engine.core.models import TaskMessage


class TaskQueue:
    """
    Task queue implementation using Redis Streams.
    
    Supports consumer groups for reliable processing and dead letter queue.
    """
    
    STREAM_PREFIX = "wf:stream:"
    DLQ_PREFIX = "wf:dlq:"
    
    def __init__(
        self,
        client: redis.Redis,
        queue_name: str,
        consumer_group: str = "workers",
    ):
        self.client = client
        self.queue_name = queue_name
        self.consumer_group = consumer_group
        self.stream_key = f"{self.STREAM_PREFIX}{queue_name}"
        self.dlq_key = f"{self.DLQ_PREFIX}{queue_name}"
        self.settings = get_settings()
    
    async def init(self) -> None:
        """Initialize the stream and consumer group."""
        try:
            # Create consumer group (creates stream if not exists)
            await self.client.xgroup_create(
                self.stream_key,
                self.consumer_group,
                id="0",
                mkstream=True,
            )
        except redis.ResponseError as e:
            # Group already exists
            if "BUSYGROUP" not in str(e):
                raise
    
    async def publish(
        self,
        message: TaskMessage,
    ) -> str:
        """
        Publish a task message to the queue.
        
        Returns the stream message ID.
        """
        data = {
            "id": str(message.id),
            "node_execution_id": str(message.node_execution_id),
            "workflow_execution_id": str(message.workflow_execution_id),
            "node_id": message.node_id,
            "handler": message.handler.value,
            "config": json.dumps(message.config.model_dump()),
            "input_data": json.dumps(message.input_data),
            "attempt": str(message.attempt),
            "retry_config": json.dumps(message.retry_config.model_dump()),
            "idempotency_key": message.idempotency_key,
            "created_at": message.created_at.isoformat(),
        }
        
        if message.expires_at:
            data["expires_at"] = message.expires_at.isoformat()
        
        message_id = await self.client.xadd(self.stream_key, data)
        return message_id
    
    async def consume(
        self,
        consumer_id: str,
        count: int = 1,
        block_ms: int = 5000,
    ) -> list[tuple[str, dict[str, Any]]]:
        """
        Consume messages from the queue.
        
        Uses XREADGROUP for reliable processing with consumer groups.
        
        Returns list of (message_id, message_data) tuples.
        """
        try:
            messages = await self.client.xreadgroup(
                groupname=self.consumer_group,
                consumername=consumer_id,
                streams={self.stream_key: ">"},
                count=count,
                block=block_ms,
            )
            
            if not messages:
                return []
            
            # Parse messages
            result = []
            for _, stream_messages in messages:
                for msg_id, msg_data in stream_messages:
                    # Parse JSON fields
                    parsed = {
                        "id": msg_data["id"],
                        "node_execution_id": msg_data["node_execution_id"],
                        "workflow_execution_id": msg_data["workflow_execution_id"],
                        "node_id": msg_data["node_id"],
                        "handler": msg_data["handler"],
                        "config": json.loads(msg_data["config"]),
                        "input_data": json.loads(msg_data["input_data"]),
                        "attempt": int(msg_data["attempt"]),
                        "retry_config": json.loads(msg_data["retry_config"]),
                        "idempotency_key": msg_data["idempotency_key"],
                        "created_at": msg_data["created_at"],
                    }
                    if "expires_at" in msg_data:
                        parsed["expires_at"] = msg_data["expires_at"]
                    
                    result.append((msg_id, parsed))
            
            return result
            
        except redis.ResponseError as e:
            if "NOGROUP" in str(e):
                await self.init()
                return []
            raise
    
    async def acknowledge(
        self,
        message_id: str,
    ) -> None:
        """Acknowledge successful message processing."""
        await self.client.xack(self.stream_key, self.consumer_group, message_id)
    
    async def reject(
        self,
        message_id: str,
        message_data: dict[str, Any],
        error: str,
        move_to_dlq: bool = True,
    ) -> None:
        """
        Reject a message, optionally moving to dead letter queue.
        """
        # Acknowledge original message
        await self.client.xack(self.stream_key, self.consumer_group, message_id)
        
        if move_to_dlq:
            # Add to DLQ with error info
            dlq_data = {
                **message_data,
                "config": json.dumps(message_data["config"]),
                "input_data": json.dumps(message_data["input_data"]),
                "retry_config": json.dumps(message_data["retry_config"]),
                "original_message_id": message_id,
                "error": error,
                "rejected_at": datetime.utcnow().isoformat(),
            }
            await self.client.xadd(self.dlq_key, dlq_data)
    
    async def get_pending_count(self) -> int:
        """Get count of pending (unacknowledged) messages."""
        try:
            info = await self.client.xpending(self.stream_key, self.consumer_group)
            return info["pending"] if info else 0
        except redis.ResponseError:
            return 0
    
    async def get_dlq_count(self) -> int:
        """Get count of messages in dead letter queue."""
        return await self.client.xlen(self.dlq_key)
    
    async def claim_stale_messages(
        self,
        consumer_id: str,
        min_idle_ms: int = 60000,
        count: int = 10,
    ) -> list[tuple[str, dict[str, Any]]]:
        """
        Claim messages that have been idle (worker died).
        
        Used for automatic reassignment of stuck messages.
        """
        try:
            result = await self.client.xautoclaim(
                self.stream_key,
                self.consumer_group,
                consumer_id,
                min_idle_time=min_idle_ms,
                count=count,
            )
            
            if not result or len(result) < 2:
                return []
            
            # Parse claimed messages
            messages = []
            for msg_id, msg_data in result[1]:
                if msg_data:  # Skip deleted messages
                    parsed = {
                        "id": msg_data.get("id"),
                        "node_execution_id": msg_data.get("node_execution_id"),
                        "workflow_execution_id": msg_data.get("workflow_execution_id"),
                        "node_id": msg_data.get("node_id"),
                        "handler": msg_data.get("handler"),
                        "config": json.loads(msg_data.get("config", "{}")),
                        "input_data": json.loads(msg_data.get("input_data", "{}")),
                        "attempt": int(msg_data.get("attempt", 1)),
                        "retry_config": json.loads(msg_data.get("retry_config", "{}")),
                        "idempotency_key": msg_data.get("idempotency_key", ""),
                        "created_at": msg_data.get("created_at", ""),
                    }
                    messages.append((msg_id, parsed))
            
            return messages
            
        except redis.ResponseError:
            return []
    
    async def retry_from_dlq(
        self,
        count: int = 10,
    ) -> int:
        """
        Retry messages from dead letter queue.
        
        Returns count of messages retried.
        """
        retried = 0
        
        # Read from DLQ
        messages = await self.client.xrange(self.dlq_key, count=count)
        
        for msg_id, msg_data in messages:
            # Re-publish to main stream
            retry_data = {
                k: v for k, v in msg_data.items()
                if k not in ("original_message_id", "error", "rejected_at")
            }
            retry_data["attempt"] = str(int(msg_data.get("attempt", 1)) + 1)
            
            await self.client.xadd(self.stream_key, retry_data)
            await self.client.xdel(self.dlq_key, msg_id)
            retried += 1
        
        return retried


class ResultQueue:
    """
    Result queue implementation using Redis Streams.
    
    Workers publish task results to this stream, and orchestrators consume them.
    Supports consumer groups for multiple orchestrator instances.
    """
    
    STREAM_KEY = "wf:stream:results"
    CONSUMER_GROUP = "orchestrators"
    
    def __init__(self, client: redis.Redis):
        self.client = client
        self.settings = get_settings()
    
    async def init(self) -> None:
        """Initialize the stream and consumer group."""
        try:
            await self.client.xgroup_create(
                self.STREAM_KEY,
                self.CONSUMER_GROUP,
                id="0",
                mkstream=True,
            )
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise
    
    async def publish_result(
        self,
        workflow_execution_id: str,
        node_id: str,
        success: bool,
        output_data: Optional[dict] = None,
        error_message: Optional[str] = None,
        error_type: Optional[str] = None,
        is_retryable: bool = True,
        worker_id: str = "",
    ) -> str:
        """
        Publish a task result to the results stream.
        
        Returns the stream message ID.
        """
        data = {
            "workflow_execution_id": str(workflow_execution_id),
            "node_id": node_id,
            "success": "1" if success else "0",
            "output_data": json.dumps(output_data or {}),
            "error_message": error_message or "",
            "error_type": error_type or "",
            "is_retryable": "1" if is_retryable else "0",
            "worker_id": worker_id,
            "published_at": datetime.utcnow().isoformat(),
        }
        
        message_id = await self.client.xadd(self.STREAM_KEY, data)
        return message_id
    
    async def consume(
        self,
        consumer_id: str,
        count: int = 10,
        block_ms: int = 1000,
    ) -> list[tuple[str, dict[str, Any]]]:
        """
        Consume results from the stream.
        
        Returns list of (message_id, result_data) tuples.
        """
        try:
            messages = await self.client.xreadgroup(
                groupname=self.CONSUMER_GROUP,
                consumername=consumer_id,
                streams={self.STREAM_KEY: ">"},
                count=count,
                block=block_ms,
            )
            
            if not messages:
                return []
            
            results = []
            for _, stream_messages in messages:
                for msg_id, msg_data in stream_messages:
                    parsed = {
                        "workflow_execution_id": msg_data["workflow_execution_id"],
                        "node_id": msg_data["node_id"],
                        "success": msg_data["success"] == "1",
                        "output_data": json.loads(msg_data["output_data"]),
                        "error_message": msg_data["error_message"] or None,
                        "error_type": msg_data["error_type"] or None,
                        "is_retryable": msg_data["is_retryable"] == "1",
                        "worker_id": msg_data["worker_id"],
                    }
                    results.append((msg_id, parsed))
            
            return results
            
        except redis.ResponseError as e:
            if "NOGROUP" in str(e):
                await self.init()
                return []
            raise
    
    async def acknowledge(self, message_id: str) -> None:
        """Acknowledge successful result processing."""
        await self.client.xack(self.STREAM_KEY, self.CONSUMER_GROUP, message_id)
    
    async def get_pending_count(self) -> int:
        """Get count of pending (unacknowledged) results."""
        try:
            info = await self.client.xpending(self.STREAM_KEY, self.CONSUMER_GROUP)
            return info["pending"] if info else 0
        except redis.ResponseError:
            return 0
    
    async def claim_stale_results(
        self,
        consumer_id: str,
        min_idle_ms: int = 30000,
        count: int = 10,
    ) -> list[tuple[str, dict[str, Any]]]:
        """
        Claim results that have been idle (orchestrator died).
        
        Used for automatic reassignment of stuck results.
        """
        try:
            result = await self.client.xautoclaim(
                self.STREAM_KEY,
                self.CONSUMER_GROUP,
                consumer_id,
                min_idle_time=min_idle_ms,
                count=count,
            )
            
            if not result or len(result) < 2:
                return []
            
            results = []
            for msg_id, msg_data in result[1]:
                if msg_data:
                    parsed = {
                        "workflow_execution_id": msg_data.get("workflow_execution_id", ""),
                        "node_id": msg_data.get("node_id", ""),
                        "success": msg_data.get("success", "0") == "1",
                        "output_data": json.loads(msg_data.get("output_data", "{}")),
                        "error_message": msg_data.get("error_message") or None,
                        "error_type": msg_data.get("error_type") or None,
                        "is_retryable": msg_data.get("is_retryable", "1") == "1",
                        "worker_id": msg_data.get("worker_id", ""),
                    }
                    results.append((msg_id, parsed))
            
            return results
            
        except redis.ResponseError:
            return []


class MessageBroker:
    """
    High-level message broker for task distribution.
    
    Manages multiple task queues for different handler types.
    """
    
    def __init__(self, client: redis.Redis):
        self.client = client
        self._queues: dict[str, TaskQueue] = {}
        self._result_queue: Optional[ResultQueue] = None
    
    async def get_queue(
        self,
        queue_name: str,
        consumer_group: str = "workers",
    ) -> TaskQueue:
        """Get or create a task queue."""
        key = f"{queue_name}:{consumer_group}"
        
        if key not in self._queues:
            queue = TaskQueue(self.client, queue_name, consumer_group)
            await queue.init()
            self._queues[key] = queue
        
        return self._queues[key]
    
    async def publish_task(
        self,
        message: TaskMessage,
        queue_name: Optional[str] = None,
    ) -> str:
        """
        Publish a task message.
        
        If queue_name not specified, routes based on handler type.
        """
        if queue_name is None:
            queue_name = f"tasks:{message.handler.value}"
        
        queue = await self.get_queue(queue_name)
        return await queue.publish(message)
    
    async def get_result_queue(self) -> ResultQueue:
        """Get or create the result queue."""
        if self._result_queue is None:
            self._result_queue = ResultQueue(self.client)
            await self._result_queue.init()
        return self._result_queue
    
    async def get_queue_stats(self) -> dict[str, dict[str, int]]:
        """Get statistics for all queues."""
        stats = {}
        
        for key, queue in self._queues.items():
            stats[key] = {
                "pending": await queue.get_pending_count(),
                "dlq": await queue.get_dlq_count(),
            }
        
        # Include result queue stats
        if self._result_queue:
            stats["results"] = {
                "pending": await self._result_queue.get_pending_count(),
            }
        
        return stats
