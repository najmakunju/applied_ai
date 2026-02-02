"""
Base worker implementation.

Provides common worker functionality including heartbeat,
graceful shutdown, and idempotent execution.
"""

import asyncio
import logging
import signal
import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Optional

from workflow_engine.config import get_settings
from workflow_engine.core.models import TaskResult
from workflow_engine.messaging.broker import MessageBroker
from workflow_engine.messaging.consumer import TaskConsumer
from workflow_engine.storage.redis.cache import RedisCache
from workflow_engine.storage.redis.connection import get_redis

logger = logging.getLogger(__name__)


class BaseWorker(ABC):
    """
    Base class for workflow workers.
    
    Provides:
    - Automatic heartbeat registration
    - Graceful shutdown handling
    - Idempotent execution support
    - Retry logic
    """
    
    def __init__(
        self,
        worker_id: Optional[str] = None,
        capabilities: Optional[list[str]] = None,
    ):
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.capabilities = capabilities or []
        self.settings = get_settings()
        
        self._running = False
        self._redis = None
        self._cache: Optional[RedisCache] = None
        self._broker: Optional[MessageBroker] = None
        self._consumer: Optional[TaskConsumer] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._processed_keys: set[str] = set()  # In-memory idempotency cache
    
    @abstractmethod
    async def process_task(
        self,
        task_data: dict[str, Any],
    ) -> TaskResult:
        """
        Process a task. Implemented by subclasses.
        
        Args:
            task_data: Task message data
            
        Returns:
            TaskResult with output or error
        """
        pass
    
    @property
    def queue_names(self) -> list[str]:
        """Queue names this worker consumes from."""
        # Default to handler-specific queues based on capabilities
        if self.capabilities:
            return [f"tasks:{cap}" for cap in self.capabilities]
        return ["tasks:default"]
    
    async def start(self) -> None:
        """Start the worker."""
        if self._running:
            return
        
        logger.info(f"Starting worker {self.worker_id}")
        self._running = True
        
        # Initialize connections
        self._redis = await get_redis()
        self._cache = RedisCache(self._redis)
        self._broker = MessageBroker(self._redis)
        
        # Register worker
        await self._register()
        
        # Start heartbeat
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        
        # Setup signal handlers
        self._setup_signal_handlers()
        
        # Start consumer
        self._consumer = TaskConsumer(
            broker=self._broker,
            consumer_id=self.worker_id,
            queue_names=self.queue_names,
        )
        
        # Register handler
        self._consumer.register_handler("default", self._handle_task)
        for cap in self.capabilities:
            self._consumer.register_handler(cap, self._handle_task)
        
        await self._consumer.start()
        
        logger.info(f"Worker {self.worker_id} started")
    
    async def stop(self) -> None:
        """Stop the worker gracefully."""
        if not self._running:
            return
        
        logger.info(f"Stopping worker {self.worker_id}")
        self._running = False
        
        # Stop consumer
        if self._consumer:
            await self._consumer.stop()
        
        # Stop heartbeat
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
        
        # Deregister worker
        await self._deregister()
        
        logger.info(f"Worker {self.worker_id} stopped")
    
    async def run(self) -> None:
        """Run the worker until stopped."""
        await self.start()
        
        try:
            while self._running:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop()
    
    async def _register(self) -> None:
        """Register worker with heartbeat."""
        await self._cache.register_worker(
            worker_id=self.worker_id,
            capabilities=self.capabilities,
            metadata={
                "started_at": datetime.utcnow().isoformat(),
                "queue_names": self.queue_names,
            },
        )
    
    async def _deregister(self) -> None:
        """Deregister worker."""
        await self._cache.deregister_worker(self.worker_id)
    
    async def _heartbeat_loop(self) -> None:
        """Send periodic heartbeats."""
        interval = self.settings.worker.heartbeat_interval
        
        while self._running:
            try:
                success = await self._cache.worker_heartbeat(self.worker_id)
                if not success:
                    # Re-register if heartbeat failed
                    await self._register()
            except Exception as e:
                logger.error(f"Heartbeat failed: {e}")
            
            await asyncio.sleep(interval)
    
    async def _handle_task(self, task_data: dict[str, Any]) -> TaskResult:
        """
        Handle a task with idempotency support.
        
        Checks multiple layers to avoid duplicate processing:
        1. In-memory cache (fast, within same worker instance)
        2. Redis cache (cross-worker, survives restarts)
        """
        idempotency_key = task_data.get("idempotency_key", "")
        workflow_execution_id = uuid.UUID(task_data["workflow_execution_id"])
        node_id = task_data["node_id"]
        
        # Layer 1: Check in-memory idempotency cache (fast path)
        if idempotency_key in self._processed_keys:
            logger.info(f"Skipping duplicate task (in-memory): {idempotency_key}")
            return TaskResult(
                task_message_id=uuid.UUID(task_data["id"]),
                node_execution_id=uuid.UUID(task_data["node_execution_id"]),
                workflow_execution_id=workflow_execution_id,
                worker_id=self.worker_id,
                success=True,
                output_data={"skipped": True, "reason": "duplicate_in_memory"},
                started_at=datetime.utcnow(),
            )
        
        # Layer 2: Check Redis cache for completed node (survives restarts)
        # This prevents duplicate work after orchestrator recovery re-dispatches
        if self._cache:
            try:
                is_completed = await self._cache.is_node_completed(
                    workflow_execution_id, node_id
                )
                if is_completed:
                    logger.info(
                        f"Skipping task for already completed node: "
                        f"workflow={workflow_execution_id}, node={node_id}"
                    )
                    return TaskResult(
                        task_message_id=uuid.UUID(task_data["id"]),
                        node_execution_id=uuid.UUID(task_data["node_execution_id"]),
                        workflow_execution_id=workflow_execution_id,
                        worker_id=self.worker_id,
                        success=True,
                        output_data={"skipped": True, "reason": "already_completed"},
                        started_at=datetime.utcnow(),
                    )
            except Exception as e:
                # Log but don't fail - better to process potentially duplicate
                # than to block on Redis errors
                logger.warning(f"Redis idempotency check failed: {e}")
        
        # Process task
        started_at = datetime.utcnow()
        
        try:
            result = await self.process_task(task_data)
            
            # Mark as processed
            self._processed_keys.add(idempotency_key)
            # Limit in-memory cache size
            if len(self._processed_keys) > 10000:
                self._processed_keys.clear()
            
            return result
            
        except Exception as e:
            logger.error(f"Task processing failed: {e}", exc_info=True)
            
            return TaskResult(
                task_message_id=uuid.UUID(task_data["id"]),
                node_execution_id=uuid.UUID(task_data["node_execution_id"]),
                workflow_execution_id=uuid.UUID(task_data["workflow_execution_id"]),
                worker_id=self.worker_id,
                success=False,
                error_message=str(e),
                error_type=type(e).__name__,
                is_retryable=self._is_retryable_error(e),
                started_at=started_at,
            )
    
    def _is_retryable_error(self, error: Exception) -> bool:
        """Determine if an error is retryable."""
        # Non-retryable error types
        non_retryable = (
            ValueError,
            TypeError,
            KeyError,
            AttributeError,
        )
        return not isinstance(error, non_retryable)
    
    def _setup_signal_handlers(self) -> None:
        """Setup graceful shutdown signal handlers."""
        loop = asyncio.get_event_loop()
        
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(
                sig,
                lambda: asyncio.create_task(self._signal_handler(sig)),
            )
    
    async def _signal_handler(self, sig: signal.Signals) -> None:
        """Handle shutdown signal."""
        logger.info(f"Received signal {sig.name}, shutting down...")
        self._running = False
