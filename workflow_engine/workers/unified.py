"""
Unified worker that handles all task types.

Registers handlers for all supported node types and processes tasks.

Uses Redis Streams for reliable result reporting:
- Workers publish results to a Redis Stream
- Orchestrators consume from the stream with consumer groups
- At-least-once delivery guarantees
- Decoupled architecture (no direct orchestrator dependency)
"""

import asyncio
import logging
import uuid
from datetime import datetime
from typing import Any, Optional

from workflow_engine.config import get_settings
from workflow_engine.core.models import HandlerType, TaskResult
from workflow_engine.messaging.broker import MessageBroker, ResultQueue
from workflow_engine.messaging.consumer import TaskConsumer
from workflow_engine.storage.redis.cache import RedisCache
from workflow_engine.storage.redis.connection import get_redis
from workflow_engine.workers.handlers import (
    ExternalServiceHandler,
    InputHandler,
    LLMServiceHandler,
    OutputHandler,
)

logger = logging.getLogger(__name__)


class UnifiedWorker:
    """
    Unified worker that can process all task types.
    
    Registers individual handlers and routes tasks appropriately.
    Reports results via Redis Streams for reliable, decoupled delivery.
    """
    
    def __init__(
        self,
        worker_id: Optional[str] = None,
    ):
        self.worker_id = worker_id or f"unified-worker-{uuid.uuid4().hex[:8]}"
        self.settings = get_settings()
        
        self._running = False
        self._redis = None
        self._cache: Optional[RedisCache] = None
        self._broker: Optional[MessageBroker] = None
        self._result_queue: Optional[ResultQueue] = None
        self._consumer: Optional[TaskConsumer] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        
        # Initialize individual handlers
        self._input_handler = InputHandler()
        self._output_handler = OutputHandler()
        self._external_handler = ExternalServiceHandler()
        self._llm_handler = LLMServiceHandler()
        
        # Map handlers to their types
        self._handler_map = {
            HandlerType.INPUT.value: self._input_handler.process_task,
            HandlerType.OUTPUT.value: self._output_handler.process_task,
            HandlerType.CALL_EXTERNAL_SERVICE.value: self._external_handler.process_task,
            HandlerType.LLM_SERVICE.value: self._llm_handler.process_task,
        }
    
    @property
    def queue_names(self) -> list[str]:
        """Get all queue names to consume from."""
        return [
            f"tasks:{handler_type}"
            for handler_type in self._handler_map.keys()
        ]
    
    async def start(self) -> None:
        """Start the unified worker."""
        if self._running:
            return
        
        logger.info(f"Starting unified worker {self.worker_id}")
        self._running = True
        
        # Initialize Redis
        self._redis = await get_redis()
        self._cache = RedisCache(self._redis)
        self._broker = MessageBroker(self._redis)
        
        # Initialize result queue for publishing results
        self._result_queue = await self._broker.get_result_queue()
        
        # Register worker
        await self._cache.register_worker(
            worker_id=self.worker_id,
            capabilities=list(self._handler_map.keys()),
            metadata={
                "started_at": datetime.utcnow().isoformat(),
                "type": "unified",
            },
        )
        
        # Start heartbeat
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        
        # Start consumer for each handler type
        self._consumer = TaskConsumer(
            broker=self._broker,
            consumer_id=self.worker_id,
            queue_names=self.queue_names,
            result_queue=self._result_queue,
            cache=self._cache,
        )
        
        # Register handlers
        for handler_type, handler_func in self._handler_map.items():
            self._consumer.register_handler(handler_type, self._wrap_handler(handler_func))
        
        await self._consumer.start()
        logger.info(f"Unified worker {self.worker_id} started")
    
    async def stop(self) -> None:
        """Stop the unified worker gracefully."""
        if not self._running:
            return
        
        logger.info(f"Stopping unified worker {self.worker_id}")
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
        if self._cache:
            await self._cache.deregister_worker(self.worker_id)
        
        logger.info(f"Unified worker {self.worker_id} stopped")
    
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
    
    def _wrap_handler(self, handler_func):
        """Wrap handler to add result reporting."""
        async def wrapped(task_data: dict[str, Any]) -> TaskResult:
            result = await handler_func(task_data)
            
            # Report result to orchestrator
            await self._report_result(task_data, result)
            
            return result
        
        return wrapped
    
    async def _report_result(
        self,
        task_data: dict[str, Any],
        result: TaskResult,
    ) -> None:
        """
        Report task result via Redis Streams.
        
        Publishing to a Redis Stream provides:
        - At-least-once delivery guarantees
        - Decoupled architecture (no direct orchestrator dependency)
        - Durability (results persist until acknowledged)
        - Support for multiple orchestrator consumers
        """
        if not self._result_queue:
            logger.error("Cannot report result: Result queue not initialized")
            return
        
        try:
            message_id = await self._result_queue.publish_result(
                workflow_execution_id=task_data["workflow_execution_id"],
                node_id=task_data["node_id"],
                success=result.success,
                output_data=result.output_data,
                error_message=result.error_message,
                error_type=result.error_type,
                is_retryable=result.is_retryable,
                worker_id=self.worker_id,
            )
            
            logger.debug(
                f"Published result for node {task_data['node_id']} "
                f"(message_id: {message_id})"
            )
            
        except Exception as e:
            # Log error but don't fail - the message will be in the stream
            # or can be retried. This should rarely happen with Redis Streams.
            logger.error(
                f"Failed to publish result for {task_data['node_id']}: {e}",
                exc_info=True,
            )
    
    async def _heartbeat_loop(self) -> None:
        """Send periodic heartbeats."""
        interval = self.settings.worker.heartbeat_interval
        
        while self._running:
            try:
                if self._cache:
                    await self._cache.worker_heartbeat(self.worker_id)
            except Exception as e:
                logger.error(f"Heartbeat failed: {e}")
            
            await asyncio.sleep(interval)


async def run_worker():
    """Entry point for running the unified worker."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    
    worker = UnifiedWorker()
    
    try:
        await worker.run()
    except KeyboardInterrupt:
        logger.info("Worker interrupted")
    finally:
        await worker.stop()


if __name__ == "__main__":
    asyncio.run(run_worker())
