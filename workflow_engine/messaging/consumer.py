"""
Task consumer for processing messages from the queue.

Handles message consumption, processing, and acknowledgment.

Uses a semaphore-controlled task pool for production-grade concurrency:
- Configurable concurrency via WORKER_CONCURRENCY setting
- Tasks from any queue compete for pool slots
- Backpressure when pool is full (consumers block on semaphore)
- Fair scheduling across queue types
"""

import asyncio
import logging
from typing import Any, Callable, Optional

import redis.asyncio as redis
import redis.exceptions

from workflow_engine.config import get_settings
from workflow_engine.core.models import HandlerType, TaskResult
from workflow_engine.messaging.broker import MessageBroker, TaskQueue

logger = logging.getLogger(__name__)


class TaskConsumer:
    """
    Consumes and processes tasks from the message queue.
    
    Uses a shared semaphore to limit concurrent task execution across all queues.
    This decouples concurrency from the number of queue types, allowing:
    - Configurable max concurrent tasks via WORKER_CONCURRENCY
    - Fair resource sharing across queue types
    - Backpressure when the worker is at capacity
    """
    
    def __init__(
        self,
        broker: MessageBroker,
        consumer_id: str,
        queue_names: Optional[list[str]] = None,
    ):
        self.broker = broker
        self.consumer_id = consumer_id
        self.queue_names = queue_names or ["tasks:default"]
        self.settings = get_settings()
        
        self._handlers: dict[str, Callable] = {}
        self._running = False
        self._tasks: list[asyncio.Task] = []
        self._processing_tasks: set[asyncio.Task] = set()
        
        # Semaphore controls max concurrent task processing across ALL queues
        self._concurrency = self.settings.worker.concurrency
        self._semaphore: Optional[asyncio.Semaphore] = None
    
    def register_handler(
        self,
        handler_type: str | HandlerType,
        handler: Callable,
    ) -> None:
        """
        Register a task handler function.
        
        Args:
            handler_type: The handler type string or enum
            handler: Async function that processes the task
        """
        if isinstance(handler_type, HandlerType):
            handler_type = handler_type.value
        self._handlers[handler_type] = handler
    
    async def start(self) -> None:
        """Start consuming messages."""
        if self._running:
            return
        
        self._running = True
        
        # Initialize semaphore for concurrency control
        self._semaphore = asyncio.Semaphore(self._concurrency)
        
        logger.info(
            f"Starting consumer {self.consumer_id} for queues: {self.queue_names} "
            f"(concurrency: {self._concurrency})"
        )
        
        # Start consumer tasks for each queue. Reads new msg from stream
        for queue_name in self.queue_names:
            task = asyncio.create_task(self._consume_loop(queue_name))
            self._tasks.append(task)
        
        # Start stale message claimer
        task = asyncio.create_task(self._claim_stale_messages())
        self._tasks.append(task)
    
    async def stop(self) -> None:
        """Stop consuming messages gracefully."""
        if not self._running:
            return
        
        self._running = False
        logger.info(f"Stopping consumer {self.consumer_id}")
        
        # Cancel consume loops
        for task in self._tasks:
            task.cancel()
        
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        
        # Wait for in-flight processing tasks to complete (with timeout)
        if self._processing_tasks:
            logger.info(f"Waiting for {len(self._processing_tasks)} in-flight tasks to complete")
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._processing_tasks, return_exceptions=True),
                    timeout=self.settings.worker.graceful_shutdown_timeout,
                )
            except asyncio.TimeoutError:
                logger.warning("Graceful shutdown timed out, cancelling remaining tasks")
                for task in self._processing_tasks:
                    task.cancel()
                await asyncio.gather(*self._processing_tasks, return_exceptions=True)
        
        self._processing_tasks.clear()
    
    async def _consume_loop(self, queue_name: str) -> None:
        """Main consumption loop for a queue."""
        queue = await self.broker.get_queue(queue_name)
        block_ms = self.settings.redis.stream_block_ms
        
        while self._running:
            try:
                # Wait for a slot in the task pool before consuming
                # This provides backpressure - we don't pull more messages
                # than we can handle concurrently
                await self._semaphore.acquire()
                
                # Check if we're still running after acquiring semaphore
                if not self._running:
                    self._semaphore.release()
                    break
                
                messages = await queue.consume(
                    consumer_id=self.consumer_id,
                    count=1,
                    block_ms=block_ms,
                )
                
                if not messages:
                    # No messages, release semaphore and continue
                    self._semaphore.release()
                    continue
                
                for msg_id, msg_data in messages:
                    # Spawn processing task (semaphore already acquired)
                    task = asyncio.create_task(
                        self._process_message_with_semaphore(queue, msg_id, msg_data)
                    )
                    self._processing_tasks.add(task)
                    task.add_done_callback(self._processing_tasks.discard)
                    
            except asyncio.CancelledError:
                logger.debug(f"Consume loop for {queue_name} cancelled")
                # Release semaphore if we acquired it
                try:
                    self._semaphore.release()
                except ValueError:
                    pass  # Semaphore wasn't held
                break
            except redis.exceptions.TimeoutError as e:
                # Redis socket timeout - release semaphore and retry
                logger.debug(f"Redis timeout on {queue_name}, retrying: {e}")
                self._semaphore.release()
                continue
            except redis.exceptions.ConnectionError as e:
                logger.warning(f"Redis connection error on {queue_name}: {e}")
                self._semaphore.release()
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Error in consume loop for {queue_name}: {e}", exc_info=True)
                self._semaphore.release()
                await asyncio.sleep(1)
    
    async def _process_message_with_semaphore(
        self,
        queue: TaskQueue,
        msg_id: str,
        msg_data: dict[str, Any],
    ) -> None:
        """Process a message and release semaphore when done."""
        try:
            await self._process_message(queue, msg_id, msg_data)
        finally:
            self._semaphore.release()
    
    async def _process_message(
        self,
        queue: TaskQueue,
        msg_id: str,
        msg_data: dict[str, Any],
    ) -> None:
        """Process a single message."""
        handler_type = msg_data.get("handler")
        
        if handler_type not in self._handlers:
            logger.warning(f"No handler for type: {handler_type}")
            await queue.reject(
                msg_id,
                msg_data,
                f"No handler registered for type: {handler_type}",
            )
            return
        
        handler = self._handlers[handler_type]
        
        try:
            # Call handler
            result = await handler(msg_data)
            
            # Acknowledge on success
            await queue.acknowledge(msg_id)
            
            logger.debug(f"Successfully processed message {msg_id}")
            
        except Exception as e:
            logger.error(f"Error processing message {msg_id}: {e}", exc_info=True)
            
            # Check retry count
            attempt = int(msg_data.get("attempt", 1))
            max_retries = msg_data.get("retry_config", {}).get("max_retries", 3)
            
            if attempt >= max_retries:
                # Move to DLQ
                await queue.reject(msg_id, msg_data, str(e), move_to_dlq=True)
            else:
                # Acknowledge and re-queue with incremented attempt
                await queue.acknowledge(msg_id)
                
                # Re-publish with incremented attempt
                retry_data = {
                    **msg_data,
                    "attempt": attempt + 1,
                }
                await self.broker.client.xadd(
                    queue.stream_key,
                    self._serialize_message(retry_data),
                )
    
    def _serialize_message(self, data: dict[str, Any]) -> dict[str, str]:
        """Serialize message data for Redis."""
        import json
        
        result = {}
        for key, value in data.items():
            if isinstance(value, dict):
                result[key] = json.dumps(value)
            else:
                result[key] = str(value)
        return result
    
    def _get_handler_type_from_queue(self, queue_name: str) -> str:
        """
        Extract handler type from queue name.
        
        Queue names are formatted as 'tasks:{handler_type}'
        e.g., 'tasks:input' -> 'input'
              'tasks:call_external_service' -> 'call_external_service'
        """
        if ":" in queue_name:
            return queue_name.split(":", 1)[1]
        return queue_name
    
    async def _claim_stale_messages(self) -> None:
        """
        Periodically claim stale messages from dead workers.
        
        Uses per-handler timeouts to avoid claiming messages that are
        still being processed by slow handlers (e.g., LLM calls).
        """
        stale_timeouts = self.settings.worker.stale_timeouts
        claim_interval = self.settings.worker.stale_claim_interval
        
        while self._running:
            try:
                for queue_name in self.queue_names:
                    queue = await self.broker.get_queue(queue_name)
                    
                    # Get handler-specific timeout
                    handler_type = self._get_handler_type_from_queue(queue_name)
                    min_idle_ms = stale_timeouts.get_timeout_ms(handler_type)
                    
                    claimed = await queue.claim_stale_messages(
                        consumer_id=self.consumer_id,
                        min_idle_ms=min_idle_ms,
                        count=5,
                    )
                    
                    # Process claimed messages in parallel using the task pool
                    for msg_id, msg_data in claimed:
                        logger.info(
                            f"Claimed stale message: {msg_id} from {queue_name} "
                            f"(idle > {min_idle_ms}ms)"
                        )
                        # Acquire semaphore slot and spawn task (same as regular processing)
                        await self._semaphore.acquire()
                        task = asyncio.create_task(
                            self._process_message_with_semaphore(queue, msg_id, msg_data)
                        )
                        self._processing_tasks.add(task)
                        task.add_done_callback(self._processing_tasks.discard)
                
                await asyncio.sleep(claim_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error claiming stale messages: {e}")
                await asyncio.sleep(5)
