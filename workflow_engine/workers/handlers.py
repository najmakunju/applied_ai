"""
Task handlers for different node types.

Implements mock handlers for External Service and LLM nodes.
All handlers have timeout protection to prevent tasks from hanging indefinitely.
"""

import asyncio
import logging
import random
import uuid
from datetime import datetime
from typing import Any

from workflow_engine.config import get_settings
from workflow_engine.core.models import HandlerType, TaskResult
from workflow_engine.workers.base import BaseWorker

logger = logging.getLogger(__name__)


class TaskTimeoutError(Exception):
    """Raised when a task exceeds its timeout."""
    pass


class InputHandler(BaseWorker):
    """
    Handler for input nodes.
    
    Passes through input parameters to downstream nodes.
    """
    
    def __init__(self, worker_id: str | None = None):
        super().__init__(
            worker_id=worker_id or f"input-worker-{uuid.uuid4().hex[:8]}",
            capabilities=[HandlerType.INPUT.value],
        )
    
    async def process_task(self, task_data: dict[str, Any]) -> TaskResult:
        """Process input node - just passes through input data."""
        started_at = datetime.utcnow()
        
        input_data = task_data.get("input_data", {})
        
        logger.info(f"Processing input node: {task_data.get('node_id')}")
        
        return TaskResult(
            task_message_id=uuid.UUID(task_data["id"]),
            node_execution_id=uuid.UUID(task_data["node_execution_id"]),
            workflow_execution_id=uuid.UUID(task_data["workflow_execution_id"]),
            worker_id=self.worker_id,
            success=True,
            output_data={"input": input_data},
            started_at=started_at,
            duration_ms=0,
        )


class OutputHandler(BaseWorker):
    """
    Handler for output nodes.
    
    Aggregates outputs from upstream nodes.
    """
    
    def __init__(self, worker_id: str | None = None):
        super().__init__(
            worker_id=worker_id or f"output-worker-{uuid.uuid4().hex[:8]}",
            capabilities=[HandlerType.OUTPUT.value],
        )
    
    async def process_task(self, task_data: dict[str, Any]) -> TaskResult:
        """Process output node - aggregates input from all dependencies."""
        started_at = datetime.utcnow()
        
        input_data = task_data.get("input_data", {})
        
        logger.info(f"Processing output node: {task_data.get('node_id')}")
        
        # Output node aggregates all inputs
        return TaskResult(
            task_message_id=uuid.UUID(task_data["id"]),
            node_execution_id=uuid.UUID(task_data["node_execution_id"]),
            workflow_execution_id=uuid.UUID(task_data["workflow_execution_id"]),
            worker_id=self.worker_id,
            success=True,
            output_data={"result": input_data},
            started_at=started_at,
            duration_ms=0,
        )


class ExternalServiceHandler(BaseWorker):
    """
    Handler for external service calls.
    
    Mocks HTTP calls by sleeping for 1-2 seconds and returning dummy JSON.
    Includes timeout protection to prevent hanging tasks.
    """
    
    def __init__(self, worker_id: str | None = None):
        super().__init__(
            worker_id=worker_id or f"external-worker-{uuid.uuid4().hex[:8]}",
            capabilities=[HandlerType.CALL_EXTERNAL_SERVICE.value],
        )
        self._settings = get_settings()
    
    async def process_task(self, task_data: dict[str, Any]) -> TaskResult:
        """Mock external service call with timeout protection."""
        started_at = datetime.utcnow()
        
        config = task_data.get("config", {})
        url = config.get("url", "http://example.com/api")
        method = config.get("method", "GET")
        
        node_id = task_data.get("node_id", "unknown")
        
        logger.info(f"Processing external service node: {node_id}, URL: {url}")
        
        # Get timeout for this handler type
        timeout = self._settings.worker.handler_timeouts.get_timeout("call_external_service")
        
        try:
            # Wrap the actual work in a timeout to prevent hanging
            async with asyncio.timeout(timeout):
                # Simulate network latency (1-2 seconds as per spec)
                delay = random.uniform(1.0, 2.0)
                await asyncio.sleep(delay)
                
                # Generate mock response based on URL/node
                mock_response = self._generate_mock_response(node_id, url)
        except asyncio.TimeoutError:
            duration_ms = int((datetime.utcnow() - started_at).total_seconds() * 1000)
            logger.error(f"External service task {node_id} timed out after {timeout}s")
            return TaskResult(
                task_message_id=uuid.UUID(task_data["id"]),
                node_execution_id=uuid.UUID(task_data["node_execution_id"]),
                workflow_execution_id=uuid.UUID(task_data["workflow_execution_id"]),
                worker_id=self.worker_id,
                success=False,
                error_message=f"Task timed out after {timeout} seconds",
                error_type="TaskTimeoutError",
                is_retryable=True,
                started_at=started_at,
                duration_ms=duration_ms,
            )
        
        duration_ms = int((datetime.utcnow() - started_at).total_seconds() * 1000)
        
        return TaskResult(
            task_message_id=uuid.UUID(task_data["id"]),
            node_execution_id=uuid.UUID(task_data["node_execution_id"]),
            workflow_execution_id=uuid.UUID(task_data["workflow_execution_id"]),
            worker_id=self.worker_id,
            success=True,
            output_data={
                "status_code": 200,
                "url": url,
                "method": method,
                "response": mock_response,
            },
            started_at=started_at,
            duration_ms=duration_ms,
        )
    
    def _generate_mock_response(self, node_id: str, url: str) -> dict[str, Any]:
        """Generate mock API response based on node type."""
        # Generate contextual mock data based on node_id
        if "user" in node_id.lower():
            return {
                "id": random.randint(1, 1000),
                "name": f"User_{random.randint(1, 100)}",
                "email": f"user{random.randint(1, 100)}@example.com",
                "status": "active",
            }
        elif "post" in node_id.lower():
            return {
                "posts": [
                    {
                        "id": i,
                        "title": f"Post Title {i}",
                        "content": f"Content for post {i}",
                    }
                    for i in range(1, 4)
                ]
            }
        elif "comment" in node_id.lower():
            return {
                "comments": [
                    {
                        "id": i,
                        "text": f"Comment {i} text",
                        "author": f"Author_{i}",
                    }
                    for i in range(1, 6)
                ]
            }
        else:
            # Generic response
            return {
                "success": True,
                "data": {
                    "id": str(uuid.uuid4()),
                    "timestamp": datetime.utcnow().isoformat(),
                    "source": url,
                },
            }


class LLMServiceHandler(BaseWorker):
    """
    Handler for LLM service calls.
    
    Mocks AI generation by accepting a prompt and returning a mock response.
    Includes timeout protection to prevent hanging tasks.
    """
    
    def __init__(self, worker_id: str | None = None):
        super().__init__(
            worker_id=worker_id or f"llm-worker-{uuid.uuid4().hex[:8]}",
            capabilities=[HandlerType.LLM_SERVICE.value],
        )
        self._settings = get_settings()
    
    async def process_task(self, task_data: dict[str, Any]) -> TaskResult:
        """Mock LLM service call with timeout protection."""
        started_at = datetime.utcnow()
        
        config = task_data.get("config", {})
        prompt = config.get("prompt", "")
        model = config.get("model", "gpt-4")
        temperature = config.get("temperature", 0.7)
        max_tokens = config.get("max_tokens", 1000)
        
        input_data = task_data.get("input_data", {})
        node_id = task_data.get("node_id", "unknown")
        
        logger.info(f"Processing LLM node: {node_id}, model: {model}")
        
        # Get timeout for this handler type
        timeout = self._settings.worker.handler_timeouts.get_timeout("llm_service")
        
        try:
            # Wrap the actual work in a timeout to prevent hanging
            async with asyncio.timeout(timeout):
                # Simulate LLM processing time (1-3 seconds)
                delay = random.uniform(1.0, 3.0)
                await asyncio.sleep(delay)
                
                # Generate mock LLM response
                mock_response = self._generate_mock_llm_response(prompt, input_data)
        except asyncio.TimeoutError:
            duration_ms = int((datetime.utcnow() - started_at).total_seconds() * 1000)
            logger.error(f"LLM service task {node_id} timed out after {timeout}s")
            return TaskResult(
                task_message_id=uuid.UUID(task_data["id"]),
                node_execution_id=uuid.UUID(task_data["node_execution_id"]),
                workflow_execution_id=uuid.UUID(task_data["workflow_execution_id"]),
                worker_id=self.worker_id,
                success=False,
                error_message=f"Task timed out after {timeout} seconds",
                error_type="TaskTimeoutError",
                is_retryable=True,
                started_at=started_at,
                duration_ms=duration_ms,
            )
        
        duration_ms = int((datetime.utcnow() - started_at).total_seconds() * 1000)
        
        return TaskResult(
            task_message_id=uuid.UUID(task_data["id"]),
            node_execution_id=uuid.UUID(task_data["node_execution_id"]),
            workflow_execution_id=uuid.UUID(task_data["workflow_execution_id"]),
            worker_id=self.worker_id,
            success=True,
            output_data={
                "model": model,
                "prompt": prompt[:100] + "..." if len(prompt) > 100 else prompt,
                "response": mock_response,
                "usage": {
                    "prompt_tokens": len(prompt.split()),
                    "completion_tokens": len(mock_response.split()),
                    "total_tokens": len(prompt.split()) + len(mock_response.split()),
                },
            },
            started_at=started_at,
            duration_ms=duration_ms,
        )
    
    def _generate_mock_llm_response(
        self,
        prompt: str,
        input_data: dict[str, Any],
    ) -> str:
        """Generate mock LLM response based on prompt."""
        # Generate contextual mock response
        prompt_lower = prompt.lower()
        
        if "summarize" in prompt_lower or "summary" in prompt_lower:
            return (
                "Based on the provided information, here is a concise summary: "
                "The data shows key insights across multiple dimensions. "
                "The main findings indicate positive trends with room for improvement. "
                "Further analysis is recommended for actionable recommendations."
            )
        elif "analyze" in prompt_lower or "analysis" in prompt_lower:
            return (
                "Analysis Results:\n"
                "1. Data Quality: High confidence in source data\n"
                "2. Key Patterns: Multiple recurring themes identified\n"
                "3. Recommendations: Consider optimizing workflow efficiency\n"
                "4. Risk Assessment: Low to moderate risk levels detected"
            )
        elif "translate" in prompt_lower:
            return "Mock translation: [Translated content would appear here]"
        elif "code" in prompt_lower or "function" in prompt_lower:
            return (
                "```python\n"
                "def process_data(data):\n"
                "    # Mock generated code\n"
                "    result = transform(data)\n"
                "    return validate(result)\n"
                "```"
            )
        else:
            return (
                f"Mock LLM response for prompt: '{prompt[:50]}...'\n"
                f"Input data keys: {list(input_data.keys())}\n"
                "This is a simulated response demonstrating the LLM handler functionality. "
                "In production, this would be replaced with actual LLM API calls."
            )


