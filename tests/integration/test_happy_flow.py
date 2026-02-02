"""
Happy flow integration test for the workflow engine.

This test validates the complete workflow lifecycle:
1. Submit a workflow
2. Trigger execution
3. Process tasks (simulated worker)
4. Verify workflow completion

Requires Redis and PostgreSQL running (use docker-compose up redis postgres).
"""

import asyncio
import logging
import pytest
import pytest_asyncio
from datetime import datetime
from typing import Any
from uuid import UUID

import redis.asyncio as redis

from workflow_engine.config import get_settings
from workflow_engine.core.models import HandlerType, TaskResult
from workflow_engine.core.state_machine import NodeState, WorkflowState
from workflow_engine.messaging.broker import MessageBroker, ResultQueue
from workflow_engine.orchestrator.engine import WorkflowOrchestrator
from workflow_engine.storage.postgres.database import Database
from workflow_engine.storage.redis.cache import RedisCache

logger = logging.getLogger(__name__)

# Skip if Redis not available
pytest.importorskip("redis")


@pytest_asyncio.fixture
async def redis_client():
    """Create Redis client for tests."""
    settings = get_settings()
    client = redis.Redis(
        host=settings.redis.host,
        port=settings.redis.port,
        db=settings.redis.db,
        decode_responses=True,
    )
    
    try:
        await client.ping()
    except redis.ConnectionError:
        pytest.skip("Redis not available")
    
    yield client
    
    # Cleanup test keys
    async for key in client.scan_iter(match="wf:*"):
        await client.delete(key)
    
    await client.aclose()


@pytest_asyncio.fixture
async def database():
    """Create database connection for tests."""
    settings = get_settings()
    db = Database()
    
    try:
        await db.init()
    except Exception as e:
        pytest.skip(f"PostgreSQL not available: {e}")
    
    yield db
    
    await db.close()


@pytest_asyncio.fixture
async def orchestrator(redis_client, database):
    """Create and initialize orchestrator."""
    orch = WorkflowOrchestrator(
        redis_client=redis_client,
        database=database,
        orchestrator_id="test-orchestrator",
    )
    
    await orch.init()
    # Note: Don't start the background tasks for this test
    # We'll manually process results
    
    yield orch
    
    await orch.stop()


class MockWorker:
    """
    Mock worker that processes tasks and reports results.
    
    Used to simulate worker behavior in tests without running actual workers.
    """
    
    def __init__(self, redis_client: redis.Redis, worker_id: str = "test-worker"):
        self.redis = redis_client
        self.worker_id = worker_id
        self.broker = MessageBroker(redis_client)
        self._result_queue: ResultQueue | None = None
    
    async def init(self):
        """Initialize the mock worker."""
        self._result_queue = await self.broker.get_result_queue()
    
    async def process_task(self, queue_name: str) -> bool:
        """
        Consume and process one task from the specified queue.
        
        Returns True if a task was processed.
        """
        queue = await self.broker.get_queue(queue_name)
        
        # Consume one message
        messages = await queue.consume(
            consumer_id=self.worker_id,
            count=1,
            block_ms=1000,
        )
        
        if not messages:
            return False
        
        msg_id, task_data = messages[0]
        
        # Process based on handler type
        handler = task_data["handler"]
        output_data = await self._handle_task(handler, task_data)
        
        # Acknowledge the task
        await queue.acknowledge(msg_id)
        
        # Publish result
        await self._result_queue.publish_result(
            workflow_execution_id=task_data["workflow_execution_id"],
            node_id=task_data["node_id"],
            success=True,
            output_data=output_data,
            worker_id=self.worker_id,
        )
        
        logger.info(f"Processed task: {task_data['node_id']} ({handler})")
        return True
    
    async def _handle_task(self, handler: str, task_data: dict[str, Any]) -> dict[str, Any]:
        """Generate mock output based on handler type."""
        node_id = task_data["node_id"]
        input_data = task_data.get("input_data", {})
        
        if handler == HandlerType.INPUT.value:
            return {"input": input_data}
        
        elif handler == HandlerType.OUTPUT.value:
            return {"result": input_data}
        
        elif handler == HandlerType.CALL_EXTERNAL_SERVICE.value:
            # Generate contextual mock data
            if "user" in node_id.lower():
                return {
                    "status_code": 200,
                    "response": {
                        "id": 1,
                        "name": "Test User",
                        "email": "test@example.com",
                    }
                }
            elif "post" in node_id.lower():
                return {
                    "status_code": 200,
                    "response": {
                        "posts": [{"id": 1, "title": "Post 1"}, {"id": 2, "title": "Post 2"}]
                    }
                }
            elif "comment" in node_id.lower():
                return {
                    "status_code": 200,
                    "response": {
                        "comments": [{"id": 1, "text": "Comment 1"}]
                    }
                }
            else:
                return {"status_code": 200, "response": {"success": True}}
        
        elif handler == HandlerType.LLM_SERVICE.value:
            return {
                "model": "gpt-4",
                "response": "Mock LLM response for testing",
            }
        
        return {"processed": True}
    
    async def process_all_pending(self, timeout: float = 30.0) -> int:
        """
        Process all pending tasks across all queues.
        
        Returns the number of tasks processed.
        """
        queue_names = [
            f"tasks:{HandlerType.INPUT.value}",
            f"tasks:{HandlerType.OUTPUT.value}",
            f"tasks:{HandlerType.CALL_EXTERNAL_SERVICE.value}",
            f"tasks:{HandlerType.LLM_SERVICE.value}",
        ]
        
        processed = 0
        start_time = asyncio.get_event_loop().time()
        
        while asyncio.get_event_loop().time() - start_time < timeout:
            found_task = False
            
            for queue_name in queue_names:
                if await self.process_task(queue_name):
                    found_task = True
                    processed += 1
            
            if not found_task:
                # No tasks found, wait a bit before checking again
                await asyncio.sleep(0.1)
        
        return processed


class TestHappyFlowLinearWorkflow:
    """Test happy flow for a simple linear workflow: input -> process -> output."""
    
    @pytest.mark.asyncio
    async def test_linear_workflow_completes(self, orchestrator, redis_client):
        """Test that a linear workflow completes successfully."""
        # Create mock worker
        worker = MockWorker(redis_client)
        await worker.init()
        
        # Define a simple linear workflow
        workflow_json = {
            "name": "Linear Test Workflow",
            "dag": {
                "nodes": [
                    {
                        "id": "input",
                        "handler": "input",
                        "dependencies": [],
                    },
                    {
                        "id": "process",
                        "handler": "call_external_service",
                        "dependencies": ["input"],
                        "config": {
                            "url": "http://api.example.com/process",
                        },
                    },
                    {
                        "id": "output",
                        "handler": "output",
                        "dependencies": ["process"],
                    },
                ]
            },
        }
        
        # Submit workflow
        definition_id, execution_id = await orchestrator.submit_workflow(workflow_json)
        
        assert definition_id is not None
        assert execution_id is not None
        
        # Check initial state
        status = await orchestrator.get_workflow_status(execution_id)
        assert status["state"] == WorkflowState.PENDING.value
        
        # Trigger workflow
        await orchestrator.trigger_workflow(execution_id, input_params={"test": "data"})
        
        # Process tasks and handle results
        for _ in range(10):  # Max iterations to prevent infinite loop
            # Process one task
            await worker.process_task(f"tasks:{HandlerType.INPUT.value}")
            await worker.process_task(f"tasks:{HandlerType.CALL_EXTERNAL_SERVICE.value}")
            await worker.process_task(f"tasks:{HandlerType.OUTPUT.value}")
            
            # Manually process results (since orchestrator background task isn't running)
            results = await orchestrator._result_queue.consume(
                consumer_id="test-orchestrator",
                count=10,
                block_ms=500,
            )
            
            for msg_id, result_data in results:
                await orchestrator._handle_result_message(result_data)
                await orchestrator._result_queue.acknowledge(msg_id)
            
            # Check if workflow completed
            status = await orchestrator.get_workflow_status(execution_id)
            if status and status["state"] in (
                WorkflowState.COMPLETED.value,
                WorkflowState.FAILED.value,
            ):
                break
            
            await asyncio.sleep(0.1)
        
        # Verify completion
        status = await orchestrator.get_workflow_status(execution_id)
        assert status["state"] == WorkflowState.COMPLETED.value
        
        # Verify all nodes completed
        for node_state in status["node_states"].values():
            assert node_state == NodeState.COMPLETED.value
        
        # Verify results
        results = await orchestrator.get_workflow_results(execution_id)
        assert results["state"] == WorkflowState.COMPLETED.value
        assert results["output_data"] is not None


class TestHappyFlowFanOutFanIn:
    """Test happy flow for fan-out/fan-in workflow: input -> (A, B, C) -> output."""
    
    @pytest.mark.asyncio
    async def test_fanout_fanin_workflow_completes(self, orchestrator, redis_client):
        """Test that a fan-out/fan-in workflow completes successfully."""
        # Create mock worker
        worker = MockWorker(redis_client)
        await worker.init()
        
        # Define fan-out/fan-in workflow
        workflow_json = {
            "name": "Fan-Out Fan-In Test",
            "dag": {
                "nodes": [
                    {
                        "id": "input",
                        "handler": "input",
                        "dependencies": [],
                    },
                    {
                        "id": "get_user",
                        "handler": "call_external_service",
                        "dependencies": ["input"],
                        "config": {"url": "http://api.example.com/user"},
                    },
                    {
                        "id": "get_posts",
                        "handler": "call_external_service",
                        "dependencies": ["input"],
                        "config": {"url": "http://api.example.com/posts"},
                    },
                    {
                        "id": "get_comments",
                        "handler": "call_external_service",
                        "dependencies": ["input"],
                        "config": {"url": "http://api.example.com/comments"},
                    },
                    {
                        "id": "output",
                        "handler": "output",
                        "dependencies": ["get_user", "get_posts", "get_comments"],
                    },
                ]
            },
        }
        
        # Submit workflow
        definition_id, execution_id = await orchestrator.submit_workflow(workflow_json)
        
        # Trigger workflow
        await orchestrator.trigger_workflow(execution_id, input_params={"user_id": 123})
        
        # Process tasks and results
        for _ in range(20):  # More iterations for parallel nodes
            # Process tasks from all queues
            await worker.process_task(f"tasks:{HandlerType.INPUT.value}")
            await worker.process_task(f"tasks:{HandlerType.CALL_EXTERNAL_SERVICE.value}")
            await worker.process_task(f"tasks:{HandlerType.CALL_EXTERNAL_SERVICE.value}")
            await worker.process_task(f"tasks:{HandlerType.CALL_EXTERNAL_SERVICE.value}")
            await worker.process_task(f"tasks:{HandlerType.OUTPUT.value}")
            
            # Process results
            results = await orchestrator._result_queue.consume(
                consumer_id="test-orchestrator",
                count=10,
                block_ms=500,
            )
            
            for msg_id, result_data in results:
                await orchestrator._handle_result_message(result_data)
                await orchestrator._result_queue.acknowledge(msg_id)
            
            # Check completion
            status = await orchestrator.get_workflow_status(execution_id)
            if status and status["state"] in (
                WorkflowState.COMPLETED.value,
                WorkflowState.FAILED.value,
            ):
                break
            
            await asyncio.sleep(0.1)
        
        # Verify completion
        status = await orchestrator.get_workflow_status(execution_id)
        assert status["state"] == WorkflowState.COMPLETED.value
        
        # Verify all 5 nodes completed
        assert len(status["node_states"]) == 5
        for node_id, node_state in status["node_states"].items():
            assert node_state == NodeState.COMPLETED.value, f"Node {node_id} not completed"
        
        # Verify output contains aggregated results
        results = await orchestrator.get_workflow_results(execution_id)
        assert results["output_data"] is not None


class TestHappyFlowWithLLM:
    """Test happy flow for workflow with LLM service node."""
    
    @pytest.mark.asyncio
    async def test_llm_workflow_completes(self, orchestrator, redis_client):
        """Test that a workflow with LLM service node completes successfully."""
        # Create mock worker
        worker = MockWorker(redis_client)
        await worker.init()
        
        # Define workflow with LLM node
        workflow_json = {
            "name": "LLM Workflow Test",
            "dag": {
                "nodes": [
                    {
                        "id": "input",
                        "handler": "input",
                        "dependencies": [],
                    },
                    {
                        "id": "fetch_data",
                        "handler": "call_external_service",
                        "dependencies": ["input"],
                        "config": {"url": "http://api.example.com/data"},
                    },
                    {
                        "id": "analyze",
                        "handler": "llm_service",
                        "dependencies": ["fetch_data"],
                        "config": {
                            "prompt": "Analyze the following data",
                            "model": "gpt-4",
                        },
                    },
                    {
                        "id": "output",
                        "handler": "output",
                        "dependencies": ["analyze"],
                    },
                ]
            },
        }
        
        # Submit and trigger
        definition_id, execution_id = await orchestrator.submit_workflow(workflow_json)
        await orchestrator.trigger_workflow(execution_id, input_params={"query": "test"})
        
        # Process tasks and results
        for _ in range(15):
            await worker.process_task(f"tasks:{HandlerType.INPUT.value}")
            await worker.process_task(f"tasks:{HandlerType.CALL_EXTERNAL_SERVICE.value}")
            await worker.process_task(f"tasks:{HandlerType.LLM_SERVICE.value}")
            await worker.process_task(f"tasks:{HandlerType.OUTPUT.value}")
            
            results = await orchestrator._result_queue.consume(
                consumer_id="test-orchestrator",
                count=10,
                block_ms=500,
            )
            
            for msg_id, result_data in results:
                await orchestrator._handle_result_message(result_data)
                await orchestrator._result_queue.acknowledge(msg_id)
            
            status = await orchestrator.get_workflow_status(execution_id)
            if status and status["state"] == WorkflowState.COMPLETED.value:
                break
            
            await asyncio.sleep(0.1)
        
        # Verify completion
        status = await orchestrator.get_workflow_status(execution_id)
        assert status["state"] == WorkflowState.COMPLETED.value
        
        results = await orchestrator.get_workflow_results(execution_id)
        assert results["output_data"] is not None


class TestHappyFlowDatabasePersistence:
    """Test that workflow state is persisted to database."""
    
    @pytest.mark.asyncio
    async def test_workflow_persisted_to_database(self, orchestrator, redis_client, database):
        """Test that completed workflow is persisted to PostgreSQL."""
        from workflow_engine.storage.postgres.repository import WorkflowRepository
        
        # Create mock worker
        worker = MockWorker(redis_client)
        await worker.init()
        
        # Define simple workflow
        workflow_json = {
            "name": f"Persistence Test {datetime.utcnow().isoformat()}",
            "dag": {
                "nodes": [
                    {"id": "input", "handler": "input", "dependencies": []},
                    {"id": "output", "handler": "output", "dependencies": ["input"]},
                ]
            },
        }
        
        # Submit and trigger
        definition_id, execution_id = await orchestrator.submit_workflow(workflow_json)
        await orchestrator.trigger_workflow(execution_id)
        
        # Process until completion
        for _ in range(10):
            await worker.process_task(f"tasks:{HandlerType.INPUT.value}")
            await worker.process_task(f"tasks:{HandlerType.OUTPUT.value}")
            
            results = await orchestrator._result_queue.consume(
                consumer_id="test-orchestrator",
                count=10,
                block_ms=500,
            )
            
            for msg_id, result_data in results:
                await orchestrator._handle_result_message(result_data)
                await orchestrator._result_queue.acknowledge(msg_id)
            
            status = await orchestrator.get_workflow_status(execution_id)
            if status and status["state"] == WorkflowState.COMPLETED.value:
                break
            
            await asyncio.sleep(0.1)
        
        # Verify workflow is in database
        async with database.session() as session:
            repo = WorkflowRepository(session)
            
            # Check workflow definition
            def_model = await repo.get_workflow_definition(definition_id)
            assert def_model is not None
            assert def_model.name == workflow_json["name"]
            
            # Check workflow execution
            exec_model = await repo.get_workflow_execution(execution_id)
            assert exec_model is not None
            assert exec_model.state == WorkflowState.COMPLETED.value


if __name__ == "__main__":
    # Run with: python -m pytest tests/integration/test_happy_flow.py -v -s
    pytest.main([__file__, "-v", "-s"])
