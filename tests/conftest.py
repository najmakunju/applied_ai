"""
Pytest fixtures and configuration for tests.
"""

import asyncio
from typing import AsyncGenerator, Generator
from uuid import uuid4

import pytest
import pytest_asyncio
from httpx import AsyncClient

from workflow_engine.api.app import create_app
from workflow_engine.config import Settings, Environment


@pytest.fixture(scope="session")
def event_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    """Create event loop for async tests."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def test_settings() -> Settings:
    """Create test settings."""
    return Settings(
        environment=Environment.TEST,
        debug=True,
        log_level="DEBUG",
    )


@pytest.fixture
def sample_linear_workflow() -> dict:
    """Sample linear workflow: A -> B -> C."""
    return {
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


@pytest.fixture
def sample_fanout_fanin_workflow() -> dict:
    """Sample fan-out/fan-in workflow: A -> (B, C) -> D."""
    return {
        "name": "Fan-Out Fan-In Test Workflow",
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
                    "config": {
                        "url": "http://api.example.com/user",
                    },
                },
                {
                    "id": "get_posts",
                    "handler": "call_external_service",
                    "dependencies": ["input"],
                    "config": {
                        "url": "http://api.example.com/posts",
                    },
                },
                {
                    "id": "get_comments",
                    "handler": "call_external_service",
                    "dependencies": ["input"],
                    "config": {
                        "url": "http://api.example.com/comments",
                    },
                },
                {
                    "id": "output",
                    "handler": "output",
                    "dependencies": ["get_user", "get_posts", "get_comments"],
                },
            ]
        },
    }


@pytest.fixture
def sample_cyclic_workflow() -> dict:
    """Sample invalid workflow with cycle: A -> B -> C -> A."""
    return {
        "name": "Cyclic Workflow (Invalid)",
        "dag": {
            "nodes": [
                {
                    "id": "a",
                    "handler": "input",
                    "dependencies": ["c"],
                },
                {
                    "id": "b",
                    "handler": "call_external_service",
                    "dependencies": ["a"],
                    "config": {"url": "http://example.com"},
                },
                {
                    "id": "c",
                    "handler": "output",
                    "dependencies": ["b"],
                },
            ]
        },
    }


@pytest.fixture
def sample_workflow_with_templates() -> dict:
    """Sample workflow with template variables."""
    return {
        "name": "Template Test Workflow",
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
                    "config": {
                        "url": "http://api.example.com/user/{{ input.user_id }}",
                    },
                },
                {
                    "id": "llm_process",
                    "handler": "llm_service",
                    "dependencies": ["get_user"],
                    "config": {
                        "prompt": "Analyze user: {{ get_user.output.name }}",
                        "model": "gpt-4",
                    },
                },
                {
                    "id": "output",
                    "handler": "output",
                    "dependencies": ["llm_process"],
                },
            ]
        },
    }


@pytest.fixture
def example_payload_workflow() -> dict:
    """The exact example payload from the task description."""
    return {
        "name": "Parallel API Fetcher",
        "dag": {
            "nodes": [
                {
                    "id": "input",
                    "handler": "input",
                    "dependencies": []
                },
                {
                    "id": "get_user",
                    "handler": "call_external_service",
                    "dependencies": ["input"],
                    "config": {
                        "url": "http://localhost:8911/document/policy/list"
                    }
                },
                {
                    "id": "get_posts",
                    "handler": "call_external_service",
                    "dependencies": ["input"],
                    "config": {
                        "url": "http://localhost:8911/document/policy/list"
                    }
                },
                {
                    "id": "get_comments",
                    "handler": "call_external_service",
                    "dependencies": ["input"],
                    "config": {
                        "url": "http://localhost:8911/document/policy/list"
                    }
                },
                {
                    "id": "output",
                    "handler": "output",
                    "dependencies": ["get_user", "get_posts", "get_comments"]
                }
            ]
        }
    }
