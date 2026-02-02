# Workflow Orchestration Engine

A production-grade event-driven workflow orchestration engine with DAG-based task execution, parallel processing, and distributed worker coordination.

## Features

- **DAG-based Workflow Definition**: Define workflows as directed acyclic graphs with JSON
- **Parallel Execution**: Independent nodes execute simultaneously for optimal performance
- **Fan-Out/Fan-In**: Support for complex parallel patterns with proper synchronization
- **Event-Driven Architecture**: Asynchronous task processing with Redis Streams
- **Template Resolution**: Dynamic data injection between nodes using `{{ node.output }}` syntax
- **Fault Tolerance**: Retry policies and graceful failure handling
- **Distributed Workers**: Scalable worker architecture with capability-based routing

> For detailed design decisions, trade-offs, and internal architecture, see [DESIGN.md](DESIGN.md).

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                         API Layer                               │
│                    (FastAPI + Pydantic)                         │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Orchestrator                               │
│    DAG Validation • State Management • Fan-In Coordination      │
└─────────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│  Redis Streams  │ │  Redis Cache    │ │  PostgreSQL     │
│  (Task Queues)  │ │  (Hot State)    │ │  (Persistence)  │
└─────────────────┘ └─────────────────┘ └─────────────────┘
              ▲
              │
┌─────────────────────────────────────────────────────────────────┐
│                     Unified Worker                              │
│         Handles all task types • Reports results via Streams    │
└─────────────────────────────────────────────────────────────────┘
```

**Components:**
- **API Layer**: Receives workflow submissions, status queries, and health checks
- **Orchestrator**: Validates DAGs, manages state, coordinates parallel execution
- **Redis**: Task queues (Streams) and hot state cache (fan-in counters, active workflows)
- **PostgreSQL**: Durable storage for workflow definitions and completed executions
- **Unified Worker**: Single process handling all handler types (input, output, external service, LLM)

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Python 3.10+ (for local development)

### Running with Docker

Start all services with a single command:

```bash
cd applied_ai
docker-compose up
```

This starts:
- **API Server**: http://localhost:8000
- **Redis**: localhost:6379
- **PostgreSQL**: localhost:5432
- **Worker**: Processing tasks from queues

### Scaling Components

Scale orchestrators and workers independently or together:

```bash
# Scale orchestrators only
docker compose up -d --scale api=3

# Scale workers only
docker compose up -d --scale worker=3

# Scale both together
docker compose up -d --scale api=2 --scale worker=2
```

> **Note:** When scaling orchestrators, Docker assigns ports from the configured range (8000-8010). Use a load balancer in production for proper request distribution.

### API Documentation

Once running, access the API documentation at:
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## Usage

### 1. Submit a Workflow

Workflow submission creates a workflow definition and execution record in PENDING state:

```bash
curl -X POST http://localhost:8000/v1/workflow \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Parallel API Fetcher",
    "dag": {
      "nodes": [
        {"id": "input", "handler": "input", "dependencies": []},
        {
          "id": "get_user",
          "handler": "call_external_service",
          "dependencies": ["input"],
          "config": {"url": "http://api.example.com/user"}
        },
        {
          "id": "get_posts",
          "handler": "call_external_service",
          "dependencies": ["input"],
          "config": {"url": "http://api.example.com/posts"}
        },
        {
          "id": "output",
          "handler": "output",
          "dependencies": ["get_user", "get_posts"]
        }
      ]
    }
  }'
```

Response:
```json
{
  "workflow_definition_id": "550e8400-e29b-41d4-a716-446655440000",
  "execution_id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
  "message": "Workflow submitted successfully"
}
```

### 2. Trigger Execution

Trigger the workflow to start processing. This two-step approach allows:
- Workflow definition validation before execution
- Deferred execution with custom input parameters
- Re-triggering the same workflow definition with different inputs

```bash
curl -X POST http://localhost:8000/v1/workflow/trigger/6ba7b810-9dad-11d1-80b4-00c04fd430c8 \
  -H "Content-Type: application/json" \
  -d '{"input_params": {"user_id": 123}}'
```

Response:
```json
{
  "execution_id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
  "message": "Workflow started successfully"
}
```

### 3. Check Status

```bash
curl http://localhost:8000/v1/workflows/6ba7b810-9dad-11d1-80b4-00c04fd430c8
```

Response:
```json
{
  "id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
  "state": "RUNNING",
  "started_at": "2024-01-29T10:30:00Z",
  "node_states": {
    "input": "COMPLETED",
    "get_user": "RUNNING",
    "get_posts": "RUNNING",
    "output": "PENDING"
  }
}
```

### 4. Get Results

```bash
curl http://localhost:8000/v1/workflows/6ba7b810-9dad-11d1-80b4-00c04fd430c8/results
```

## Example Workflows

### Linear Workflow (A → B → C)

```json
{
  "name": "Linear Processing",
  "dag": {
    "nodes": [
      {"id": "input", "handler": "input", "dependencies": []},
      {
        "id": "process",
        "handler": "call_external_service",
        "dependencies": ["input"],
        "config": {"url": "http://api.example.com/process"}
      },
      {"id": "output", "handler": "output", "dependencies": ["process"]}
    ]
  }
}
```

### Fan-Out/Fan-In (A → [B, C, D] → E)

```json
{
  "name": "Parallel Processing",
  "dag": {
    "nodes": [
      {"id": "input", "handler": "input", "dependencies": []},
      {"id": "task_b", "handler": "call_external_service", "dependencies": ["input"], "config": {"url": "http://api/b"}},
      {"id": "task_c", "handler": "call_external_service", "dependencies": ["input"], "config": {"url": "http://api/c"}},
      {"id": "task_d", "handler": "call_external_service", "dependencies": ["input"], "config": {"url": "http://api/d"}},
      {"id": "output", "handler": "output", "dependencies": ["task_b", "task_c", "task_d"]}
    ]
  }
}
```

### LLM Processing Workflow

```json
{
  "name": "LLM Analysis",
  "dag": {
    "nodes": [
      {"id": "input", "handler": "input", "dependencies": []},
      {
        "id": "fetch_data",
        "handler": "call_external_service",
        "dependencies": ["input"],
        "config": {"url": "http://api.example.com/data"}
      },
      {
        "id": "analyze",
        "handler": "llm_service",
        "dependencies": ["fetch_data"],
        "config": {
          "prompt": "Analyze this data: {{ fetch_data.output.response }}",
          "model": "gpt-4"
        }
      },
      {"id": "output", "handler": "output", "dependencies": ["analyze"]}
    ]
  }
}
```

## Node Handlers

| Handler | Description |
|---------|-------------|
| `input` | Entry point, passes through input parameters |
| `output` | Exit point, aggregates results from dependencies |
| `call_external_service` | Makes HTTP calls (mocked in demo with 1-2s latency) |
| `llm_service` | LLM processing (mocked in demo with contextual responses) |

### Handler Configuration

**External Service Handler:**
```json
{
  "id": "fetch_data",
  "handler": "call_external_service",
  "dependencies": ["input"],
  "config": {
    "url": "http://api.example.com/data",
    "method": "GET"
  }
}
```

**LLM Service Handler:**
```json
{
  "id": "analyze",
  "handler": "llm_service",
  "dependencies": ["fetch_data"],
  "config": {
    "prompt": "Analyze: {{ fetch_data.output.response }}",
    "model": "gpt-4",
    "temperature": 0.7,
    "max_tokens": 1000
  }
}
```

## Template Syntax

Reference outputs from completed nodes:

```
{{ node_id.output }}           # Full output
{{ node_id.output.field }}     # Specific field
{{ node_id.output.nested.path }} # Nested field
{{ input.param_name }}         # Workflow input parameter
```

## Running Tests

```bash
# All tests
pytest

# Unit tests only
pytest tests/unit/

# With coverage
pytest --cov=workflow_engine --cov-report=html
```

## Database Migrations

Migrations run **automatically** on Docker startup via the entrypoint script.

## Configuration

All configuration is defined in `docker-compose.yml` with sensible defaults.

**Core settings:**

| Variable | Default | Description |
|----------|---------|-------------|
| `ENVIRONMENT` | `dev` | Environment (dev/test/prod) |
| `DEBUG` | `true` | Enable debug mode |
| `LOG_LEVEL` | `INFO` | Logging level |
| `REDIS_HOST` | `redis` | Redis hostname |
| `REDIS_PORT` | `6379` | Redis port |
| `POSTGRES_HOST` | `postgres` | PostgreSQL hostname |
| `POSTGRES_PORT` | `5432` | PostgreSQL port |
| `POSTGRES_USER` | `postgres` | PostgreSQL user |
| `POSTGRES_PASSWORD` | `postgres` | PostgreSQL password |
| `POSTGRES_DATABASE` | `workflow_engine` | Database name |

**Worker settings:**

| Variable | Default | Description |
|----------|---------|-------------|
| `WORKER_CONCURRENCY` | `10` | Max concurrent tasks per worker (shared across all handler types) |
| `WORKER_HEARTBEAT_INTERVAL` | `5.0` | How often workers send heartbeats (seconds) |
| `WORKER_HEARTBEAT_TIMEOUT` | `15.0` | When to consider a worker dead (seconds) |
| `WORKER_STALE_CLAIM_INTERVAL` | `30.0` | How often to check for stale messages (seconds) |

**Stale message timeouts (per handler type):**

These control how long a message can be pending before another worker claims it (assuming the original worker died). Set these to be greater than the maximum expected task duration for each handler type.

| Variable | Default | Description |
|----------|---------|-------------|
| `STALE_TIMEOUT_INPUT` | `30` | Input handler timeout (seconds) |
| `STALE_TIMEOUT_OUTPUT` | `30` | Output handler timeout (seconds) |
| `STALE_TIMEOUT_CALL_EXTERNAL_SERVICE` | `120` | External service handler timeout (seconds) |
| `STALE_TIMEOUT_LLM_SERVICE` | `180` | LLM service handler timeout (seconds) |
| `STALE_TIMEOUT_DEFAULT` | `120` | Fallback for unknown handler types (seconds) |

> **Note:** If a stale timeout is too short, tasks may be claimed while still running, causing duplicate processing. If too long, recovery from dead workers takes longer.

**Orchestrator settings:**

| Variable | Default | Description |
|----------|---------|-------------|
| `ORCHESTRATOR_CHECKPOINT_INTERVAL` | `10.0` | How often to persist state to PostgreSQL (seconds) |
| `ORCHESTRATOR_RECOVERY_TIMEOUT` | `60.0` | Min age for workflows to recover after restart (seconds) |
| `ORCHESTRATOR_STALE_CLAIM_INTERVAL` | `15.0` | How often to check for stale results from dead orchestrators (seconds) |
| `ORCHESTRATOR_STALE_RESULT_TIMEOUT` | `30.0` | Results idle longer than this are claimed by another orchestrator (seconds) |

> **Note:** When running multiple orchestrators, if one dies while processing a result, the result remains pending in Redis. Other orchestrators will claim these stale results after `STALE_RESULT_TIMEOUT` seconds. Set this higher than the expected result processing time to avoid duplicate processing.

**Retry settings:**

| Variable | Default | Description |
|----------|---------|-------------|
| `RETRY_MAX_RETRIES` | `3` | Maximum retry attempts for failed tasks |
| `RETRY_INITIAL_DELAY` | `1.0` | Initial delay before first retry (seconds) |
| `RETRY_MAX_DELAY` | `60.0` | Maximum delay between retries (seconds) |

## API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/v1/workflow` | Submit a new workflow definition (returns in PENDING state) |
| POST | `/v1/workflow/trigger/{execution_id}` | Trigger workflow execution with optional input params |
| GET | `/v1/workflows/{execution_id}` | Get workflow status and node states |
| GET | `/v1/workflows/{execution_id}/results` | Get workflow output data |
| GET | `/v1/health` | Health check (Redis, PostgreSQL, Orchestrator, Workers) |

## Verified Test Scenarios

The following scenarios have been tested and verified:

### Scenario A: Linear Chain (A → B → C)
Sequential execution where each node waits for its predecessor. Data flows correctly through the chain.

### Scenario B: Fan-Out/Fan-In (A → B,C → D)
- Node A triggers B and C in parallel (same millisecond dispatch)
- Node D waits for BOTH B and C to complete
- D receives aggregated results from both branches

### Scenario C: Race Condition Handling
When B and C complete within milliseconds of each other, the orchestrator correctly triggers D exactly once. No duplicate dispatches occur due to atomic Redis Lua script coordination.

### Scaling Scenarios Tested
- 1 Orchestrator + 1 Worker: Basic operation verified
- 2 Orchestrators + 2 Workers: Horizontal scaling verified
