# Design Document

This document describes key design decisions in the Workflow Orchestration Engine, focusing on detecting node readiness, handling fan-in patterns, and trade-offs made.

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Two-Step Workflow Execution](#two-step-workflow-execution)
3. [Detecting Node Readiness](#detecting-node-readiness)
4. [Handling Fan-In Patterns](#handling-fan-in-patterns)
5. [State Machine Design](#state-machine-design)
6. [DAG Validation](#dag-validation)
7. [Unified Worker Architecture](#unified-worker-architecture)
8. [Dead Letter Queue (DLQ)](#dead-letter-queue-dlq)
9. [Retry Policy & Error Handling](#retry-policy--error-handling)
10. [Concurrency Control & Backpressure](#concurrency-control--backpressure)
11. [Graceful Shutdown](#graceful-shutdown)
12. [Configuration Management](#configuration-management)
13. [Crash Recovery](#crash-recovery)
14. [Tested Scenarios](#tested-scenarios)
15. [Trade-offs](#trade-offs)
16. [Future Considerations](#future-considerations)

---

## Architecture Overview

The system consists of the following layers:

- **API Layer (FastAPI + Pydantic)**
  - Receives workflow submissions and status queries
  - Validates input using Pydantic models

- **Orchestrator**
  - Contains DAG Validator (uses Kahn's algorithm)
  - Manages State Machine for workflow/node states
  - Coordinates Fan-In patterns using Redis counters

- **Storage Layer**
  - Redis Streams for task queues
  - Redis Cache for hot state (active executions, fan-in counters)
  - PostgreSQL for persistence (completed workflows, audit log)

- **Workers**
  - Consume tasks from Redis Streams
  - Execute handlers based on task type
  - Report results back via Redis Streams

---

## Two-Step Workflow Execution

### Problem

Workflow execution has different concerns that benefit from separation:
- **Definition validation**: Ensure the DAG is valid before any execution
- **Deferred execution**: Create workflow definitions now, execute later
- **Parameterized execution**: Run the same workflow with different inputs

### Solution: Submit + Trigger Pattern

**Implementation Location**: `workflow_engine/api/routes.py`

**Step 1: Submit Workflow** (`POST /v1/workflow`)
- Validates the DAG structure (cycle detection, dependency resolution)
- Creates workflow definition in PostgreSQL
- Creates execution record in PENDING state
- Returns `workflow_definition_id` and `execution_id`
- No tasks are dispatched yet

**Step 2: Trigger Execution** (`POST /v1/workflow/trigger/{execution_id}`)
- Accepts optional `input_params` for the workflow
- Dispatches the first nodes (those with no dependencies)
- Transitions workflow state from PENDING to RUNNING
- Returns confirmation of execution start

**Benefits**:
- **Fail-fast validation**: Invalid DAGs are rejected before any execution
- **Idempotent submission**: Same workflow can be submitted multiple times
- **Flexible triggering**: Workflows can be triggered with different parameters
- **Scheduled execution**: External systems can trigger workflows at specific times

**Workflow lifecycle**:
```
PENDING (after submit) → RUNNING (after trigger) → COMPLETED/FAILED
```

---

## Detecting Node Readiness

### Problem

A node is "ready" to execute when all its dependencies have completed successfully. In a DAG with parallel branches, multiple dependencies may complete at different times, requiring efficient readiness detection.

### Solution: Dependency-Based Readiness Check

**Implementation Location**: `workflow_engine/core/dag.py` - `DAGParser.get_ready_nodes()`

**How it works**:
- Maintain a set of completed node IDs
- For each pending node, check if ALL dependencies are in the completed set
- If yes, the node is ready for execution
- Time complexity: O(dependencies) per node

### Push-based vs Pull-based Triggering

We use a **hybrid approach**:

| Dependency Type | Approach | How It Works |
|-----------------|----------|--------------|
| Simple (1 parent) | Push-based | Parent completion immediately triggers child |
| Fan-in (multiple parents) | Counter-based | Atomic decrement on each parent completion |

---

## Handling Fan-In Patterns

### Problem

Fan-in occurs when a node depends on multiple parallel branches (e.g., node E depends on nodes B, C, and D completing).

**Race condition risks**:
- Triggering the downstream node multiple times
- Missing the trigger entirely
- Losing output data from some branches

### Solution: Redis Lua Script with Atomic Counter

**Implementation Location**: `workflow_engine/orchestrator/coordinator.py`

**Key insight**: Use Redis's single-threaded execution model with Lua scripts for atomic counter operations.

**How the counter mechanism works**:

1. **Initialization** (when workflow starts):
   - Create a Redis key for the fan-in node
   - Set counter value to the number of dependencies (e.g., 3 for B, C, D)

2. **When each dependency completes**:
   - Atomically decrement the counter
   - Store that dependency's output data in a Redis hash
   - Check the new counter value:
     - If counter > 0: Still waiting for other dependencies
     - If counter = 0: All dependencies complete, trigger downstream node
     - If counter < 0: Already triggered (race condition handled gracefully)

3. **Data aggregation**:
   - Each dependency's output is stored in a Redis hash keyed by node ID
   - When triggering the fan-in node, retrieve all outputs from the hash
   - Pass aggregated outputs to the downstream node

**Why Lua scripts instead of Redis transactions**:
- Lua scripts execute atomically without interruption
- GET-DECREMENT-CHECK happens as a single operation
- No race window between checking and decrementing
- Handles duplicate triggers gracefully via negative value detection

---

## State Machine Design

### Node States

**Implementation Location**: `workflow_engine/core/state_machine.py`

| State | Description |
|-------|-------------|
| PENDING | Waiting for dependencies to complete |
| QUEUED | Dependencies met, task added to queue |
| RUNNING | Worker is executing the task |
| COMPLETED | Task finished successfully |
| FAILED | Failed after retries exhausted (includes timeouts) |
| CANCELLED | Manually cancelled |

**Valid state transitions**:
- PENDING → QUEUED, CANCELLED
- QUEUED → RUNNING, CANCELLED, FAILED
- RUNNING → COMPLETED, FAILED
- COMPLETED → (terminal, no transitions)
- FAILED → (terminal, no transitions)

### Workflow State Computation

Workflow state is derived from individual node states with this priority:
- If any node is FAILED → Workflow is FAILED
- If any node is CANCELLED → Workflow is CANCELLED
- If all nodes are COMPLETED → Workflow is COMPLETED
- Otherwise → Workflow is RUNNING

---

## DAG Validation

### Cycle Detection: Kahn's Algorithm

**Implementation Location**: `workflow_engine/core/dag.py`

**Why Kahn's Algorithm**:
- O(V + E) time complexity where V = nodes, E = edges
- Produces topological order as a byproduct (useful for execution planning)
- Clear cycle detection: if not all nodes are processed, a cycle exists

**How Kahn's Algorithm works**:
1. Calculate in-degree (number of incoming edges) for each node
2. Start with all nodes that have in-degree = 0 (no dependencies)
3. Process each node:
   - Add it to the topological order
   - Decrement in-degree of all its dependents
   - If any dependent's in-degree becomes 0, add it to the processing queue
4. If processed count ≠ total nodes, a cycle exists

**Additional validation checks**:
- All dependency references point to existing nodes
- No node depends on itself (self-loops)
- Workflow has at least one entry point (node with no dependencies)
- Workflow has at least one exit point (node with no dependents)
- All nodes are reachable from root nodes

---

## Unified Worker Architecture

### Problem

Running separate workers for each handler type (input, output, external service, LLM) introduces:
- Operational complexity (multiple deployments to manage)
- Resource inefficiency (idle workers when certain task types are rare)
- Complex scaling decisions (which worker type to scale?)

### Solution: Unified Worker

**Implementation Location**: `workflow_engine/workers/unified.py`

**How it works**:
- Single worker process registers handlers for all task types
- Worker maintains a handler map: task type → handler function
- Consumes from multiple queues (one per handler type)
- Routes each task to the appropriate handler based on type

**Queue naming convention**: `tasks:{handler_type}`
- `tasks:input`
- `tasks:output`
- `tasks:call_external_service`
- `tasks:llm_service`

### Result Reporting via Redis Streams

**How workers report results**:
- Worker completes task execution
- Worker publishes result to Redis Stream (`wf:stream:results`)
- Result includes: workflow_execution_id, node_id, success flag, output data, error message

**How orchestrator consumes results**:
- Orchestrator uses consumer groups for reliable delivery
- Multiple orchestrators can consume from the same stream
- Results are acknowledged after processing
- Unacknowledged messages are automatically reclaimed after `ORCHESTRATOR_STALE_RESULT_TIMEOUT` (default: 30s)
- Stale result claiming runs every `ORCHESTRATOR_STALE_CLAIM_INTERVAL` (default: 15s)

**Benefits over HTTP callbacks**:
- At-least-once delivery: Results persist until acknowledged
- Decoupled architecture: Workers don't need orchestrator URL
- Multi-consumer support: Consumer groups allow orchestrator scaling
- Automatic retry: Unacknowledged messages are reclaimed by other consumers
- Replay capability: Can reprocess historical results if needed

### Worker Heartbeat

**Purpose**: Detect dead workers for health monitoring

**How it works**:
- Workers send periodic heartbeats to Redis (default: every 5 seconds)
- Heartbeat updates a timestamp key for the worker
- Health endpoint checks if timestamp is within timeout threshold (default: 15 seconds)
- Workers that miss heartbeats are considered dead

---

## Dead Letter Queue (DLQ)

### Problem

Some task failures require manual investigation rather than automatic retries:
- Configuration errors (no handler registered for task type)
- Data validation errors (malformed input data)
- Programming errors (attribute errors, type errors)

These messages should be preserved for debugging rather than discarded or endlessly retried.

### Solution: Per-Queue Dead Letter Queue

**Implementation Location**: `workflow_engine/messaging/broker.py`

**Key design**: Each task queue has an associated DLQ stream (`wf:dlq:{queue_name}`).

**When messages go to DLQ**:
1. **No handler registered**: Task type not recognized by any worker (misconfiguration)
2. **Non-retryable errors**: ValueError, TypeError, KeyError, AttributeError, JSONDecodeError

**When messages do NOT go to DLQ**:
- Retries exhausted for retryable errors (network failures, timeouts)
- These are normal failures that don't need manual investigation

**DLQ message structure**:
```
{
  ...original_message_fields...,
  "original_message_id": "<stream_id>",
  "error": "<error_description>",
  "rejected_at": "<timestamp>"
}
```

**DLQ operations**:
- `get_dlq_count()`: Monitor DLQ depth for alerting
- `retry_from_dlq(count)`: Re-queue messages after fixing the underlying issue

**Important**: All failures (DLQ or not) are reported to the orchestrator via the result queue so it can fail the node and potentially the workflow.

---

## Retry Policy & Error Handling

### Problem

Transient failures (network issues, service unavailability) should be retried, but permanent failures (bad data, missing configuration) should not waste resources with repeated attempts.

### Solution: Error Classification with Configurable Retry

**Implementation Locations**:
- `workflow_engine/messaging/consumer.py` - Error handling logic
- `workflow_engine/config/settings.py` - Retry configuration

**Error classification**:

| Error Type | Retryable | Action |
|------------|-----------|--------|
| Network errors, timeouts | Yes | Re-queue with incremented attempt |
| Service unavailable | Yes | Re-queue with incremented attempt |
| ValueError, TypeError | No | Report failure + DLQ |
| KeyError, AttributeError | No | Report failure + DLQ |
| JSONDecodeError | No | Report failure + DLQ |

**Retry configuration** (via environment variables):

| Setting | Default | Description |
|---------|---------|-------------|
| `RETRY_MAX_RETRIES` | 3 | Maximum retry attempts |
| `RETRY_INITIAL_DELAY` | 1.0s | Initial retry delay |
| `RETRY_MAX_DELAY` | 60.0s | Maximum retry delay |
| `RETRY_EXPONENTIAL_BASE` | 2.0 | Exponential backoff base |
| `RETRY_JITTER` | true | Add jitter to prevent thundering herd |

**Retry flow**:
```
Attempt 1 fails → Wait (delay) → Attempt 2 fails → Wait (delay * 2) → Attempt 3 fails → Mark as failed
```

**Per-task retry override**: Tasks can specify their own retry configuration in the `retry_config` field, overriding the defaults.

---

## Concurrency Control & Backpressure

### Problem

Without concurrency limits:
- Workers may consume messages faster than they can process them
- Memory exhaustion from too many in-flight tasks
- Unfair resource allocation across queue types

### Solution: Semaphore-Controlled Task Pool

**Implementation Location**: `workflow_engine/messaging/consumer.py`

**How it works**:
1. Worker creates a semaphore with `WORKER_CONCURRENCY` permits (default: 4)
2. Before consuming a message, worker acquires a semaphore permit
3. If no permits available, consumer blocks (backpressure)
4. After task completes, permit is released
5. Tasks from ALL queues compete for the same pool

**Benefits**:
- **Backpressure**: Workers don't pull more than they can handle
- **Fair scheduling**: All queue types share the same pool
- **Configurable**: Tune `WORKER_CONCURRENCY` based on worker resources

**Configuration**:
```
WORKER_CONCURRENCY=4  # Number of concurrent tasks per worker
```

**Example with 4 concurrency**:
```
[Queue: tasks:input] → Semaphore (4 slots) → [Task Pool]
[Queue: tasks:output] →                    → Task 1, Task 2, Task 3, Task 4
[Queue: tasks:llm_service] →               → (blocks until slot available)
```

---

## Graceful Shutdown

### Problem

Abrupt worker termination can leave:
- Tasks in inconsistent state
- Messages unacknowledged (will be re-delivered)
- Resources not properly released

### Solution: Signal-Based Graceful Shutdown

**Implementation Location**: `workflow_engine/workers/base.py`

**Signal handlers**: Workers register handlers for SIGINT and SIGTERM.

**Shutdown sequence**:
1. Signal received → Set `_running = False`
2. Stop consuming new messages from queues
3. Wait for in-flight tasks to complete (up to `WORKER_GRACEFUL_SHUTDOWN_TIMEOUT`)
4. If timeout exceeded, cancel remaining tasks
5. Deregister worker from Redis
6. Close connections

**Configuration**:
```
WORKER_GRACEFUL_SHUTDOWN_TIMEOUT=30.0  # Seconds to wait for in-flight tasks
```

**Benefits**:
- Tasks complete cleanly, reducing duplicate processing
- Messages are acknowledged before shutdown
- Health checks show worker as unavailable

---

## Configuration Management

### Design: Environment-Aware Pydantic Settings

**Implementation Location**: `workflow_engine/config/settings.py`

**Environment support**: dev, test, prod

**Configuration hierarchy**:
```
Settings
├── RedisSettings (REDIS_* env vars)
├── PostgresSettings (POSTGRES_* env vars)
├── WorkerSettings (WORKER_* env vars)
│   └── StaleMessageTimeouts (STALE_TIMEOUT_* env vars)
├── OrchestratorSettings (ORCHESTRATOR_* env vars)
└── RetrySettings (RETRY_* env vars)
```

**Key settings by component**:

**Redis**:
| Setting | Default | Description |
|---------|---------|-------------|
| `REDIS_HOST` | localhost | Redis server hostname |
| `REDIS_PORT` | 6379 | Redis server port |
| `REDIS_MAX_CONNECTIONS` | 50 | Connection pool size |
| `REDIS_SOCKET_TIMEOUT` | 10.0s | Socket timeout |
| `REDIS_STREAM_BLOCK_MS` | 5000 | XREADGROUP block time |

**Worker**:
| Setting | Default | Description |
|---------|---------|-------------|
| `WORKER_CONCURRENCY` | 4 | Concurrent tasks per worker |
| `WORKER_HEARTBEAT_INTERVAL` | 5.0s | Heartbeat frequency |
| `WORKER_HEARTBEAT_TIMEOUT` | 15.0s | Dead worker threshold |
| `WORKER_GRACEFUL_SHUTDOWN_TIMEOUT` | 30.0s | Shutdown wait time |

**Per-Handler Stale Timeouts**:
| Handler | Default Timeout | Rationale |
|---------|-----------------|-----------|
| `input` | 30s | Fast handler |
| `output` | 30s | Fast handler |
| `call_external_service` | 120s | Network latency |
| `llm_service` | 180s | LLM inference time |

These timeouts determine how long a message can be pending before another worker claims it (assumes original worker died).

**Orchestrator**:
| Setting | Default | Description |
|---------|---------|-------------|
| `ORCHESTRATOR_CHECKPOINT_INTERVAL` | 10.0s | State checkpoint frequency |
| `ORCHESTRATOR_RECOVERY_TIMEOUT` | 60.0s | Min age for recovery |
| `ORCHESTRATOR_STALE_CLAIM_INTERVAL` | 15.0s | Stale result check frequency |
| `ORCHESTRATOR_STALE_RESULT_TIMEOUT` | 30.0s | Result idle threshold |

---

## Crash Recovery

### Problem

After an orchestrator crash or restart:
- In-memory workflow state is lost
- Fan-in counters in Redis may be stale or missing
- Nodes may be stuck in QUEUED/RUNNING state

### Solution: Multi-Layer Recovery

**Implementation Location**: `workflow_engine/orchestrator/engine.py`

**Recovery process on orchestrator startup**:

1. **Query for incomplete workflows**:
   - Fetch all workflows in PENDING or RUNNING state from PostgreSQL
   - Skip very recent workflows (within recovery timeout) to avoid conflicts

2. **For each incomplete workflow**:
   - Load the workflow definition from database
   - Reconstruct in-memory state from persisted data
   - Update Redis cache with current state

3. **Recover fan-in counters**:
   - For each fan-in node (nodes with multiple dependencies)
   - Check which dependencies have already completed
   - Set Redis counter to: total dependencies - completed dependencies
   - If counter is 0, dispatch the fan-in node

4. **Re-dispatch stuck nodes**:
   - Find nodes stuck in QUEUED or RUNNING state
   - Reset them to PENDING state
   - Increment attempt counter (counts as a retry)
   - Re-dispatch to task queue

### Periodic Checkpointing

**How it works**:
- Active workflows are periodically checkpointed to PostgreSQL
- Default interval: 10 seconds (`ORCHESTRATOR_CHECKPOINT_INTERVAL`)
- Redis provides sub-second state for real-time queries
- PostgreSQL provides durability for crash recovery

**Trade-off**: 10-second interval balances durability (minimal data loss) with performance (reasonable write load).

### Stale Result Claiming

**How it works**:
- Orchestrators periodically scan for stale (unacknowledged) results in the Redis Stream
- Results are considered stale after `ORCHESTRATOR_STALE_RESULT_TIMEOUT` (default: 30s)
- Claiming runs every `ORCHESTRATOR_STALE_CLAIM_INTERVAL` (default: 15s)
- When an orchestrator dies mid-processing, another orchestrator claims its pending results

**Trade-off**: Timeout must be longer than typical result processing time to avoid duplicate processing, but short enough for timely recovery.

### Pre-Recovery Result Draining

**Problem**: After an orchestrator crash, results may be waiting in the Redis Stream that haven't been processed yet. If recovery runs first, it sees stale node states and may re-dispatch nodes unnecessarily.

**Solution**: Drain all pending results BEFORE running recovery.

**Implementation Location**: `workflow_engine/orchestrator/engine.py` - `_drain_pending_results()`

**How it works**:
1. On startup, before recovery, consume all pending results from the stream
2. Process each result to update node states in memory and Redis cache
3. Only then run recovery with accurate node states

**Startup sequence**:
```
start() → _drain_pending_results() → _recover_incomplete_workflows() → start loops
```

### Worker-Level Idempotency

**Problem**: Even with pre-recovery draining, edge cases may still cause duplicate tasks:
1. Results arrive after draining but before recovery completes
2. Race conditions in distributed scenarios

**Solution**: Workers check if a node is already completed before processing.

**Implementation Location**: `workflow_engine/workers/base.py` - `_handle_task()`

**How it works**:
1. **Layer 1 (in-memory)**: Check local `_processed_keys` set for idempotency key
2. **Layer 2 (Redis)**: Query Redis cache to check if node state is COMPLETED
3. If completed, skip processing and return success with `{"skipped": True}`

**Defense in depth**:
- Orchestrator drains results before recovery (prevents most duplicates)
- Workers check Redis before processing (catches any that slip through)
- Graceful degradation (workers continue if Redis check fails)

---

## Tested Scenarios

The following scenarios have been verified through testing:

### Scenario A: Linear Chain (A → B → C)

**Test**: Sequential execution with data passing between nodes.

**Execution pattern observed**:
```
Dispatched A → A completed → Dispatched B → B completed → Dispatched C → C completed
```

**Verification**:
- ✅ Nodes execute in strict sequential order
- ✅ Each node waits for its predecessor to complete
- ✅ Data from earlier nodes is available to later nodes

### Scenario B: Fan-Out/Fan-In (A → B,C → D)

**Test**: Parallel execution with synchronization at fan-in node.

**Execution pattern observed**:
```
Dispatched A → A completed → Dispatched B (parallel) → Dispatched C (parallel)
                          → B completed → C completed → Dispatched D → D completed
```

**Verification**:
- ✅ B and C are dispatched at the same millisecond (true parallelism)
- ✅ D only dispatches after BOTH B and C complete
- ✅ D receives aggregated results from both B and C
- ✅ Redis atomic counter correctly tracks dependencies

### Scenario C: Race Condition Handling

**Test**: Ensure fan-in node triggers exactly once when dependencies complete near-simultaneously.

**Execution pattern observed**:
```
B completed at 12:22:36.289
C completed at 12:22:36.295 (6ms later)
D dispatched at 12:22:36.297 (EXACTLY ONCE)
```

**Verification**:
- ✅ D dispatched exactly once despite B and C completing 6ms apart
- ✅ No duplicate dispatches in logs
- ✅ Redis Lua script atomic counter prevents race conditions
- ✅ Counter correctly reaches zero on final dependency completion

### Scaling Scenario: 1 Orchestrator + 1 Worker

**Test**: Basic operation with minimal deployment.

**Verification**:
- ✅ Orchestrator correctly dispatches tasks to worker
- ✅ Worker processes all handler types (input, output, external service)
- ✅ Results flow back to orchestrator via Redis Streams
- ✅ Workflow state transitions correctly through lifecycle

### Scaling Scenario: 2 Orchestrators + 2 Workers

**Test**: Horizontal scaling of both components.

**Verification**:
- ✅ Both orchestrators receive API requests (via port range allocation)
- ✅ Workers consume from shared Redis Streams queues
- ✅ Consumer groups ensure each task is processed once
- ✅ System remains consistent under concurrent load

---

## Trade-offs

### 1. Redis for State vs PostgreSQL Only

| Approach | Pros | Cons |
|----------|------|------|
| **Redis (chosen)** | Fast state updates, atomic Lua scripts, natural fit for counters | Additional infrastructure, data sync complexity |
| PostgreSQL only | Single source of truth, ACID transactions | Slower for high-frequency state updates, complex locking for fan-in |

**Decision**: Use Redis for hot state (active executions) and PostgreSQL for persistence (completed workflows, audit log).

### 2. Push-based vs Pull-based Node Triggering

| Approach | Pros | Cons |
|----------|------|------|
| **Push (simple deps)** | Low latency, no polling | More complex coordination |
| Pull (polling) | Simpler implementation | Higher latency, resource waste |
| **Hybrid (chosen)** | Best of both worlds | More code paths to maintain |

**Decision**: Push for single dependencies, atomic counter for fan-in.

### 3. Lua Scripts vs Redis Transactions

| Approach | Pros | Cons |
|----------|------|------|
| **Lua scripts (chosen)** | Truly atomic, no race window | Lua learning curve, debugging harder |


**Decision**: Lua scripts for atomic fan-in coordination. The counter decrement must be atomic with the zero-check.

### 4. In-Memory State vs Pure Storage

| Approach | Pros | Cons |
|----------|------|------|
| **In-memory + cache (chosen)** | Fastest access, reduces Redis calls | Memory pressure, recovery complexity |
| Pure Redis | Stateless orchestrator, easy scaling | More Redis calls, higher latency |

**Decision**: Maintain active executions in-memory with Redis as cache/coordination layer. Trade-off: need to handle orchestrator restart (reload from Redis).

### 5. Single Orchestrator vs Distributed

| Approach | Pros | Cons |
|----------|------|------|
| **Single (current)** | Simpler, no coordination needed | Single point of failure, limited scale |
| Distributed | HA, horizontal scaling | Need leader election, partition handling |

**Decision**: Start with single orchestrator. Fan-in coordination via Redis Lua scripts already supports multiple orchestrators (atomic operations). Multiple orchestrators have been tested and work correctly with consumer groups for result processing. Future: add leader election for coordinated recovery.

### 6. Fail-Fast vs Partial Completion

| Approach | Pros | Cons |
|----------|------|------|
| **Fail-fast (chosen)** | Clear failure signal, resource savings | No partial results |
| Partial completion | Get what you can | Complex state, ambiguous results |

**Decision**: If any node fails (after retries), workflow fails. Simpler semantics, clearer error handling.

### 7. Unified Worker vs Separate Workers

| Approach | Pros | Cons |
|----------|------|------|
| **Unified (chosen)** | Simple deployment, efficient resources | All-or-nothing scaling |
| Separate workers | Fine-grained scaling, isolation | Operational complexity |

**Decision**: Single unified worker process handles all handler types. Simplifies deployment while allowing horizontal scaling via replicas.

### 8. Redis Streams vs HTTP Callback for Results

| Approach | Pros | Cons |
|----------|------|------|
| HTTP callback | Simple to implement initially | Tight coupling, lost results if orchestrator down |
| **Redis Streams (chosen)** | Decoupled, durable, multi-consumer | Slightly more complex setup |

**Decision**: Workers publish results to a Redis Stream. Orchestrators consume using consumer groups.

### 9. Checkpoint Interval Trade-off

| Interval | Pros | Cons |
|----------|------|------|
| Short (1s) | Minimal data loss on crash | High DB write load |
| **Medium (10s, chosen)** | Balanced | Up to 10s of state lost |
| Long (60s+) | Low DB load | Significant recovery delay |

**Decision**: 10-second checkpoint interval balances durability with performance.

### 10. Dead Letter Queue Strategy

| Approach | Pros | Cons |
|----------|------|------|
| DLQ all failures | Complete audit trail | DLQ bloat, noise |
| **DLQ non-retryable only (chosen)** | DLQ contains actionable items | Some failures not preserved |
| No DLQ | Simpler | Lost messages, no debugging |

**Decision**: Only non-retryable errors go to DLQ. These indicate configuration issues or bad data that need manual intervention. Retryable errors that exhaust retries are normal failures handled by workflow state.

### 11. Concurrency Control Model

| Approach | Pros | Cons |
|----------|------|------|
| **Shared semaphore (chosen)** | Fair, simple, backpressure | Coarse-grained control |
| Per-queue semaphores | Fine-grained isolation | Complex config, potential starvation |
| No concurrency control | Maximum throughput | Memory exhaustion risk |

**Decision**: Single semaphore shared across all queues. Provides natural backpressure and fair resource sharing without complex configuration.

---

## Future Considerations

### Workflow Definition Versioning

**Current State**: Workflow definitions have a unique name constraint. To update a workflow, delete the existing definition and create a new one.

**Future Enhancement**: Consider implementing workflow versioning to support:
- **Gradual rollouts**: Deploy new workflow versions while existing executions complete on the old version
- **Rollback capability**: Quickly revert to a previous version if issues are discovered
- **Audit trail**: Track changes to workflow definitions over time
- **A/B testing**: Run different versions of a workflow simultaneously


### Stream Maintenance (Trimming)

**Current State**: Redis Streams can grow unbounded as tasks and results are processed. The broker provides `trim_stream()` methods for both `TaskQueue` and `ResultQueue`, as well as `MessageBroker.trim_all_streams()` for batch trimming.

**Recommendation**: Stream trimming should be run as a **scheduled job** (cron, Kubernetes CronJob, etc.) rather than as part of the orchestrator event loop. This approach:
- **Reduces overhead**: Trimming is an O(n) operation that shouldn't compete with real-time task processing
- **Predictable timing**: Maintenance runs at known times (e.g., off-peak hours)
- **Separation of concerns**: Maintenance operations are decoupled from runtime operations


**Deployment**: Run via cron (e.g., every 6 hours) or Kubernetes CronJob.


### Rate Limiting

**Current State**: No rate limiting is implemented. Workers process tasks as fast as they can consume from queues.

**Future Enhancement**: Consider implementing rate limiting to:
- **Protect external services**: Prevent overwhelming third-party APIs with too many concurrent requests
- **Resource management**: Control system load during traffic spikes
- **Fair usage**: Ensure no single workflow monopolizes system resources
- **Cost control**: Limit expensive operations (e.g., LLM API calls) to manage costs


### Horizontal Scaling

**Current State**: Both orchestrator and workers are designed to be horizontally scalable.

**Workers**:
- Workers are stateless and can be scaled horizontally by running multiple instances
- Redis Streams consumer groups ensure each task is delivered to only one worker
- No coordination required between worker instances

**Orchestrator**:
- Fan-in coordination uses Redis Lua scripts with atomic operations, supporting multiple orchestrators
- Consumer groups on the results stream allow multiple orchestrators to process results
- Stale result claiming enables orchestrators to take over work from failed instances
- **Load balancing required**: When running multiple orchestrators, a load balancer (e.g., nginx, or cloud LB) must be placed in front to distribute API requests across instances

**Future Enhancement**: For full high availability, consider adding:
- Leader election for orchestrator (e.g., using Redis or etcd) to coordinate recovery and checkpointing
- Health-based load balancing across orchestrator instances


### Distributed Recovery Coordination

**Current State**: The crash recovery process has no distributed coordination mechanism. When an orchestrator starts, it independently queries PostgreSQL for incomplete workflows and attempts to recover them.

**Problem**: With multiple orchestrators running simultaneously:
- Multiple orchestrators could attempt to recover the same workflow concurrently
- This could lead to duplicate node dispatches and task execution
- Fan-in counters could be corrupted by concurrent recovery attempts
- Checkpoint writes could conflict, causing state inconsistencies

**Future Enhancement**: Consider implementing distributed locks for recovery coordination:

**Leader Election** (for centralized recovery):
   - Elect a single orchestrator as the recovery leader using Redis or etcd
   - Only the leader performs recovery on startup
   - Other orchestrators wait or handle only new workflow submissions
   - Leader re-election on failure

**Trade-offs**:

| Approach | Pros | Cons |
|----------|------|------|
| Leader election | Centralized control, no lock contention | Single recovery point, leader failure delays recovery |

---

## Summary

| Design Area | Decision | Rationale |
|-------------|----------|-----------|
| Readiness detection | Dependency set membership check | O(dependencies) per node, simple and correct |
| Fan-in coordination | Redis Lua atomic counters | Prevents race conditions, single trigger guarantee |
| DAG validation | Kahn's algorithm | O(V+E), produces topological order |
| State management | Explicit state machine | Clear transitions, prevents invalid states |
| Storage | Redis (hot) + PostgreSQL (cold) | Performance + durability balance |
| Worker architecture | Unified worker | Simple deployment, efficient resource use |
| Result reporting | Redis Streams | Decoupled, durable, multi-consumer support |
| Dead letter queue | DLQ for non-retryable errors only | Actionable items for investigation, avoid DLQ bloat |
| Retry policy | Error classification + exponential backoff | Retry transient failures, fail fast on permanent errors |
| Concurrency control | Shared semaphore across queues | Backpressure, fair scheduling, simple configuration |
| Graceful shutdown | Signal handlers + timeout | Clean task completion, proper resource release |
| Configuration | Pydantic settings, environment-aware | Type-safe, validated, environment-specific defaults |
| Crash recovery | Multi-layer (Redis → PostgreSQL) | Fast recovery with durability guarantee |
| Checkpointing | Periodic (10s) to PostgreSQL | Balanced write load and data loss risk |
| Workflow execution | Two-step (submit + trigger) | Validation before execution, flexible triggering |
| Fan-in race handling | Atomic Lua counter | Guarantees single trigger even with simultaneous completions |
