# Design Document

This document describes key design decisions in the Workflow Orchestration Engine, focusing on detecting node readiness, handling fan-in patterns, and trade-offs made.

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Detecting Node Readiness](#detecting-node-readiness)
3. [Handling Fan-In Patterns](#handling-fan-in-patterns)
4. [State Machine Design](#state-machine-design)
5. [DAG Validation](#dag-validation)
6. [Unified Worker Architecture](#unified-worker-architecture)
7. [Crash Recovery](#crash-recovery)
8. [Trade-offs](#trade-offs)
9. [Future Considerations](#future-considerations)

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
| MULTI/EXEC transactions | Familiar Redis commands | Not atomic (optimistic locking), can fail |
| Distributed locks | Works for any operation | High overhead, deadlock risk |

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

**Decision**: Start with single orchestrator. Fan-in coordination via Redis Lua scripts already supports multiple orchestrators (atomic operations). Future: add leader election for HA.

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

---

## Future Considerations

### Workflow Definition Versioning

**Current State**: Workflow definitions have a unique name constraint. To update a workflow, delete the existing definition and create a new one.

**Future Enhancement**: Consider implementing workflow versioning to support:
- **Gradual rollouts**: Deploy new workflow versions while existing executions complete on the old version
- **Rollback capability**: Quickly revert to a previous version if issues are discovered
- **Audit trail**: Track changes to workflow definitions over time
- **A/B testing**: Run different versions of a workflow simultaneously

**Potential Implementation**:
- Add version column to workflow_definitions with unique constraint on (name, version)
- Auto-increment version when submitting a workflow with an existing name
- Store workflow_version in executions to track which version was used
- Add API endpoints for version management
- Support aliases (e.g., "latest", "stable") pointing to specific versions

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
| Crash recovery | Multi-layer (Redis → PostgreSQL) | Fast recovery with durability guarantee |
| Checkpointing | Periodic (10s) to PostgreSQL | Balanced write load and data loss risk |
