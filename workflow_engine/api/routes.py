"""
FastAPI routes for the workflow engine API.

Implements the core API endpoints:
- POST /workflow - Submit workflow
- POST /workflow/trigger/:execution_id - Trigger execution
- GET /workflows/:id - Get status
- GET /workflows/:id/results - Get results
- GET /health - Health check

Note: Task results are reported via Redis Streams, not HTTP endpoints.
Workers publish results to 'wf:stream:results' and orchestrators consume them.
"""

from typing import Any, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field

router = APIRouter(prefix="/v1", tags=["workflows"])


# ==================== Request/Response Models ====================

class WorkflowSubmitRequest(BaseModel):
    """Request body for workflow submission."""
    
    name: str = Field(..., min_length=1, max_length=255)
    dag: dict[str, Any] = Field(..., description="DAG definition with nodes")
    
    class Config:
        json_schema_extra = {
            "example": {
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
                        {"id": "output", "handler": "output", "dependencies": ["get_user"]}
                    ]
                }
            }
        }


class WorkflowSubmitResponse(BaseModel):
    """Response for workflow submission."""
    
    workflow_definition_id: str
    execution_id: str
    message: str = "Workflow submitted successfully"


class WorkflowTriggerRequest(BaseModel):
    """Request body for triggering workflow execution."""
    
    input_params: Optional[dict[str, Any]] = Field(
        default=None,
        description="Input parameters for the workflow"
    )


class WorkflowTriggerResponse(BaseModel):
    """Response for workflow trigger."""
    
    execution_id: str
    message: str = "Workflow triggered successfully"


class WorkflowStatusResponse(BaseModel):
    """Response for workflow status."""
    
    id: str
    state: str
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    node_states: dict[str, str] = Field(default_factory=dict)


class WorkflowResultsResponse(BaseModel):
    """Response for workflow results."""
    
    id: str
    state: str
    output_data: Optional[dict[str, Any]] = None
    completed_at: Optional[str] = None


class HealthResponse(BaseModel):
    """Health check response."""
    
    status: str
    version: str
    services: dict[str, str]


# ==================== Dependency Injection ====================

async def get_orchestrator(request: Request):
    """Get orchestrator from app state."""
    return request.app.state.orchestrator


# ==================== Routes ====================

@router.post(
    "/workflow",
    response_model=WorkflowSubmitResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Submit a new workflow",
    description="Submit a JSON workflow definition for execution. Returns execution ID."
)
async def submit_workflow(
    request: WorkflowSubmitRequest,
    orchestrator=Depends(get_orchestrator),
) -> WorkflowSubmitResponse:
    """Submit a new workflow for execution."""
    try:
        workflow_json = {
            "name": request.name,
            "dag": request.dag,
        }
        
        definition_id, execution_id = await orchestrator.submit_workflow(workflow_json)
        
        return WorkflowSubmitResponse(
            workflow_definition_id=str(definition_id),
            execution_id=str(execution_id),
        )
    
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to submit workflow: {str(e)}",
        )


@router.post(
    "/workflow/trigger/{execution_id}",
    response_model=WorkflowTriggerResponse,
    summary="Trigger workflow execution",
    description="Start or resume execution of a workflow. Works for PENDING (start) or PAUSED (resume) workflows."
)
async def trigger_workflow(
    execution_id: UUID,
    request: Optional[WorkflowTriggerRequest] = None,
    orchestrator=Depends(get_orchestrator),
) -> WorkflowTriggerResponse:
    """Trigger execution of a submitted workflow (start or resume)."""
    try:
        # Get current state to determine action type
        status_info = await orchestrator.get_workflow_status(execution_id)
        was_paused = status_info and status_info.get("state") == "PAUSED"
        
        input_params = request.input_params if request else None
        await orchestrator.trigger_workflow(execution_id, input_params)
        
        message = "Workflow resumed successfully" if was_paused else "Workflow started successfully"
        
        return WorkflowTriggerResponse(
            execution_id=str(execution_id),
            message=message,
        )
    
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to trigger workflow: {str(e)}",
        )


@router.get(
    "/workflows/{execution_id}",
    response_model=WorkflowStatusResponse,
    summary="Get workflow status",
    description="Retrieve the current status of a workflow execution."
)
async def get_workflow_status(
    execution_id: UUID,
    orchestrator=Depends(get_orchestrator),
) -> WorkflowStatusResponse:
    """Get current status of a workflow execution."""
    result = await orchestrator.get_workflow_status(execution_id)
    
    if not result:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Workflow execution not found: {execution_id}",
        )
    
    return WorkflowStatusResponse(**result)


@router.get(
    "/workflows/{execution_id}/results",
    response_model=WorkflowResultsResponse,
    summary="Get workflow results",
    description="Retrieve the final output of a completed workflow execution."
)
async def get_workflow_results(
    execution_id: UUID,
    orchestrator=Depends(get_orchestrator),
) -> WorkflowResultsResponse:
    """Get results of a workflow execution."""
    result = await orchestrator.get_workflow_results(execution_id)
    
    if not result:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Workflow execution not found: {execution_id}",
        )
    
    return WorkflowResultsResponse(**result)


# ==================== Health Check Routes ====================

@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Health check",
    description="Check the health status of all workflow engine services."
)
async def health_check(request: Request) -> HealthResponse:
    """Check health of all services."""
    from sqlalchemy import text
    from workflow_engine import __version__
    from workflow_engine.storage.postgres.database import get_database
    from workflow_engine.storage.redis.cache import RedisCache
    
    services = {}
    
    # Check Redis
    try:
        redis_client = request.app.state.redis
        await redis_client.ping()
        services["redis"] = "healthy"
    except Exception:
        services["redis"] = "unhealthy"
    
    # Check PostgreSQL
    try:
        database = await get_database()
        async with database.session() as session:
            await session.execute(text("SELECT 1"))
        services["postgres"] = "healthy"
    except Exception:
        services["postgres"] = "unhealthy"
    
    # Check Orchestrator
    try:
        orchestrator = request.app.state.orchestrator
        services["orchestrator"] = "healthy" if orchestrator._running else "unhealthy"
    except Exception:
        services["orchestrator"] = "unhealthy"
    
    # Check Workers (via heartbeats in Redis)
    try:
        redis_client = request.app.state.redis
        cache = RedisCache(redis_client)
        active_workers = await cache.get_active_workers()
        worker_count = len(active_workers)
        services["workers"] = f"healthy ({worker_count} active)" if worker_count > 0 else "unhealthy (0 active)"
    except Exception:
        services["workers"] = "unknown"
    
    # Determine overall status
    unhealthy_count = sum(1 for s in services.values() if "unhealthy" in s)
    if unhealthy_count == 0:
        overall_status = "healthy"
    elif unhealthy_count == len(services):
        overall_status = "unhealthy"
    else:
        overall_status = "degraded"
    
    return HealthResponse(
        status=overall_status,
        version=__version__,
        services=services,
    )
