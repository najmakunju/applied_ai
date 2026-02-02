"""
FastAPI application factory.

Creates and configures the workflow engine API application.
"""

import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from workflow_engine import __version__
from workflow_engine.api.routes import router
from workflow_engine.config import get_settings
from workflow_engine.orchestrator.engine import WorkflowOrchestrator
from workflow_engine.storage.postgres.database import Database
from workflow_engine.storage.redis.connection import close_redis, get_redis

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    Application lifespan manager.
    
    Handles startup and shutdown events.
    """
    settings = get_settings()
    
    # Startup
    logger.info("Starting Workflow Orchestration Engine...")
    
    # Initialize Redis
    redis_client = await get_redis()
    app.state.redis = redis_client
    logger.info("Redis connection established")
    
    # Initialize Database
    database = Database()
    await database.init()
    app.state.database = database
    logger.info("Database connection established")
    
    # Initialize orchestrator with database for persistence
    orchestrator = WorkflowOrchestrator(redis_client, database=database)
    await orchestrator.init()
    await orchestrator.start()
    app.state.orchestrator = orchestrator
    logger.info("Orchestrator initialized")
    
    logger.info(
        f"Workflow Engine started - Environment: {settings.environment.value}"
    )
    
    yield
    
    # Shutdown
    logger.info("Shutting down Workflow Orchestration Engine...")
    
    # Stop orchestrator
    await orchestrator.stop()
    
    # Close Database
    await database.close()
    
    # Close Redis
    await close_redis()
    
    logger.info("Workflow Engine shutdown complete")


def create_app() -> FastAPI:
    """
    Create and configure the FastAPI application.
    """
    settings = get_settings()
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    
    app = FastAPI(
        title=settings.app_name,
        description="Production-grade event-driven workflow orchestration engine",
        version=__version__,
        lifespan=lifespan,
        docs_url="/docs" if settings.is_development else None,
        redoc_url="/redoc" if settings.is_development else None,
    )
    
    # Add CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"] if settings.is_development else [],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    # Include routers
    app.include_router(router)
    
    # Root endpoint
    @app.get("/", tags=["root"])
    async def root():
        return {
            "name": settings.app_name,
            "version": __version__,
            "status": "running",
        }
    
    return app


# Application instance for uvicorn
app = create_app()
