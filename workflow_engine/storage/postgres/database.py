"""
Database connection management with connection pooling.

Provides async database sessions with proper lifecycle management.
"""

from contextlib import asynccontextmanager
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from workflow_engine.config import get_settings


class Database:
    """
    Database connection manager.
    
    Handles connection pooling and session management.
    """
    
    def __init__(self):
        self._engine: AsyncEngine | None = None
        self._session_factory: async_sessionmaker[AsyncSession] | None = None
    
    async def init(self) -> None:
        """Initialize database engine and session factory."""
        settings = get_settings()
        
        self._engine = create_async_engine(
            settings.postgres.url,
            pool_size=settings.postgres.pool_size,
            max_overflow=settings.postgres.max_overflow,
            pool_timeout=settings.postgres.pool_timeout,
            # pool_pre_ping=True,  # Enable connection health checks
        )
        
        self._session_factory = async_sessionmaker(
            self._engine,
            class_=AsyncSession,
            expire_on_commit=False,
            autoflush=False,
        )
    
    async def close(self) -> None:
        """Close database connections."""
        if self._engine:
            await self._engine.dispose()
            self._engine = None
            self._session_factory = None
    
    @property
    def engine(self) -> AsyncEngine:
        """Get database engine."""
        if self._engine is None:
            raise RuntimeError("Database not initialized. Call init() first.")
        return self._engine
    
    @asynccontextmanager
    async def session(self) -> AsyncGenerator[AsyncSession, None]:
        """
        Get a database session.
        
        Usage:
            async with database.session() as session:
                # Use session for database operations
                result = await session.execute(query)
        """
        if self._session_factory is None:
            raise RuntimeError("Database not initialized. Call init() first.")
        
        session = self._session_factory()
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()
    
    @asynccontextmanager
    async def transaction(self) -> AsyncGenerator[AsyncSession, None]:
        """
        Get a database session with explicit transaction.
        
        Use this when you need to control transaction boundaries.
        """
        if self._session_factory is None:
            raise RuntimeError("Database not initialized. Call init() first.")
        
        session = self._session_factory()
        try:
            async with session.begin():
                yield session
        finally:
            await session.close()


# Global database instance
_database: Database | None = None


async def get_database() -> Database:
    """
    Get the global database instance.
    
    Initializes the database on first call.
    """
    global _database
    
    if _database is None:
        _database = Database()
        await _database.init()
    
    return _database


async def close_database() -> None:
    """Close the global database instance."""
    global _database
    
    if _database is not None:
        await _database.close()
        _database = None
