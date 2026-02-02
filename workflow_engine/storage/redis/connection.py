"""
Redis connection management.

Provides connection pooling and lifecycle management.
"""

from typing import Optional

import redis.asyncio as redis
from redis.asyncio.connection import ConnectionPool

from workflow_engine.config import get_settings


class RedisConnection:
    """
    Redis connection manager with connection pooling.
    """
    
    def __init__(self):
        self._pool: Optional[ConnectionPool] = None
        self._client: Optional[redis.Redis] = None
    
    async def init(self) -> None:
        """Initialize Redis connection pool."""
        settings = get_settings()
        
        self._pool = ConnectionPool(
            host=settings.redis.host,
            port=settings.redis.port,
            db=settings.redis.db,
            password=settings.redis.password,
            max_connections=settings.redis.max_connections,
            socket_timeout=settings.redis.socket_timeout,
            socket_connect_timeout=settings.redis.socket_connect_timeout,
            decode_responses=True,
        )
        
        self._client = redis.Redis(connection_pool=self._pool)
        
        # Test connection
        await self._client.ping()
    
    async def close(self) -> None:
        """Close Redis connections."""
        if self._client:
            await self._client.close()
            self._client = None
        
        if self._pool:
            await self._pool.disconnect()
            self._pool = None
    
    @property
    def client(self) -> redis.Redis:
        """Get Redis client."""
        if self._client is None:
            raise RuntimeError("Redis not initialized. Call init() first.")
        return self._client
    
    async def health_check(self) -> bool:
        """Check Redis connection health."""
        try:
            if self._client:
                await self._client.ping()
                return True
        except Exception:
            pass
        return False


# Global Redis connection instance
_redis_connection: Optional[RedisConnection] = None


async def get_redis() -> redis.Redis:
    """
    Get the global Redis client.
    
    Initializes connection on first call.
    """
    global _redis_connection
    
    if _redis_connection is None:
        _redis_connection = RedisConnection()
        await _redis_connection.init()
    
    return _redis_connection.client


async def close_redis() -> None:
    """Close the global Redis connection."""
    global _redis_connection
    
    if _redis_connection is not None:
        await _redis_connection.close()
        _redis_connection = None


async def get_redis_connection() -> RedisConnection:
    """Get the Redis connection manager."""
    global _redis_connection
    
    if _redis_connection is None:
        _redis_connection = RedisConnection()
        await _redis_connection.init()
    
    return _redis_connection
