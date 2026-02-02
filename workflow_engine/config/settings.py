"""
Environment-aware configuration settings for the workflow engine.

Supports dev, test, and prod environments with appropriate defaults.
"""

from enum import Enum
from functools import lru_cache
from typing import Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Environment(str, Enum):
    """Supported deployment environments."""
    
    DEV = "dev"
    TEST = "test"
    PROD = "prod"


class RedisSettings(BaseSettings):
    """Redis connection settings."""
    
    model_config = SettingsConfigDict(env_prefix="REDIS_")
    
    host: str = Field(default="localhost", description="Redis server hostname")
    port: int = Field(default=6379, description="Redis server port")
    db: int = Field(default=0, description="Redis database number")
    password: Optional[str] = Field(default=None, description="Redis password")
    max_connections: int = Field(default=50, description="Maximum connection pool size (prod: 50-100)")
    socket_timeout: float = Field(default=10.0, description="Socket timeout (must be > stream_block_ms/1000 + 3)")
    socket_connect_timeout: float = Field(default=5.0, description="Connection timeout")
    stream_block_ms: int = Field(default=5000, description="XREADGROUP block time in ms (must be < socket_timeout)")
    
    # Stream trimming settings to prevent unbounded growth
    stream_max_length: int = Field(
        default=10000, 
        description="Maximum stream length before trimming (approximate)"
    )
    stream_trim_interval: float = Field(
        default=300.0, 
        description="How often to trim streams (seconds). Default: 5 minutes"
    )
    
    @property
    def url(self) -> str:
        """Generate Redis connection URL."""
        auth = f":{self.password}@" if self.password else ""
        return f"redis://{auth}{self.host}:{self.port}/{self.db}"


class PostgresSettings(BaseSettings):
    """PostgreSQL connection settings."""
    
    model_config = SettingsConfigDict(env_prefix="POSTGRES_")
    
    host: str = Field(default="localhost", description="PostgreSQL server hostname")
    port: int = Field(default=5432, description="PostgreSQL server port")
    database: str = Field(default="workflow_engine", description="Database name")
    user: str = Field(default="postgres", description="Database user")
    password: str = Field(default="postgres", description="Database password")
    pool_size: int = Field(default=10, description="Connection pool size (prod: 10-20)")
    max_overflow: int = Field(default=20, description="Max overflow connections (prod: 20-30)")
    pool_timeout: float = Field(default=10.0, description="Pool timeout in seconds (fail fast)")
    
    @property
    def url(self) -> str:
        """Generate PostgreSQL connection URL."""
        return (
            f"postgresql+asyncpg://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )
    
    @property
    def sync_url(self) -> str:
        """Generate synchronous PostgreSQL connection URL for migrations."""
        return (
            f"postgresql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )


class StaleMessageTimeouts(BaseSettings):
    """
    Per-handler stale message timeouts.
    
    These define how long a message can be pending before another worker
    claims it (assuming the original worker died). Should be set to:
    max_expected_task_duration + buffer
    
    Too short: Tasks get claimed while still running (duplicate processing)
    Too long: Dead worker's tasks take longer to recover
    """
    
    model_config = SettingsConfigDict(env_prefix="STALE_TIMEOUT_")
    
    # Fast handlers - short timeout
    input: float = Field(default=30.0, description="Input handler stale timeout (seconds)")
    output: float = Field(default=30.0, description="Output handler stale timeout (seconds)")
    
    # Slower handlers - longer timeout
    call_external_service: float = Field(
        default=120.0, 
        description="External service handler stale timeout (seconds)"
    )
    llm_service: float = Field(
        default=180.0, 
        description="LLM service handler stale timeout (seconds)"
    )
    
    # Fallback for unknown handler types
    default: float = Field(default=120.0, description="Default stale timeout (seconds)")
    
    def get_timeout_ms(self, handler_type: str) -> int:
        """Get timeout in milliseconds for a handler type."""
        timeout_seconds = getattr(self, handler_type, self.default)
        return int(timeout_seconds * 1000)


class HandlerTimeouts(BaseSettings):
    """
    Per-handler task execution timeouts.
    
    These define the maximum time a task can run before being cancelled.
    Should be set based on expected task duration + buffer.
    """
    
    model_config = SettingsConfigDict(env_prefix="HANDLER_TIMEOUT_")
    
    # Fast handlers - short timeout
    input: float = Field(default=30.0, description="Input handler timeout (seconds)")
    output: float = Field(default=30.0, description="Output handler timeout (seconds)")
    
    # Slower handlers - longer timeout
    call_external_service: float = Field(
        default=60.0, 
        description="External service handler timeout (seconds)"
    )
    llm_service: float = Field(
        default=120.0, 
        description="LLM service handler timeout (seconds)"
    )
    
    # Fallback for unknown handler types
    default: float = Field(default=60.0, description="Default handler timeout (seconds)")
    
    def get_timeout(self, handler_type: str) -> float:
        """Get timeout in seconds for a handler type."""
        return getattr(self, handler_type, self.default)


class WorkerSettings(BaseSettings):
    """Worker configuration settings."""
    
    model_config = SettingsConfigDict(env_prefix="WORKER_")
    
    concurrency: int = Field(default=4, description="Number of concurrent tasks")
    heartbeat_interval: float = Field(default=5.0, description="Heartbeat interval (seconds)")
    heartbeat_timeout: float = Field(default=15.0, description="Heartbeat timeout (seconds)")
    graceful_shutdown_timeout: float = Field(default=30.0, description="Graceful shutdown timeout")
    stale_claim_interval: float = Field(default=30.0, description="How often to check for stale messages (seconds)")
    
    # Per-handler stale message timeouts
    stale_timeouts: StaleMessageTimeouts = Field(default_factory=StaleMessageTimeouts)
    
    # Per-handler task execution timeouts
    handler_timeouts: HandlerTimeouts = Field(default_factory=HandlerTimeouts)


class OrchestratorSettings(BaseSettings):
    """Orchestrator configuration settings."""
    
    model_config = SettingsConfigDict(env_prefix="ORCHESTRATOR_")
    
    checkpoint_interval: float = Field(default=10.0, description="State checkpoint interval")
    recovery_timeout: float = Field(default=60.0, description="Min age for workflows to recover (seconds)")
    stale_claim_interval: float = Field(default=15.0, description="How often to check for stale results (seconds)")
    stale_result_timeout: float = Field(default=30.0, description="Results idle longer than this are claimed (seconds)")


class RetrySettings(BaseSettings):
    """Default retry policy settings."""
    
    model_config = SettingsConfigDict(env_prefix="RETRY_")
    
    max_retries: int = Field(default=3, description="Maximum retry attempts")
    initial_delay: float = Field(default=1.0, description="Initial retry delay (seconds)")
    max_delay: float = Field(default=60.0, description="Maximum retry delay (seconds)")
    exponential_base: float = Field(default=2.0, description="Exponential backoff base")
    jitter: bool = Field(default=True, description="Add jitter to retry delays")


class Settings(BaseSettings):
    """Main application settings."""
    
    model_config = SettingsConfigDict(
        case_sensitive=False,  # REDIS_HOST and redis_host both work
        extra="ignore",        # Ignore unknown environment variables
    )
    
    # Application
    app_name: str = Field(default="Workflow Orchestration Engine")
    environment: Environment = Field(default=Environment.DEV)
    debug: bool = Field(default=False)
    log_level: str = Field(default="INFO")
    
    # Sub-settings
    redis: RedisSettings = Field(default_factory=RedisSettings)
    postgres: PostgresSettings = Field(default_factory=PostgresSettings)
    worker: WorkerSettings = Field(default_factory=WorkerSettings)
    orchestrator: OrchestratorSettings = Field(default_factory=OrchestratorSettings)
    retry: RetrySettings = Field(default_factory=RetrySettings)
    
    @field_validator("environment", mode="before")
    @classmethod
    def validate_environment(cls, v: str | Environment) -> Environment:
        """Validate and convert environment string to enum."""
        if isinstance(v, Environment):
            return v
        return Environment(v.lower())
    
    @property
    def is_development(self) -> bool:
        """Check if running in development mode."""
        return self.environment == Environment.DEV
    
    @property
    def is_testing(self) -> bool:
        """Check if running in test mode."""
        return self.environment == Environment.TEST
    
    @property
    def is_production(self) -> bool:
        """Check if running in production mode."""
        return self.environment == Environment.PROD


@lru_cache()
def get_settings() -> Settings:
    """
    Get cached settings instance.
    
    Uses lru_cache to ensure settings are only loaded once.
    """
    return Settings()
