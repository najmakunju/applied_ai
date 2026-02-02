#!/bin/bash
set -e

# Migration retry configuration
MAX_RETRIES=30
INITIAL_DELAY=1
MAX_DELAY=10

# Run migrations with retry logic (waits for database to be ready)
run_migrations() {
    local attempt=1
    local delay=$INITIAL_DELAY
    
    echo "Running database migrations..."
    
    while [ $attempt -le $MAX_RETRIES ]; do
        echo "Migration attempt $attempt of $MAX_RETRIES..."
        
        if alembic upgrade head 2>&1; then
            echo "Database migrations completed successfully."
            return 0
        fi
        
        if [ $attempt -eq $MAX_RETRIES ]; then
            echo "ERROR: Failed to run migrations after $MAX_RETRIES attempts."
            return 1
        fi
        
        echo "Database not ready. Retrying in ${delay}s..."
        sleep $delay
        
        # Exponential backoff with cap
        delay=$((delay * 2))
        if [ $delay -gt $MAX_DELAY ]; then
            delay=$MAX_DELAY
        fi
        
        attempt=$((attempt + 1))
    done
}

# Only run migrations if SKIP_MIGRATIONS is not set
# Workers should skip migrations - only the API service runs them
if [ "${SKIP_MIGRATIONS}" != "true" ]; then
    run_migrations
else
    echo "Skipping database migrations (SKIP_MIGRATIONS=true)"
fi

echo "Starting application..."

# Add --reload flag for uvicorn if UVICORN_RELOAD is set
if [ "${UVICORN_RELOAD}" = "true" ] && [ "$1" = "uvicorn" ]; then
    echo "Hot reload enabled"
    exec "$@" --reload --reload-dir /app/workflow_engine
else
    exec "$@"
fi
