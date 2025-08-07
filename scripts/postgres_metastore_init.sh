#!/bin/bash

# PostgreSQL Metastore Initialization Script
# This script reinitializes the metastore database (useful for testing)

set -e

echo "=== PostgreSQL Metastore Reinitialization ==="

# Check if PostgreSQL is running
if ! systemctl is-active --quiet postgresql; then
    echo "Starting PostgreSQL service..."
    sudo service postgresql start
    sleep 5
fi

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL to be ready..."
MAX_ATTEMPTS=30
ATTEMPT=1
while ! sudo -u postgres pg_isready &>/dev/null; do
    if [ $ATTEMPT -ge $MAX_ATTEMPTS ]; then
        echo "❌ PostgreSQL failed to start after $MAX_ATTEMPTS attempts"
        exit 1
    fi
    echo "Attempt $ATTEMPT/$MAX_ATTEMPTS: Waiting for PostgreSQL..."
    sleep 2
    ATTEMPT=$((ATTEMPT + 1))
done

echo "✅ PostgreSQL is ready!"

# Clean up existing metastore database completely
echo "Recreating metastore database..."
sudo -u postgres psql -c "DROP DATABASE IF EXISTS metastore;"
sudo -u postgres psql -c "CREATE DATABASE metastore;"
sudo -u postgres psql -c "ALTER DATABASE metastore OWNER TO hive;"
echo "✅ Fresh metastore database created"

# Run PostgreSQL schema initialization
SCHEMA_FILE="./scripts/dev_container_scripts/spark_minimal/hive-schema-4.0.0.postgres.sql"

if [ -f "$SCHEMA_FILE" ]; then
    echo "Initializing Hive metastore schema..."
    PGPASSWORD=hivepassword psql -h localhost -U hive -d metastore -f "$SCHEMA_FILE"
    
    # Verify schema was created
    TABLES_COUNT=$(PGPASSWORD=hivepassword psql -h localhost -U hive -d metastore -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE';" | xargs)
    echo "Created $TABLES_COUNT metastore tables"
    
    if [ "$TABLES_COUNT" -gt 0 ]; then
        echo "✅ Hive metastore schema initialized successfully"
        
        # Show some key tables
        echo ""
        echo "Key metastore tables:"
        PGPASSWORD=hivepassword psql -h localhost -U hive -d metastore -c "SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename IN ('DBS', 'TBLS', 'COLUMNS_V2', 'SDS', 'SERDES') ORDER BY tablename;"
    else
        echo "❌ Hive metastore schema initialization failed"
        exit 1
    fi
else
    echo "❌ PostgreSQL schema file not found at: $SCHEMA_FILE"
    exit 1
fi

echo ""
echo "=== PostgreSQL Metastore Reinitialization Complete ==="
echo "The metastore database has been reset to a clean state."
echo "All previous tables and data have been removed."