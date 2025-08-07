#!/bin/bash

# PostgreSQL Metastore Setup Script for Spark
# Cross-platform compatible (x64/ARM)

set -e

echo "=== PostgreSQL Metastore Setup for Spark ==="
echo "This script will install and configure PostgreSQL for use as a Hive metastore"

# Detect architecture
ARCH=$(uname -m)
echo "Detected architecture: $ARCH"

# Determine PostgreSQL version based on architecture
POSTGRES_VERSION="15"

# Check if running inside devcontainer or with sudo access
if [ "$EUID" -ne 0 ] && ! sudo -n true 2>/dev/null; then 
    echo "This script requires sudo access. Please run with sudo or ensure passwordless sudo is configured."
    exit 1
fi

# Function to run commands with sudo if needed
run_cmd() {
    local cmd="$*"
    if [ "$EUID" -eq 0 ]; then
        if ! "$@"; then
            echo "❌ Command failed: $cmd" >&2
            return 1
        fi
    else
        if ! sudo "$@"; then
            echo "❌ Command failed: sudo $cmd" >&2
            return 1
        fi
    fi
}

# Step 1: Install PostgreSQL
echo "Step 1: Installing PostgreSQL..."

# Install locales and ensure UTF-8 locale is available
echo "Setting up locales for PostgreSQL..."
run_cmd apt-get update
run_cmd apt-get install -y locales wget gnupg2 lsb-release

# Configure and generate locales
echo "Configuring locales..."
run_cmd locale-gen en_US.UTF-8
run_cmd update-locale LANG=en_US.UTF-8

# Verify locale is available and determine the correct format
echo "Available locales:"
locale -a | grep -i utf

# Check for locale availability and use the correct format
# Test what PostgreSQL actually accepts for locale names
echo "Testing PostgreSQL locale support..."

# Try to determine which locale PostgreSQL will accept
PG_LOCALE_COLLATE=""
PG_LOCALE_CTYPE=""

# Test en_US.utf8 first
if sudo -u postgres psql -c "CREATE DATABASE test_locale_check WITH ENCODING='UTF8' LC_COLLATE='en_US.utf8' LC_CTYPE='en_US.utf8' TEMPLATE=template0;" &>/dev/null; then
    sudo -u postgres psql -c "DROP DATABASE test_locale_check;" &>/dev/null
    echo "✅ PostgreSQL accepts en_US.utf8 locale"
    PG_LOCALE_COLLATE="en_US.utf8"
    PG_LOCALE_CTYPE="en_US.utf8"
# Test en_US.UTF-8
elif sudo -u postgres psql -c "CREATE DATABASE test_locale_check WITH ENCODING='UTF8' LC_COLLATE='en_US.UTF-8' LC_CTYPE='en_US.UTF-8' TEMPLATE=template0;" &>/dev/null; then
    sudo -u postgres psql -c "DROP DATABASE test_locale_check;" &>/dev/null
    echo "✅ PostgreSQL accepts en_US.UTF-8 locale"
    PG_LOCALE_COLLATE="en_US.UTF-8"
    PG_LOCALE_CTYPE="en_US.UTF-8"
# Fall back to C.UTF-8
elif sudo -u postgres psql -c "CREATE DATABASE test_locale_check WITH ENCODING='UTF8' LC_COLLATE='C.UTF-8' LC_CTYPE='C.UTF-8' TEMPLATE=template0;" &>/dev/null; then
    sudo -u postgres psql -c "DROP DATABASE test_locale_check;" &>/dev/null
    echo "⚠️  Using C.UTF-8 locale (PostgreSQL doesn't support en_US locale)"
    PG_LOCALE_COLLATE="C.UTF-8"
    PG_LOCALE_CTYPE="C.UTF-8"
# Final fallback to C
else
    echo "⚠️  Using C locale as final fallback"
    PG_LOCALE_COLLATE="C"
    PG_LOCALE_CTYPE="C"
fi

# Set shell locale based on what's available in the system
if locale -a | grep -q "^en_US\.utf8$"; then
    echo "Setting shell locale to en_US.utf8"
    export LC_ALL=en_US.utf8
    export LANG=en_US.utf8
    export LANGUAGE=en_US.utf8
elif locale -a | grep -q "^en_US\.UTF-8$"; then
    echo "Setting shell locale to en_US.UTF-8"
    export LC_ALL=en_US.UTF-8
    export LANG=en_US.UTF-8
    export LANGUAGE=en_US.UTF-8
elif locale -a | grep -q "^C\.utf8$"; then
    echo "Setting shell locale to C.utf8"
    export LC_ALL=C.utf8
    export LANG=C.utf8
    export LANGUAGE=C.utf8
else
    echo "Setting shell locale to C"
    export LC_ALL=C
    export LANG=C
    export LANGUAGE=C
fi

# Use the PostgreSQL-compatible locale for database creation
export LOCALE_COLLATE="$PG_LOCALE_COLLATE"
export LOCALE_CTYPE="$PG_LOCALE_CTYPE"

echo "Using shell locale: LANG=$LANG, LC_ALL=$LC_ALL"
echo "Using PostgreSQL locale: LC_COLLATE='$LOCALE_COLLATE', LC_CTYPE='$LOCALE_CTYPE'"

# Note about locale selection
if [[ "$LOCALE_COLLATE" == "C.UTF-8" ]]; then
    echo "ℹ️  Note: Using C.UTF-8 locale for PostgreSQL (provides UTF-8 support with basic collation)"
elif [[ "$LOCALE_COLLATE" == "C" ]]; then
    echo "⚠️  Note: Using C locale for PostgreSQL (ASCII only, no locale-specific sorting)"
fi

# Add PostgreSQL APT repository
echo "Adding PostgreSQL APT repository..."
run_cmd sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
run_cmd apt-get update

# Install PostgreSQL
run_cmd apt-get install -y postgresql-${POSTGRES_VERSION} postgresql-client-${POSTGRES_VERSION}

# Step 2: Configure PostgreSQL
echo "Step 2: Configuring PostgreSQL..."

# Start PostgreSQL service
run_cmd service postgresql start

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL to start..."
MAX_ATTEMPTS=30
ATTEMPT=1
while ! pg_isready -h localhost -p 5432 &>/dev/null; do
    if [ $ATTEMPT -ge $MAX_ATTEMPTS ]; then
        echo "❌ PostgreSQL failed to start after $MAX_ATTEMPTS attempts"
        # Check the actual status
        run_cmd service postgresql status
        exit 1
    fi
    echo "Attempt $ATTEMPT/$MAX_ATTEMPTS: Waiting for PostgreSQL to be ready..."
    sleep 2
    ATTEMPT=$((ATTEMPT + 1))
done

echo "✅ PostgreSQL is ready!"

# Step 3: Create metastore database and user
echo "Step 3: Creating metastore database and user..."
echo "Using locale settings: LC_COLLATE='$LOCALE_COLLATE', LC_CTYPE='$LOCALE_CTYPE'"

# Construct the CREATE DATABASE command with proper variable substitution
CREATE_DB_CMD="CREATE DATABASE metastore WITH ENCODING='UTF8' LC_COLLATE='${LOCALE_COLLATE}' LC_CTYPE='${LOCALE_CTYPE}' TEMPLATE=template0;"
echo "Database creation command: $CREATE_DB_CMD"

sudo -u postgres psql -c "DROP DATABASE IF EXISTS metastore;" || echo "Database metastore does not exist, skipping drop"
sudo -u postgres psql -c "$CREATE_DB_CMD"
sudo -u postgres psql -c "DROP USER IF EXISTS hive;" || echo "User hive does not exist, skipping drop"
sudo -u postgres psql -c "CREATE USER hive WITH PASSWORD 'hivepassword';"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE metastore TO hive;"
sudo -u postgres psql -c "ALTER DATABASE metastore OWNER TO hive;"

# Configure PostgreSQL for local connections
echo "Configuring PostgreSQL authentication..."
PG_VERSION_SHORT=$(echo $POSTGRES_VERSION | cut -d. -f1)
PG_CONFIG_DIR="/etc/postgresql/${POSTGRES_VERSION}/main"

# Update postgresql.conf to listen on localhost
run_cmd sed -i "s/#listen_addresses = 'localhost'/listen_addresses = 'localhost'/" ${PG_CONFIG_DIR}/postgresql.conf

# Update pg_hba.conf for password authentication
# Add entries for hive user before the default entries
run_cmd sh -c "cat > ${PG_CONFIG_DIR}/pg_hba.conf.new << EOF
# TYPE  DATABASE        USER            ADDRESS                 METHOD
# Database administrative login by Unix domain socket
local   all             postgres                                peer

# Hive metastore access
local   metastore       hive                                    md5
host    metastore       hive            127.0.0.1/32            md5
host    metastore       hive            ::1/128                 md5

# \"local\" is for Unix domain socket connections only
local   all             all                                     peer
# IPv4 local connections:
host    all             all             127.0.0.1/32            scram-sha-256
# IPv6 local connections:
host    all             all             ::1/128                 scram-sha-256
EOF"

run_cmd mv ${PG_CONFIG_DIR}/pg_hba.conf ${PG_CONFIG_DIR}/pg_hba.conf.bak
run_cmd mv ${PG_CONFIG_DIR}/pg_hba.conf.new ${PG_CONFIG_DIR}/pg_hba.conf

# Restart PostgreSQL to apply changes
echo "Restarting PostgreSQL..."
run_cmd service postgresql restart

# Wait for PostgreSQL to be ready again
sleep 5

# Test connection
echo "Testing PostgreSQL connection..."
PGPASSWORD=hivepassword psql -h localhost -U hive -d metastore -c "SELECT version();" && echo "✅ PostgreSQL connection successful" || echo "❌ PostgreSQL connection failed"

# Verify database encoding and locale settings
echo "Verifying database encoding and locale settings..."
PGPASSWORD=hivepassword psql -h localhost -U hive -d metastore -c "SELECT 
    current_setting('server_encoding') as encoding,
    current_setting('lc_collate') as lc_collate,
    current_setting('lc_ctype') as lc_ctype;" 2>/dev/null || echo "Could not verify database settings"

# Step 4: Initialize Hive metastore schema
echo "Step 4: Initializing Hive metastore schema..."
SCHEMA_FILE="./scripts/dev_container_scripts/spark_minimal/hive-schema-4.0.0.postgres.sql"

if [ -f "$SCHEMA_FILE" ]; then
    echo "Found PostgreSQL schema file: $SCHEMA_FILE"
    PGPASSWORD=hivepassword psql -h localhost -U hive -d metastore -f "$SCHEMA_FILE"
    
    # Verify schema was created
    TABLES_COUNT=$(PGPASSWORD=hivepassword psql -h localhost -U hive -d metastore -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE';" | xargs)
    echo "Created $TABLES_COUNT metastore tables"
    
    if [ "$TABLES_COUNT" -gt 0 ]; then
        echo "✅ Hive metastore schema initialized successfully"
    else
        echo "❌ Hive metastore schema initialization failed"
        exit 1
    fi
else
    echo "⚠️  PostgreSQL schema file not found at $SCHEMA_FILE"
    echo "Metastore tables will be created automatically on first use"
fi

# Step 5: Download PostgreSQL JDBC driver
echo "Step 5: Downloading PostgreSQL JDBC driver..."
POSTGRES_JDBC_VERSION="42.7.3"
POSTGRES_JDBC_URL="https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar"

if [ -n "$SPARK_HOME" ]; then
    echo "Downloading PostgreSQL JDBC driver to $SPARK_HOME/jars/"
    run_cmd mkdir -p $SPARK_HOME/jars
    run_cmd rm -f $SPARK_HOME/jars/postgresql-*.jar
    wget -q "$POSTGRES_JDBC_URL" -O $SPARK_HOME/jars/postgresql-${POSTGRES_JDBC_VERSION}.jar
    echo "✅ PostgreSQL JDBC driver downloaded"
else
    echo "⚠️  SPARK_HOME not set. Please download PostgreSQL JDBC driver manually:"
    echo "   $POSTGRES_JDBC_URL"
fi

# Step 6: Configure Spark for PostgreSQL
echo "Step 6: Configuring Spark for PostgreSQL metastore..."

if [ -n "$SPARK_HOME" ]; then
    run_cmd mkdir -p $SPARK_HOME/conf
    
    # Create spark-defaults.conf
    run_cmd tee $SPARK_HOME/conf/spark-defaults.conf > /dev/null << 'EOF'
# PostgreSQL Metastore Configuration
spark.sql.catalogImplementation=hive
spark.hadoop.javax.jdo.option.ConnectionURL=jdbc:postgresql://localhost:5432/metastore
spark.hadoop.javax.jdo.option.ConnectionDriverName=org.postgresql.Driver
spark.hadoop.javax.jdo.option.ConnectionUserName=hive
spark.hadoop.javax.jdo.option.ConnectionPassword=hivepassword

# Schema management
spark.hadoop.datanucleus.schema.autoCreateAll=true
spark.hadoop.hive.metastore.schema.verification=false
spark.hadoop.datanucleus.autoCreateSchema=true
spark.hadoop.datanucleus.fixedDatastore=true

# Connection pooling
spark.hadoop.datanucleus.connectionPool.maxActive=10
spark.hadoop.datanucleus.connectionPool.maxIdle=5
spark.hadoop.datanucleus.connectionPool.minIdle=1

# Warehouse configuration
spark.sql.warehouse.dir=/tmp/spark-warehouse

# Performance settings
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
spark.serializer=org.apache.spark.serializer.KryoSerializer
EOF

    # Create hive-site.xml
    run_cmd tee $SPARK_HOME/conf/hive-site.xml > /dev/null << 'EOF'
<?xml version="1.0"?>
<configuration>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:postgresql://localhost:5432/metastore</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>org.postgresql.Driver</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionUserName</name>
    <value>hive</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionPassword</name>
    <value>hivepassword</value>
  </property>
  <property>
    <name>datanucleus.schema.autoCreateAll</name>
    <value>true</value>
  </property>
  <property>
    <name>hive.metastore.schema.verification</name>
    <value>false</value>
  </property>
  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>/tmp/spark-warehouse</value>
  </property>
</configuration>
EOF

    echo "✅ Spark configuration files created"
else
    echo "⚠️  SPARK_HOME not set. Please configure Spark manually."
fi

# Create warehouse directory
run_cmd mkdir -p /tmp/spark-warehouse
run_cmd chmod 777 /tmp/spark-warehouse

echo ""
echo "=== PostgreSQL Metastore Setup Complete ==="
echo "Database: metastore on localhost:5432"
echo "User: hive / Password: hivepassword"
echo "Warehouse location: /tmp/spark-warehouse"
echo ""
echo "To test in Spark shell:"
echo "  spark-shell"
echo "  spark.sql(\"CREATE DATABASE IF NOT EXISTS test\")"
echo "  spark.sql(\"SHOW DATABASES\").show()"
echo "  spark.sql(\"SHOW TABLES\").show()"
echo ""

# Create a test script
cat > /tmp/test_postgres_metastore.scala << 'EOF'
// Test PostgreSQL Metastore
println("Testing PostgreSQL Metastore...")

// Show databases
println("\n=== Databases ===")
spark.sql("SHOW DATABASES").show()

// Create a test database
spark.sql("CREATE DATABASE IF NOT EXISTS test_db")

// Use the test database
spark.sql("USE test_db")

// Create a test table
spark.sql("CREATE TABLE IF NOT EXISTS test_table (id INT, name STRING) USING PARQUET")

// Insert some data
spark.sql("INSERT INTO test_table VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")

// Show tables
println("\n=== Tables in test_db ===")
spark.sql("SHOW TABLES").show()

// Query the table
println("\n=== Data in test_table ===")
spark.sql("SELECT * FROM test_table").show()

// Show table details
println("\n=== Table Details ===")
spark.sql("DESCRIBE EXTENDED test_table").show(100, false)

println("\n✅ PostgreSQL metastore test completed successfully!")
EOF

echo "Test script created at: /tmp/test_postgres_metastore.scala"
echo "Run it with: spark-shell -i /tmp/test_postgres_metastore.scala"