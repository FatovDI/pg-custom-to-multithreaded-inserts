#!/bin/bash

export PGPASSWORD="postgres"


# Database connection details
DB_HOST="localhost"
DB_PORT="5437"
DB_NAME="postgres"
DB_USER="postgres"

# Number of clients (threads)
NUM_CLIENTS=10

# Number of transactions per client
NUM_TRANSACTIONS=10

# Query file
QUERY_FILE="query_by_id.sql"
#QUERY_FILE="query_by_date.sql"

# Construct the pgbench command with custom query and parameters
function run_pgbench() {
  for (( i = 0; i < 5; i++ )); do
    pgbench -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "$NUM_CLIENTS" -j "$NUM_CLIENTS" -t "$NUM_TRANSACTIONS" -f "$QUERY_FILE"
  done
}

run_pgbench
