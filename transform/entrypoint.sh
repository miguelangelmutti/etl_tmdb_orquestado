#!/bin/bash
set -e

echo "Starting transformation setup..."

# 1. Setup dbt profile
echo "Generating profiles.yml..."
mkdir -p ~/.dbt
echo "movies_profile:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: /app/database/shared_movies.duckdb" > ~/.dbt/profiles.yml

# 2. Exec arguments
echo "Running command: $@"
cd transform
exec "$@"
