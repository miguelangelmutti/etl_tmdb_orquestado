#!/bin/bash
set -e

# Usage: ./entrypoint.sh <path_to_python_script>

# 1. Setup configuration files
echo "Setting up configuration files..."
if [ -f ingestion/.dlt/config.toml.example ]; then
  cp ingestion/.dlt/config.toml.example ingestion/.dlt/config.toml
fi
if [ -f ingestion/.dlt/secrets.toml.example ]; then
  cp ingestion/.dlt/secrets.toml.example ingestion/.dlt/secrets.toml
fi

# 2. Inject Secrets
echo "Injecting secrets..."
python -c "import os; content = open('ingestion/.dlt/secrets.toml').read().replace('your_tmdb_api_key_here', os.environ.get('TOKEN', '')); open('ingestion/.dlt/secrets.toml', 'w').write(content)"

# 3. Exec arguments
echo "Running command: $@"
exec "$@"
