#!/bin/bash
set -e

# Usage: ./entrypoint.sh <path_to_python_script>

# 1. Setup configuration files
echo "Setting up configuration files..."
mkdir -p ingestion/.dlt
mkdir -p .dlt # Also create .dlt in root for dlt discovery

if [ -f ingestion/.dlt/config.toml.example ]; then
  cp ingestion/.dlt/config.toml.example ingestion/.dlt/config.toml
  cp ingestion/.dlt/config.toml.example .dlt/config.toml # Copy to root .dlt
fi
if [ -f ingestion/.dlt/secrets.toml.example ]; then
  cp ingestion/.dlt/secrets.toml.example ingestion/.dlt/secrets.toml
  cp ingestion/.dlt/secrets.toml.example .dlt/secrets.toml # Copy to root .dlt
fi

# 2. Inject Secrets
echo "Injecting secrets..."
# Update both locations
python -c "import os; content = open('ingestion/.dlt/secrets.toml').read().replace('your_tmdb_api_key_here', os.environ.get('TOKEN', '')); open('ingestion/.dlt/secrets.toml', 'w').write(content)"
python -c "import os; content = open('.dlt/secrets.toml').read().replace('your_tmdb_api_key_here', os.environ.get('TOKEN', '')); open('.dlt/secrets.toml', 'w').write(content)"

# 3. Exec arguments
echo "Running command: $@"
echo "Current directory: $(pwd)"
echo "Listing files in /app:"
ls -la /app
exec "$@"
