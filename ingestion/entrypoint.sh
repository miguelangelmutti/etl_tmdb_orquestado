#!/bin/bash
set -e

# Usage: ./entrypoint.sh <path_to_python_script>

SCRIPT_PATH=$1

if [ -z "$SCRIPT_PATH" ]; then
  echo "Error: No python script provided."
  exit 1
fi

echo "Starting ingestion setup..."

# 1. Setup configuration files
echo "Setting up configuration files..."
cp ingestion/.dlt/config.toml.example ingestion/.dlt/config.toml
cp ingestion/.dlt/secrets.toml.example ingestion/.dlt/secrets.toml

# 2. Inject Secrets
echo "Injecting secrets..."
python -c "import os; content = open('ingestion/.dlt/secrets.toml').read().replace('your_tmdb_api_key_here', os.environ.get('TOKEN')); open('ingestion/.dlt/secrets.toml', 'w').write(content)"

# 3. Install Dependencies
echo "Installing dependencies..."
pip install -r ingestion/requirements.txt

# 4. Run the Ingestion Script
echo "Running script: $SCRIPT_PATH"
python "$SCRIPT_PATH"
