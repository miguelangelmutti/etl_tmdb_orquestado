FROM python:3.11-slim

WORKDIR /app
ENV PYTHONPATH=/app

# Install system dependencies if any (none strictly required for basic dlt/duckdb but good practice)
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for cache optimization
COPY ingestion/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy config.py to the root of the app
COPY config.py .

# Copy utils package
COPY utils/ utils/

# Create logs directory
RUN mkdir -p logs

# Copy the rest of the application
COPY ingestion/ ingestion/

# Ensure entrypoint is executable
RUN chmod +x ingestion/entrypoint.sh

# Entrypoint
ENTRYPOINT ["ingestion/entrypoint.sh"]

# Default command if none provided (optional, but good for testing)
CMD ["python", "--version"]
