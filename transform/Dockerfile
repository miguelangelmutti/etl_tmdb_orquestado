FROM python:3.11-slim

WORKDIR /app
ENV PYTHONPATH=/app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY transform/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy config.py to the root of the app
COPY config.py .

# Copy transformation code
COPY transform/ transform/

# Create database directory for duckdb
RUN mkdir -p /app/database

# Ensure entrypoint is executable
RUN chmod +x transform/entrypoint.sh

# Entrypoint
ENTRYPOINT ["transform/entrypoint.sh"]

# Default command
CMD ["dbt", "--version"]
