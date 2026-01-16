import os
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime

# Retrieve the host path from the environment variable set in docker-compose
HOST_PROJECT_PATH = os.getenv("HOST_PROJECT_PATH")

with DAG(
    'dag_tmdb_daily_exports',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    ingestar_daily_exports = DockerOperator(
        task_id='ingestar_daily_exports',
        image='python:3.11-slim',
        api_version='auto',
        auto_remove=True,
        # 1. Install dependencies from the mounted requirements file
        # 2. Run the ingestion script
        command="/bin/bash -c 'pip install -r ingestion/requirements.txt && python ingestion/dlt_export_test.py'",
        docker_url='unix://var/run/docker.sock',
        network_mode='etl-network',  # Connect to the same network as other services
        mounts=[
            Mount(
                source=HOST_PROJECT_PATH,  # Absolute path on the HOST machine
                target="/app",             # Path inside the container
                type="bind"
            )
        ],
        working_dir="/app",
        environment={
            "TOKEN": os.getenv("TOKEN"),     # Pass TMDB Token if needed explicitly
            "API_KEY": os.getenv("API_KEY")  # Pass API Key if needed explicitly
        }
    )

    transformar_daily_exports = DockerOperator(
        task_id='transformar_daily_exports',
        image='python:3.11-slim',
        api_version='auto',
        auto_remove=True,
        # 1. Generate dynamic profiles.yml
        # 2. Install dependencies
        # 3. Run dbt build using the generated profile
        command="""/bin/bash -c 'mkdir -p ~/.dbt && echo "movies_profile:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: /app/database/shared_movies.duckdb" > ~/.dbt/profiles.yml && pip install -r transform/requirements.txt && cd transform && dbt build --profiles-dir ~/.dbt'""",
        docker_url='unix://var/run/docker.sock',
        network_mode='etl-network',
        mounts=[
            Mount(
                source=HOST_PROJECT_PATH,
                target="/app",
                type="bind"
            )
        ],
        working_dir="/app",
        environment={
            "TOKEN": os.getenv("TOKEN"),
            "API_KEY": os.getenv("API_KEY")
        }
    )

    ingestar_daily_exports >> transformar_daily_exports
