import os
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime

# Retrieve the host path from the environment variable set in docker-compose
HOST_PROJECT_PATH = os.getenv("HOST_PROJECT_PATH")

with DAG(
    'dag_tmdb_changes',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    ingestar_cambios_api = DockerOperator(
        task_id='ingestar_cambios_api',
        image='python:3.11-slim',
        api_version='auto',
        auto_remove=True,
        # 1. Install dependencies from the mounted requirements file
        # 2. Run the ingestion script
        command="/bin/bash /app/ingestion/entrypoint.sh ingestion/dlthub_api_consumer.py ",
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

    transformar_cambios_db = DockerOperator(
        task_id='transformar_cambios_db',
        image='python:3.11-slim',
        api_version='auto',
        auto_remove=True,
        # 1. Install dependencies from the mounted requirements file
        # 2. Run the dbt build script
        command="/bin/bash /app/transform/entrypoint.sh ",
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

    ingestar_cambios_api >> transformar_cambios_db