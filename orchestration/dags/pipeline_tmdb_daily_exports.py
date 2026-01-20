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
    catchup=False,
    params={
        "fecha_inicio": None,
        "fecha_fin": None
    }
) as dag:

    ingestar_daily_exports = DockerOperator(
        task_id='ingestar_daily_exports',
        image='python:3.11-slim',
        api_version='auto',
        auto_remove=True,
        # 1. Install dependencies from the mounted requirements file
        # 2. Run the ingestion script
        command="/bin/bash /app/ingestion/entrypoint.sh ingestion/dlt_export_test.py ",
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

    ingestion_changes_command = (
        "/bin/bash /app/ingestion/entrypoint.sh "
        "ingestion/dlthub_api_consumer.py "
        "{% if params.fecha_inicio %}--fecha_inicio {{ params.fecha_inicio }}{% endif %} "
        "{% if params.fecha_fin %}--fecha_fin {{ params.fecha_fin }}{% endif %} "
    )

    ingestar_cambios_api = DockerOperator(
        task_id='ingestar_cambios_api',
        image='python:3.11-slim',
        api_version='auto',
        auto_remove=True,
        # 1. Install dependencies from the mounted requirements file
        # 2. Run the ingestion script
        command=ingestion_changes_command,
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
        command="/bin/bash /app/transform/entrypoint.sh ",
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

    ingestar_daily_exports >> ingestar_cambios_api >> transformar_daily_exports
