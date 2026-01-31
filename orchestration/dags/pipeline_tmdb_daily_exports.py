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
        # Reemplaza [usuario]/[repo] con tu usuario y nombre de repositorio
        image='ghcr.io/miguelangelmutti/etl_tmdb_orquestado/ingesta:latest',
        api_version='auto',
        auto_remove=True,
        # El comando ahora es simplemente la ejecución del script, 
        # las dependencias ya están en la imagen.
        command="python ingestion/dlt_export_test.py",
        docker_url='unix://var/run/docker.sock',
        network_mode='etl-network',
        mounts=[
            # IMPORTANTE: Ya no montamos todo el código (/app <- HOST_PROJECT_PATH).
            # Solo montamos los directorios donde persisten datos (DB, exports, logs).
            Mount(
                source=f"{HOST_PROJECT_PATH}/database",
                target="/app/database",
                type="bind"
            ),
            Mount(
                source=f"{HOST_PROJECT_PATH}/daily_exports",
                target="/app/daily_exports",
                type="bind"
            ),
             Mount(
                source=f"{HOST_PROJECT_PATH}/ingestion/.dlt", # Para persistir estado de dlt si es necesario
                target="/app/ingestion/.dlt",
                type="bind"
            ),
             Mount(
                source=f"{HOST_PROJECT_PATH}/logs",
                target="/app/logs",
                type="bind"
            )
        ],
        environment={
            "TOKEN": os.getenv("TOKEN"),
            "API_KEY": os.getenv("API_KEY"),
            #"TMDB_ACCESS_TOKEN": os.getenv("TOKEN") # For dlt native resolution
        }
    )

    ingestion_changes_command = (        
        "python ingestion/dlthub_api_consumer.py "
        "{% if params.fecha_inicio %}--fecha_inicio {{ params.fecha_inicio }}{% endif %} "
        "{% if params.fecha_fin %}--fecha_fin {{ params.fecha_fin }}{% endif %} "
    )

    ingestar_cambios_api = DockerOperator(
        task_id='ingestar_cambios_api',
        image='ghcr.io/miguelangelmutti/etl_tmdb_orquestado/ingesta:latest',
        api_version='auto',
        auto_remove=True,
        # 1. Install dependencies from the mounted requirements file
        # 2. Run the ingestion script
        command=ingestion_changes_command,
        docker_url='unix://var/run/docker.sock',
        network_mode='etl-network',  # Connect to the same network as other services
        mounts=[
            # IMPORTANTE: Ya no montamos todo el código (/app <- HOST_PROJECT_PATH).
            # Solo montamos los directorios donde persisten datos (DB, exports, logs).
            Mount(
                source=f"{HOST_PROJECT_PATH}/database",
                target="/app/database",
                type="bind"
            ),
            Mount(
                source=f"{HOST_PROJECT_PATH}/daily_exports",
                target="/app/daily_exports",
                type="bind"
            ),
             Mount(
                source=f"{HOST_PROJECT_PATH}/ingestion/.dlt", # Para persistir estado de dlt si es necesario
                target="/app/ingestion/.dlt",
                type="bind"
            ),
             Mount(
                source=f"{HOST_PROJECT_PATH}/logs",
                target="/app/logs",
                type="bind"
            )
        ],
        working_dir="/app",
        environment={
            "TOKEN": os.getenv("TOKEN"),     # Pass TMDB Token if needed explicitly
            "API_KEY": os.getenv("API_KEY"),  # Pass API Key if needed explicitly
            "TMDB_ACCESS_TOKEN": os.getenv("TOKEN") # For dlt native resolution
        }
    )    

    transformar_daily_exports = DockerOperator(
        task_id='transformar_daily_exports',
        image='ghcr.io/miguelangelmutti/etl_tmdb_orquestado/transformacion:latest',
        api_version='auto',
        auto_remove=True,
        command="dbt build",
        docker_url='unix://var/run/docker.sock',
        network_mode='etl-network',
        mounts=[
            Mount(
                source=f"{HOST_PROJECT_PATH}/database",
                target="/app/database",
                type="bind"
            )
        ],
        environment={
             # Pasar variables necesarias para dbt/profiles si las hubiera
            "TOKEN": os.getenv("TOKEN"),
            "API_KEY": os.getenv("API_KEY")
        }
    )

    ingestar_daily_exports >> ingestar_cambios_api >> transformar_daily_exports
