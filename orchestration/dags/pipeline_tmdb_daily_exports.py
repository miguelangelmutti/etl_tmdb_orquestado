from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime

with DAG(
    'dag_tmdb_daily_exports',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    ejecutar_tarea_consumir_api = DockerOperator(
        task_id='tarea_consumir_api',
        image='python:3.11-slim', # O una imagen tuya personalizada
        api_version='auto',
        auto_remove=True,
        # Comando para clonar y ejecutar (si usas la Opción A)
        command="/bin/bash -c 'apt-get update && apt-get install -y git && \
                 git clone https://github.com/tu-usuario/tu-repo.git && \
                 cd tu-repo && pip install -r requirements.txt && \
                 python main.py'",
        docker_url='unix://var/run/docker.sock', # Conexión al socket montado
        network_mode='bridge'
    )