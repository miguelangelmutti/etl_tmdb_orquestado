# ETL Movies Pipeline

Este proyecto implementa un pipeline ELT (Extract, Load, Transform) completo para extraer datos de películas y personas desde la API de **TMDB (The Movie Database)**, cargarlos en una base de datos **DuckDB**, y transformarlos utilizando **dbt**. Todo el flujo es orquestado mediante **Apache Airflow** corriendo en Docker.

## 🏗 Arquitectura

El proyecto consta de tres componentes principales:

1.  **Ingestion (Extraer & Cargar)**:
    *   Utiliza la librería **`dlt`** (Data Load Tool) para consumir la API de TMDB.
    *   Maneja la descarga de "Daily Exports" de TMDB (archivos JSON comprimidos) para una carga inicial masiva eficiente.
    *   Carga los datos en bruto (`raw`) en una base de datos **DuckDB** local.

2.  **Transformation (Transformar)**:
    *   Utiliza **`dbt`** (Data Build Tool) para modelar y limpiar los datos crudos.
    *   Los modelos SQL definen la estructura del Data Warehouse analítico final dentro de DuckDB.

3.  **Orchestration (Orquestar)**:
    *   **Apache Airflow** gestiona las dependencias y la programación de tareas.
    *   Se ejecuta en contenedores **Docker** para asegurar un entorno reproducible.
    *   Los DAGs de Airflow disparan los scripts de carga (`dlt`) y transformación (`dbt`).

## 📋 Prerrequisitos

*   [Docker Desktop](https://www.docker.com/products/docker-desktop/) instalado y corriendo.
*   Una cuenta en [TMDB](https://www.themoviedb.org/) y una **API Key** o **Access Token**.

## ⚙️ Configuración

1.  **Clonar el repositorio**:
    ```bash
    git clone <url-del-repositorio>
    cd etl-movies
    ```

2.  **Configurar variables de entorno**:
    Crea un archivo `.env` en la raíz del proyecto basándote en `.env.example`.
    
    ```bash
    cp .env.example .env
    ```

    Edita el archivo `.env` y define las siguientes variables:

    ```ini
    # Claves de TMDB
    API_KEY=tu_api_key_de_tmdb
    TOKEN=tu_read_access_token_de_tmdb

    # Ruta absoluta al proyecto en tu máquina HOST (necesario para montar volúmenes en Docker)
    # En Windows ejemplo: /mnt/g/etl-movies o G:\etl-movies dependiendo de tu terminal
    HOST_PROJECT_PATH=G:\etl-movies
    ```

## 🚀 Ejecución

La forma recomendada de ejecutar el pipeline es a través de Docker y Airflow.

1.  **Iniciar los servicios**:
    Desde la carpeta `orchestration`, levanta los contenedores con Docker Compose:

    ```bash
    cd orchestration
    docker-compose up -d
    ```

2.  **Acceder a Airflow**:
    *   Abre tu navegador y ve a `http://localhost:8080`.
    *   Credenciales por defecto (definidas en `docker-compose.yml`):
        *   Usuario: `airflow`
        *   Contraseña: `airflow`

3.  **Ejecutar un DAG**:
    *   Busca el DAG `dag_tmdb_daily_exports` en la interfaz de Airflow.
    *   Actívalo (toggle ON) y haz clic en el botón "Trigger DAG" (play) para iniciar una ejecución manual.
    *   Este DAG ejecutará secuencialmente:
        1.  Ingesta de datos (descarga backup de ayer de TMDB).
        2.  Transformación con dbt.

## 📂 Estructura del Proyecto

*   `ingestion/`: Scripts de Python usando `dlt` para extraer datos.
*   `transform/`: Proyecto `dbt` con modelos SQL y tests.
*   `orchestration/`: Configuración de Airflow (DAGs y `docker-compose.yml`).
*   `daily_exports/`: Directorio temporal donde se descargan los archivos JSON de TMDB.
*   `database/`: Contiene la base de datos `shared_movies.duckdb`.

## 🛠 Desarrollo Local (Opcional)

Si deseas ejecutar los scripts de python o dbt fuera de Docker (en tu máquina local):

1.  Crea un entorno virtual:
    ```bash
    python -m venv venv
    source venv/bin/activate  # En Windows: venv\Scripts\activate
    ```
2.  Instala las dependencias:
    ```bash
    pip install -r ingestion/requirements.txt
    pip install -r transform/requirements.txt
    ```
3.  Ejecuta el script de ingestión:
    ```bash
    python ingestion/dlt_export_test.py
    ```
