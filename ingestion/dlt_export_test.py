import dlt
import gzip
from datetime import datetime
from dlt.sources.helpers import requests as dlt_requests
from requests.exceptions import HTTPError
from dlt.destinations import duckdb
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config import MOVIE_URL, DAILY_EXPORT_BASE_URL, TEST_DAILY_EXPORT_BASE_URL, DB_PATH, BASE_DIR, LOG_FILE, MOVIE_URL,DLT_SCHEMA_PATH, DAILY_EXPORT_BASE_URL, PERSON_URL
from utils.logger import setup_logger

# 1. EL RECURSO PADRE (El archivo comprimido)
# Este recurso no escribe en la BD (selected=False) porque solo sirve de "alimentador"
# para el siguiente paso.
@dlt.resource(selected=False) 
def tmdb_daily_ids_stream(entity="movie"):
    date_str = datetime.now().strftime("%m_%d_%Y")
    
    daily_export_url = DAILY_EXPORT_BASE_URL.format(entity, date_str)

    print(f"--- Iniciando Stream desde: {daily_export_url} ---")
    response = dlt_requests.get(daily_export_url, stream=True)
    response.raise_for_status()
    
    with gzip.open(response.raw, mode='rb') as f:
        for line in f:
            if line:
                record = dlt.common.json.loads(line.decode("utf-8"))
                # Inyectamos el tipo de entidad en el registro para que el transformador lo sepa
                record["_entity_type"] = entity
                yield record

# 2. EL TRANSFORMADOR (La API en Paralelo)
# data_from=tmdb_daily_ids_stream vincula este paso con el anterior
@dlt.transformer(
    data_from=tmdb_daily_ids_stream, 
    write_disposition="append", 
    primary_key="id",
    table_name="tmdb_movies"
)
def fetch_tmdb_details(record, api_key=dlt.secrets["tmdb_access_token"]):
    # Recuperamos el entity inyectado por el recurso padre
    entity_type = record.get("_entity_type", "movie")
    
    entity_id = record["id"]
    
    # Construcción de la request
    if entity_type == "movie":
        url = f"{MOVIE_URL}{entity_id}?append_to_response=credits"
    elif entity_type == "person":
        url = f"{PERSON_URL}{entity_id}"
    else:
        raise ValueError(f"Entity type {entity_type} not supported")
    
    params = {
        "api_key": api_key       
    }

    # dlt maneja los reintentos si la API devuelve 429
    try:
        response = dlt_requests.get(url, params=params)
        response.raise_for_status() # Aseguramos que se lance excepción si no es 200 OK
        
        # Devolvemos el JSON completo de la película enriquecida
        data = response.json()
        # Opcional: Limpiamos el campo auxiliar si no queremos que vaya a la BD final
        # (Aunque dlt suele manejar bien campos extra, a veces es mejor quitarlos si son solo metadatos de pipeline)
        # record.pop("_entity_type", None) 
        
        yield data

    except HTTPError as e:
        if e.response.status_code == 404:
            print(f"Entity ID {entity_id} not found ({url}). Skipping.")
            return # Saltamos este registro silenciosamente
        else:
            print(f"Error fetching ID {entity_id}: {e}")
            raise e # Relanzamos otros errores para que dlt los maneje (ej. 500)

# 3. EJECUCIÓN DEL PIPELINE CON PARALELISMO
# Utiliza DB_PATH definido en config.py (Fuente única de verdad)

pipeline = dlt.pipeline(
    pipeline_name="tmdb_get_all_movies_pipeline",
    destination=duckdb(DB_PATH), 
    dataset_name="raw_movies",
    import_schema_path=DLT_SCHEMA_PATH+"/import",
    export_schema_path=DLT_SCHEMA_PATH+"/export"
)

# Aquí es donde ocurre la MAGIA de la concurrencia.
# El pipe (|) conecta los recursos.

logger = setup_logger(
            __name__, 
            log_file=LOG_FILE, 
            capture_external_loggers=["dlt"]  # Captura logs de dlt
            )
logger.info("Iniciando pipeline de primer ingesta")

# Ejemplo de uso pasando entity explícito. 
#pipeline.run(tmdb_daily_ids_stream(entity="movie") | fetch_tmdb_details)
pipeline.run(tmdb_daily_ids_stream(entity="person") | fetch_tmdb_details)

logger.info("Pipeline de primer ingesta completado")
