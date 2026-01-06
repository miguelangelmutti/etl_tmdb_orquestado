import dlt
import gzip
from datetime import datetime
from dlt.sources.helpers import requests as dlt_requests
from requests.exceptions import HTTPError
from dlt.destinations import duckdb
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config import MOVIE_URL, DAILY_EXPORT_BASE_URL, TEST_DAILY_EXPORT_BASE_URL, DB_PATH, BASE_DIR, LOG_FILE, MOVIE_URL,DLT_SCHEMA_PATH
from utils.logger import setup_logger

# 1. EL RECURSO PADRE (El archivo comprimido)
# Este recurso no escribe en la BD (selected=False) porque solo sirve de "alimentador"
# para el siguiente paso.
@dlt.resource(selected=False) 
def tmdb_daily_ids_stream(entity_type="movie"):
    date_str = datetime.now().strftime("%m_%d_%Y")
    url = TEST_DAILY_EXPORT_BASE_URL
    
    print(f"--- Iniciando Stream desde: {url} ---")
    response = dlt_requests.get(url, stream=True)
    
    with gzip.open(response.raw, mode='rb') as f:
        for line in f:
            if line:
                yield dlt.common.json.loads(line.decode("utf-8"))                

# 2. EL TRANSFORMADOR (La API en Paralelo)
# data_from=tmdb_daily_ids_stream vincula este paso con el anterior
@dlt.transformer(
    data_from=tmdb_daily_ids_stream, 
    write_disposition="append", 
    primary_key="id",
    table_name="tmdb_movies"
)
def fetch_movie_details(record, api_key=dlt.secrets["tmdb_access_token"]):
    # ESTRATEGIA DE FILTRADO (CRÍTICO)
    # El archivo diario tiene +800k películas. Consultar la API para todas tomaría días.
    # Filtramos para procesar solo las que tienen cierta popularidad.
    #popularity = record.get("popularity", 0)
    
    # Ejemplo: Solo enriquecer si la popularidad es mayor a 50
    #if popularity < 50:
        #return  # Saltamos este registro (no se guarda nada)

    movie_id = record["id"]
    
    # Construcción de la request
    # Construcción de la request
    # MOVIE_URL termina en /, así que concatenamos directamente
    url = f"{MOVIE_URL}{movie_id}?append_to_response=credits"
    params = {
        "api_key": api_key       
    }

    # dlt maneja los reintentos si la API devuelve 429
    try:
        response = dlt_requests.get(url, params=params)
        response.raise_for_status() # Aseguramos que se lance excepción si no es 200 OK
        
        # Devolvemos el JSON completo de la película enriquecida
        yield response.json()     

    except HTTPError as e:
        if e.response.status_code == 404:
            print(f"Movie ID {movie_id} not found (404). Skipping.")
            return # Saltamos este registro silenciosamente
        else:
            print(f"Error fetching ID {movie_id}: {e}")
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
pipeline.run(tmdb_daily_ids_stream | fetch_movie_details)
logger.info("Pipeline de primer ingesta completado")
