import dlt
import gzip
from datetime import datetime
from dlt.sources.helpers import requests as dlt_requests
from dlt.destinations import duckdb
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config import MOVIE_URL, DAILY_EXPORT_BASE_URL, TEST_DAILY_EXPORT_BASE_URL, DATABASE_FOLDER, DATABASE_NAME, BASE_DIR, LOG_FILE
from utils.logger import setup_logger

setup_logger(LOG_FILE)

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
    write_disposition="merge", 
    primary_key="id",
    table_name="movies_enriched"
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
    url = f"https://api.themoviedb.org/3/movie/{movie_id}"
    params = {
        "api_key": api_key,
        "language": "es-ES" # Opcional: traer datos en español
    }

    # dlt maneja los reintentos si la API devuelve 429
    response = dlt_requests.get(url, params=params)
    
    if response.status_code == 200:
        # Devolvemos el JSON completo de la película enriquecida
        yield response.json()     
    else:
        # Opcional: Manejo de errores suaves (soft errors)
        print(f"Error fetching ID {movie_id}: {response.status_code}")

# 3. EJECUCIÓN DEL PIPELINE CON PARALELISMO
# Construimos la ruta absoluta a la DB para evitar errores por CWD
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DB_PATH = os.path.join(BASE_DIR, DATABASE_FOLDER, DATABASE_NAME)

pipeline = dlt.pipeline(
    pipeline_name="tmdb_enrichment_pipeline",
    destination=duckdb(DB_PATH), 
    dataset_name="tmdb_data"
)

# Aquí es donde ocurre la MAGIA de la concurrencia.
# El pipe (|) conecta los recursos.
# workers=16 lanzará 16 peticiones simultáneas a la API.
logger = setup_logger(
    __name__, 
    log_file=LOG_FILE, 
    capture_external_loggers=["dlt"]  # Captura logs de dlt
    )
logger.info("Iniciando pipeline de primer ingesta")
pipeline.run(tmdb_daily_ids_stream | fetch_movie_details)
logger.info("Pipeline de primer ingesta completado")

