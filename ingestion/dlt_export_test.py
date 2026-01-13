import dlt
import gzip
from datetime import datetime,timedelta
from dlt.sources.helpers import requests as dlt_requests
from requests.exceptions import HTTPError
from dlt.destinations import duckdb
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config import DAILY_EXPORT_BASE_URL, DB_PATH, BASE_DIR, LOG_FILE, MOVIE_URL,DLT_SCHEMA_PATH, PERSON_URL, DAILY_EXPORTS_DIR
from utils.logger import setup_logger

def download_file(url, save_path):
    """Descarga un archivo si no existe, en chunks."""
    if os.path.exists(save_path):
        print(f"Archivo ya existe: {save_path}")
        return

    print(f"Descargando {url} -> {save_path} ...")
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    
    try:
        response = dlt_requests.get(url, stream=True)
        response.raise_for_status()
        
        with open(save_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        print("Descarga completada.")
    except Exception as e:
        # Si falla, borramos el archivo parcial para no corromper futuras ejecuciones
        if os.path.exists(save_path):
            os.remove(save_path)
        raise e

# 1. EL RECURSO PADRE (El archivo comprimido)
# Este recurso no escribe en la BD (selected=False) porque solo sirve de "alimentador"
# para el siguiente paso.
@dlt.resource(selected=False) 
def tmdb_daily_ids_stream(entity="movie", limit=None):
    date_str = (datetime.now() - timedelta(days=1)).strftime("%m_%d_%Y")
    
    # 1. Construir URL y nombre de archivo local
    # DAILY_EXPORT_BASE_URL tiene placeholders {} or {entity}
    try:
        # Intento de formateo posicional (el más probable según config actual)
        daily_export_url = DAILY_EXPORT_BASE_URL.format(entity, date_str)
    except IndexError:
        # Si falla, intentamos con keys por si acaso (aunque config actual es {})
         daily_export_url = DAILY_EXPORT_BASE_URL.format(entity=entity, date_str=date_str)

    filename = f"{entity}_ids_{date_str}.json.gz"

    local_path = DAILY_EXPORTS_DIR / filename

    # 2. Descargar si no existe
    download_file(daily_export_url, local_path)

    # 3. Leer archivo local
    logger.info(f"--- Procesando archivo local: {local_path} ---")
    
    with gzip.open(local_path, mode='rb') as f:
        for i, line in enumerate(f):
            if limit is not None and i >= limit:
                logger.info(f"Límite de {limit} registros alcanzado. Deteniendo lectura.")
                break
                
            if line:
                record = dlt.common.json.loads(line.decode("utf-8"))
                # Inyectamos el tipo de entidad en el registro para que el transformador lo sepa
                record["_entity_type"] = entity
                logger.info(f"Procesando registro: {record}")
                yield record

def get_table_name(record):
    """Determina el nombre de la tabla destino basado en el tipo de entidad."""
    entity = record.get("_entity_type")
    logger.warning(f"Entity type: {entity}")
    return f"tmdb_{entity}s"

# 2. EL TRANSFORMADOR (La API en Paralelo)
# data_from=tmdb_daily_ids_stream vincula este paso con el anterior
@dlt.transformer(
    data_from=tmdb_daily_ids_stream, 
    write_disposition="append", 
    primary_key="id",
    table_name=get_table_name
)
def fetch_tmdb_details(record, api_key=dlt.secrets["tmdb_access_token"]):
    # Recuperamos el entity inyectado por el recurso padre
    entity_type = record.get("_entity_type")
       
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
        
        # IMPORTANTE: Inyectamos de nuevo el entity_type en el resultado final
        # para que get_table_name pueda determinar la tabla de destino correcta.
        data["_entity_type"] = entity_type
        
        logger.info(f"Procesando registro: {data.get('id')} - Entity: {entity_type}")  
        
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
# Activamos un limite para pruebas rapidas
pipeline.run(tmdb_daily_ids_stream(entity="movie", limit=50) | fetch_tmdb_details)
pipeline.run(tmdb_daily_ids_stream(entity="person", limit=50) | fetch_tmdb_details)

logger.info("Pipeline de primer ingesta completado")
