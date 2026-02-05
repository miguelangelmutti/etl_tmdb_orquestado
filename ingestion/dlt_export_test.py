import dlt
import gzip
import argparse
from datetime import datetime,timedelta
from dlt.sources.helpers import requests as dlt_requests
from requests.exceptions import HTTPError
from dlt.destinations import duckdb
import duckdb as duckdb_lib
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
# Ahora SI escribimos en la BD (selected=True por defecto) para persistir primero
# y luego ordenar con SQL.
@dlt.resource(write_disposition="replace") 
def tmdb_daily_ids_stream(entity="movie"):
    date_str = (datetime.now() - timedelta(days=1)).strftime("%m_%d_%Y")
    
    # 1. Construir URL y nombre de archivo local
    try:
        daily_export_url = DAILY_EXPORT_BASE_URL.format(entity, date_str)
    except IndexError:
         daily_export_url = DAILY_EXPORT_BASE_URL.format(entity=entity, date_str=date_str)

    filename = f"{entity}_ids_{date_str}.json.gz"
    local_path = DAILY_EXPORTS_DIR / filename

    # 2. Descargar si no existe
    download_file(daily_export_url, local_path)

    # 3. Leer archivo local y streamear a DuckDB sin ordenar en RAM
    logger.info(f"--- Procesando archivo local: {local_path} ---")
    
    # Definimos el nombre de la tabla raw explícitamente
    table_name = f"raw_tmdb_{entity}_ids"

    with gzip.open(local_path, mode='rb') as f:
        logger.info(f"Leyendo archivo e insertando en tabla {table_name}...")
        for line in f:
            if line:
                try:
                    record = dlt.common.json.loads(line.decode("utf-8"))
                    record["_entity_type"] = entity
                    # DLT enrutará esto a la tabla correspondiente si usamos la funcion dynamic o yield table hints
                    # Aquí lo hacemos simple: Usamos dlt.mark.with_table_name o confiamos en el recurso.
                    # Mejor: Yield el record directo y dejamos que el recurso defina la tabla dinámicamente o fija.
                    # Dado que el recurso se llama 'tmdb_daily_ids_stream', esa sería la tabla por defecto.
                    # Pero queremos tablas separadas por entidad.
                    yield dlt.mark.with_table_name(record, table_name)
                except Exception as e:
                    logger.warning(f"Error al leer linea json: {e}")

def get_sorted_ids(entity, limit=None):
    """Generador que lee de DuckDB ordenado por popularidad."""
    raw_table = f"raw_tmdb_{entity}_ids"
    # raw_movies es el dataset_name definido en el pipeline
    dataset = "raw_movies" 
    
    query = f"SELECT * FROM {dataset}.{raw_table} ORDER BY popularity DESC"
    if limit:
        query += f" LIMIT {limit}"
        
    logger.info(f"Ejecutando query en DuckDB: {query}")
    
    # Conectamos directo a DuckDB en modo lectura
    conn = duckdb_lib.connect(DB_PATH)
    try:
        # Ejecutamos y obtenemos resultados como dicts
        # fetchall() carga en memoria pero solo los IDs que necesitamos (limitados), 
        # o podemos iterar el cursor si es muy grande.
        # Si limit es None (todo el dataset), fetchmany es mejor.
        
        # Ojo: fetch_arrow_table() o fetch_df() podrian ser mas rapidos pero dlt espera dicts
        # Iterar el cursor es lo mas memory-safe.
        cursor = conn.execute(query)
        while True:
            # Traemos en batches para ser amigables con la RAM si no hay limite
            rows = cursor.fetchmany(1000)
            if not rows:
                break
            
            # Convertir tuplas a dicts. Necesitamos los nombres de columnas.
            columns = [desc[0] for desc in cursor.description]
            for row in rows:
                record = dict(zip(columns, row))
                # Asegurar que _entity_type esté presente (seguro lo guardamos en el paso 1)
                record["_entity_type"] = entity
                yield record
                
    except Exception as e:
        logger.error(f"Error consultando DuckDB: {e}")
        raise e
    finally:
        conn.close()

def get_table_name(record):
    """Determina el nombre de la tabla destino basado en el tipo de entidad."""
    entity = record.get("_entity_type")
    logger.warning(f"Entity type: {entity}")
    return f"tmdb_{entity}s"

# 2. EL TRANSFORMADOR (La API en Paralelo)
# data_from=tmdb_daily_ids_stream vincula este paso con el anterior
@dlt.transformer(
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
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run TMDB ingestion pipeline")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of records to process")
    args = parser.parse_args()

    logger.info("Iniciando pipeline de primer ingesta")

    # Ejemplo de uso pasando entity explícito. 
    # FASE 1: Ingesta RAW (Descarga -> DuckDB Raw Tables)
    logger.info(">>> FASE 1: Ingesta de IDs RAW a DuckDB...")
    pipeline.run(tmdb_daily_ids_stream(entity="movie"))
    pipeline.run(tmdb_daily_ids_stream(entity="person"))

    # FASE 2: Enriquecimiento (DuckDB Sorted -> API -> DuckDB Final Tables)
    logger.info(">>> FASE 2: Enriquecimiento consultando IDs ordenados...")
    
    # Creamos un recurso wrapping para el generador, para poder usar pipe |
    # (Aunque pipeline.run acepta generadores directos, envolverlo en resource es mas limpio si queremos opciones)
    # Pero simple es mejor:
    
    movies_source = dlt.resource(
        get_sorted_ids(entity="movie", limit=args.limit), 
        name="sorted_movie_ids"
    )
    
    people_source = dlt.resource(
        get_sorted_ids(entity="person", limit=args.limit), 
        name="sorted_person_ids"
    )

    pipeline.run(movies_source | fetch_tmdb_details)
    pipeline.run(people_source | fetch_tmdb_details)

    logger.info("Pipeline de primer ingesta completado")
