import sys
import os

# Agrega el directorio padre al sys.path para poder importar config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import dlt
from config import MOVIE_URL, DAILY_EXPORT_BASE_URL, TEST_DAILY_EXPORT_BASE_URL, DATABASE_FOLDER, DATABASE_NAME, BASE_DIR,LOG_FILE
from dlt.sources.rest_api import rest_api_source
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
from dlt.common.pipeline import get_dlt_pipelines_dir
from dlt.destinations import duckdb
from utils.logger import setup_logger

class tmdb_api_consumer:
    def __init__(self, MOVIE_URL, fecha_inicio, fecha_fin):
            self.tmdb_client = rest_api_source({
            "client": {
                    "base_url": MOVIE_URL,
                    "auth": {
                        "type": "bearer",
                        "token": dlt.secrets["tmdb_access_token"]
                    },
                    "paginator": PageNumberPaginator(total_path="total_pages",base_page=1)
            },    
                "resources": [  
                    {
                        "name": "changes",
                        "endpoint": {
                            "path": "changes",
                            "params": {
                                "start_date": fecha_inicio,
                                "end_date": fecha_fin
                            },
                        },
                    },
                    {
                        "name": "movie",                        
                        "endpoint": {
                            "path": "/{resources.changes.id}",
                            "response_actions": [
                                {"status_code": 404, "action": "ignore"},
                                {"content": "Not found", "action": "ignore"},
                            ]
                        },                
                    },
                ],
                
        },parallelized=True)
    
    def run(self):
                
        # Construimos la ruta absoluta a la DB
        #BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        DB_PATH = os.path.join(BASE_DIR, DATABASE_FOLDER, DATABASE_NAME)

        pipeline = dlt.pipeline(
            pipeline_name="tmdb_movies_changes",
            destination=duckdb(DB_PATH),
            dataset_name="raw_movies",
        )

        pipeline.run(self.tmdb_client)

if __name__ == "__main__":
    logger = setup_logger(
    __name__, 
    log_file=LOG_FILE, 
    capture_external_loggers=["dlt"]  # Captura logs de dlt
    )

    tmdb_api_consumer = tmdb_api_consumer(MOVIE_URL, "2025-12-03", "2025-12-03")
    tmdb_api_consumer.run()