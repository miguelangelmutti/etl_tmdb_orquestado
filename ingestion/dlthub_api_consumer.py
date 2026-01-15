import sys
import os

# Agrega el directorio padre al sys.path para poder importar config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import dlt
from config import TMDB_BASE_URL, DAILY_EXPORT_BASE_URL, TEST_DAILY_EXPORT_BASE_URL, DB_PATH, BASE_DIR,LOG_FILE,DLT_SCHEMA_PATH
from dlt.sources.rest_api import rest_api_source
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
from dlt.common.pipeline import get_dlt_pipelines_dir
from dlt.destinations import duckdb
from utils.logger import setup_logger

class tmdb_api_consumer:
    def __init__(self, url, fecha_inicio, fecha_fin):
            self.tmdb_client = rest_api_source({
            "client": {
                    "base_url": url,
                    "auth": {
                        "type": "bearer",
                        "token": dlt.secrets["tmdb_access_token"]
                    },
                    "paginator": PageNumberPaginator(total_path="total_pages",base_page=1)
            },    
                "resources": [  
                    {
                        "name": "ids_movies_changes",
                        "endpoint": {
                            "path": "movie/changes",
                            "params": {
                                "start_date": fecha_inicio,
                                "end_date": fecha_fin
                            },
                        },
                    },
                    {
                        "name": "movie_changes",                        
                        "endpoint": {
                            "path": "movie/{resources.ids_movies_changes.id}?append_to_response=credits",
                            "response_actions": [
                                {"status_code": 404, "action": "ignore"},
                                {"content": "Not found", "action": "ignore"},
                            ]
                        },                
                    },
                    {
                        "name": "ids_persons_changes",
                        "endpoint": {
                            "path": "person/changes",
                            "params": {
                                "start_date": fecha_inicio,
                                "end_date": fecha_fin
                            },
                        },
                    },
                    {
                        "name": "person_changes",                        
                        "endpoint": {
                            "path": "person/{resources.ids_persons_changes.id}",
                            "response_actions": [
                                {"status_code": 404, "action": "ignore"},
                                {"content": "Not found", "action": "ignore"},
                            ]
                        },                
                    },                    
                ],
                
        },parallelized=True, name="TMDb_API_movies_changes")
    
    def run(self):
                
        # Construimos la ruta absoluta a la DB
        #BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        # DB_PATH ya viene absoluta desde config.py
        
        pipeline = dlt.pipeline(
            pipeline_name="tmdb_movies_changes",
            destination=duckdb(DB_PATH),
            dataset_name="raw_movies",
            import_schema_path=DLT_SCHEMA_PATH+"/import",
            export_schema_path=DLT_SCHEMA_PATH+"/export"
        )    

        pipeline.run(self.tmdb_client)

if __name__ == "__main__":
    logger = setup_logger(
    __name__, 
    log_file=LOG_FILE, 
    capture_external_loggers=["dlt"]  # Captura logs de dlt
    )

    tmdb_api_consumer = tmdb_api_consumer(TMDB_BASE_URL, "2026-01-12", "2026-01-14")
    tmdb_api_consumer.run()