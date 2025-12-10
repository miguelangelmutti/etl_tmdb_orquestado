from dotenv import load_dotenv
import os
from pathlib import Path

load_dotenv()
BASE_DIR = Path(__file__).parent
LOG_FILE = BASE_DIR / "logs" / "elt-movies.log"

API_KEY = os.getenv("API_KEY")
TMDB_ACCESS_TOKEN = os.getenv("TOKEN")
headers = {
    "Authorization": f"Bearer {API_KEY}",
    "Accept": "application/json",
    "Content-Type": "application/json",
}

MAX_CONCURRENCY = 5    # Máximo de peticiones simultáneas
TOTAL_REQUESTS = 20    # Cantidad total de IDs a consultar (ej. 20 películas)
MOVIES_CHANGES_URL = "https://api.themoviedb.org/3/movie/changes"
MOVIE_URL = "https://api.themoviedb.org/3/movie/"
DAILY_EXPORT_BASE_URL = f"http://files.tmdb.org/p/exports/"
TEST_DAILY_EXPORT_BASE_URL = "https://drive.usercontent.google.com/uc?id=1CrorVUM2v_bOukryphiaVp2kw_M69fAL&export=download"
DATABASE_FOLDER = "database"
DATABASE_NAME = "shared_movies.duckdb"



