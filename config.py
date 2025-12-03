from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")
headers = {
    "Authorization": f"Bearer {API_KEY}",
    "Accept": "application/json",
    "Content-Type": "application/json",
}

MAX_CONCURRENCY = 5    # Máximo de peticiones simultáneas
TOTAL_REQUESTS = 20    # Cantidad total de IDs a consultar (ej. 20 películas)
MOVIES_CHANGES_URL = "https://api.themoviedb.org/3/movie/changes"

