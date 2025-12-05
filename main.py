from ingestion import AsyncAPIConsumer
import asyncio
from config import MAX_CONCURRENCY, TOTAL_REQUESTS, MOVIES_CHANGES_URL, API_KEY, headers, MOVIE_URL
from utils.logger import setup_logger
import json

if __name__ == "__main__":
    logger = setup_logger(__name__)
    #MOVIES_CHANGES_URL_WITH_DATES = MOVIES_CHANGES_URL + "?start_date=2025-12-03&end_date=2025-12-03"
    #consumer = AsyncAPIConsumer(MAX_CONCURRENCY, TOTAL_REQUESTS, MOVIES_CHANGES_URL_WITH_DATES, API_KEY, headers)
    #logger.info("Iniciando consumo de API...")
    #asyncio.run(consumer.run())
    #logger.info("Consumo de API terminado.")

    
    list_movies_ids = []
    with open('all_changes_results.json') as f:
        data = json.load(f)

    for i in data:
        for j in i['results']:
            list_movies_ids.append(j['id'])
    
    logger.info("Iniciando consumo de API de películas...")
    movie_consumer = AsyncAPIConsumer(MAX_CONCURRENCY, TOTAL_REQUESTS, MOVIE_URL, API_KEY, headers)
    asyncio.run(movie_consumer.fetch_by_ids(list_movies_ids, MOVIE_URL))
    logger.info("Consumo de API de películas terminado.")
