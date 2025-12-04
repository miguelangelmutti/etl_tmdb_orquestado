from ingestion import AsyncAPIConsumer
import asyncio
from config import MAX_CONCURRENCY, TOTAL_REQUESTS, MOVIES_CHANGES_URL, API_KEY, headers
from utils.logger import setup_logger

if __name__ == "__main__":
    logger = setup_logger(__name__)
    consumer = AsyncAPIConsumer(MAX_CONCURRENCY, TOTAL_REQUESTS, MOVIES_CHANGES_URL, API_KEY, headers)
    logger.info("Iniciando consumo de API...")
    asyncio.run(consumer.run())
