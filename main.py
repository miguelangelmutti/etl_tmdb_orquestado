from ingestion import AsyncAPIConsumer
import asyncio
from config import MAX_CONCURRENCY, TOTAL_REQUESTS, MOVIES_CHANGES_URL, API_KEY, headers

if __name__ == "__main__":
    consumer = AsyncAPIConsumer(MAX_CONCURRENCY, TOTAL_REQUESTS, MOVIES_CHANGES_URL, API_KEY, headers)
    asyncio.run(consumer.run())
