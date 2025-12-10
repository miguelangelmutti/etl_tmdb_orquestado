from config import MOVIE_URL, LOG_FILE
from utils.logger import setup_logger
from ingestion.dlthub_api_consumer import tmdb_api_consumer

if __name__ == "__main__":
    logger = setup_logger(__name__, level="INFO", log_file=LOG_FILE, capture_external_loggers=["dlt"])
    dlthub_consumer = tmdb_api_consumer(MOVIE_URL, "2025-12-03", "2025-12-03")
    logger.info("Iniciando consumo de API...")
    dlthub_consumer.run()
    logger.info("Consumo de API terminado.")
