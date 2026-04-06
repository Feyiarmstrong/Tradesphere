import logging
from s3_ingester import ingest_csv_files
from postgres_ingester import ingest_postgres_tables
from gsheets_ingester import ingest_store_regions
from api_ingester import ingest_exchange_rates

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def run_all():
    """
    Main entry point for the TradeSphere ingestion pipeline.
    Runs all 4 source ingestions in sequence.
    Each step is independent — one failure does not block others.
    """
    logger.info("Starting TradeSphere ingestion pipeline...")
    errors = []

    try:
        logger.info("[1/4] Ingesting CSV files to S3...")
        ingest_csv_files()
    except Exception as e:
        logger.error(f"CSV ingestion failed: {e}")
        errors.append("csv")

    try:
        logger.info("[2/4] Ingesting PostgreSQL tables...")
        ingest_postgres_tables()
    except Exception as e:
        logger.error(f"PostgreSQL ingestion failed: {e}")
        errors.append("postgres")

    try:
        logger.info("[3/4] Ingesting Google Sheets data...")
        ingest_store_regions()
    except Exception as e:
        logger.error(f"Google Sheets ingestion failed: {e}")
        errors.append("gsheets")

    try:
        logger.info("[4/4] Ingesting exchange rates from API...")
        ingest_exchange_rates()
    except Exception as e:
        logger.error(f"Exchange rates ingestion failed: {e}")
        errors.append("api")

    if errors:
        logger.error(f"Ingestion completed with failures: {errors}")
        raise Exception(f"Some sources failed to ingest: {errors}")

    logger.info("All sources ingested successfully.")


if __name__ == "__main__":
    run_all()