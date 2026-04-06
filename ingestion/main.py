from s3_ingester import ingest_csv_files
from postgres_ingester import ingest_postgres_tables
from gsheets_ingester import ingest_store_regions
from api_ingester import ingest_exchange_rates

def run_all():
    print("Starting TradeSphere ingestion pipeline...")

    print("\n[1/4] Ingesting CSV files to S3...")
    ingest_csv_files()

    print("\n[2/4] Ingesting PostgreSQL tables...")
    ingest_postgres_tables()

    print("\n[3/4] Ingesting Google Sheets data...")
    ingest_store_regions()

    print("\n[4/4] Ingesting exchange rates from API...")
    ingest_exchange_rates()

    print("\nAll sources ingested successfully.")

if __name__ == "__main__":
    run_all()