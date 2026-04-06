
import psycopg2
import boto3
import pandas as pd
import os
import logging
from dotenv import load_dotenv
from io import StringIO

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

s3_client = boto3.client("s3", region_name="us-east-1")
RAW_BUCKET = "tradesphere-raw-feyisayo"


def get_db_connection():
    """Creates and returns a Supabase PostgreSQL connection using env credentials."""
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            sslmode="require"
        )
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        raise


def table_to_s3(table_name, s3_key):
    """
    Reads a full table from PostgreSQL and uploads it to S3 as CSV.
    Idempotent — S3 put_object overwrites existing file on each run.
    """
    try:
        conn = get_db_connection()
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        conn.close()

        if len(df) == 0:
            logger.warning(f"Table {table_name} is empty — skipping upload")
            return

        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        s3_client.put_object(
            Bucket=RAW_BUCKET,
            Key=s3_key,
            Body=csv_buffer.getvalue()
        )
        logger.info(f"Ingested {table_name} to s3://{RAW_BUCKET}/{s3_key} — {len(df)} rows")

    except Exception as e:
        logger.error(f"Failed to ingest {table_name}: {e}")
        raise


def ingest_postgres_tables():
    """Ingests order_returns and customer_complaints from Supabase to S3."""
    table_to_s3("order_returns", "source/order_returns/order_returns.csv")
    table_to_s3("customer_complaints", "source/customer_complaints/customer_complaints.csv")


if __name__ == "__main__":
    ingest_postgres_tables()