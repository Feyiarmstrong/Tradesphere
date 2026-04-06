import psycopg2
import boto3
import json
import pandas as pd
import os
from dotenv import load_dotenv
from io import StringIO

load_dotenv()

s3_client = boto3.client("s3", region_name="us-east-1")
RAW_BUCKET = "tradesphere-raw-feyisayo"

def get_db_connection():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        sslmode="require"
    )
    return conn

def table_to_s3(table_name, s3_key):
    conn = get_db_connection()
    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    conn.close()

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    s3_client.put_object(
        Bucket=RAW_BUCKET,
        Key=s3_key,
        Body=csv_buffer.getvalue()
    )
    print(f"Ingested {table_name} to s3://{RAW_BUCKET}/{s3_key} — {len(df)} rows")

def ingest_postgres_tables():
    table_to_s3("order_returns", "source/order_returns/order_returns.csv")
    table_to_s3("customer_complaints", "source/customer_complaints/customer_complaints.csv")

if __name__ == "__main__":
    ingest_postgres_tables()