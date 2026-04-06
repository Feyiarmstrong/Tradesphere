import boto3
import pandas as pd
import os
import shutil
import snowflake.connector
from dotenv import load_dotenv
from deltalake import DeltaTable

load_dotenv()

# S3 client using AWS credentials from .env
s3_client = boto3.client(
    "s3",
    region_name="us-east-1",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
)

RAW_BUCKET = "tradesphere-raw-feyisayo"


def get_snowflake_connection():
    """Creates and returns a Snowflake connection using env credentials."""
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )


def read_delta_from_s3(s3_prefix, local_temp):
    """
    Downloads Delta Lake files from S3 to a local temp folder
    and reads them into a pandas DataFrame.
    """
    if os.path.exists(local_temp):
        shutil.rmtree(local_temp)
    os.makedirs(local_temp)

    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=RAW_BUCKET, Prefix=s3_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            relative = os.path.relpath(key, s3_prefix).replace("/", os.sep)
            local_file = os.path.join(local_temp, relative)
            os.makedirs(os.path.dirname(local_file), exist_ok=True)
            s3_client.download_file(RAW_BUCKET, key, local_file)

    dt = DeltaTable(local_temp)
    return dt.to_pandas()


def create_table_if_not_exists(cursor, table_name, df):
    """
    Dynamically creates a Snowflake table based on DataFrame columns.
    All columns are created as VARCHAR for simplicity — dbt will cast types later.
    """
    columns = ", ".join([f'"{col}" VARCHAR' for col in df.columns])
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {columns}
        )
    """)


def load_dataframe_to_snowflake(df, table_name):
    """
    Loads a pandas DataFrame into a Snowflake table using
    write_pandas for efficient bulk loading.
    """
    from snowflake.connector.pandas_tools import write_pandas

    conn = get_snowflake_connection()
    cursor = conn.cursor()

    # Create table if it doesn't exist
    create_table_if_not_exists(cursor, table_name, df)

    # Truncate existing data before loading fresh batch
    cursor.execute(f"TRUNCATE TABLE IF EXISTS {table_name}")

    # Convert all columns to string to avoid type conflicts
    df = df.astype(str)

    success, nchunks, nrows, _ = write_pandas(
    conn=conn,
    df=df,
    table_name=table_name,
    auto_create_table=False,
    chunk_size=10000
    )
    print(f"Loaded {nrows} rows into {table_name}")
    cursor.close()
    conn.close()


def load_orders():
    """Reads orders from Silver Delta Lake and loads into Snowflake RAW schema."""
    print("Loading orders to Snowflake...")
    df = read_delta_from_s3("silver/orders/", "snowflake_temp/orders")
    load_dataframe_to_snowflake(df, "RAW_ORDERS")
    print(f"Orders loaded — {df.shape[0]} rows")


def load_returns():
    """Reads order_returns from Silver Delta Lake and loads into Snowflake RAW schema."""
    print("Loading order_returns to Snowflake...")
    df = read_delta_from_s3("silver/order_returns/", "snowflake_temp/returns")
    load_dataframe_to_snowflake(df, "RAW_RETURNS")
    print(f"order_returns loaded — {df.shape[0]} rows")


def load_complaints():
    """Reads customer_complaints from Silver Delta Lake and loads into Snowflake RAW schema."""
    print("Loading customer_complaints to Snowflake...")
    df = read_delta_from_s3("silver/customer_complaints/", "snowflake_temp/complaints")
    load_dataframe_to_snowflake(df, "RAW_COMPLAINTS")
    print(f"customer_complaints loaded — {df.shape[0]} rows")


def load_store_regions():
    """Reads store_regions from Silver Delta Lake and loads into Snowflake RAW schema."""
    print("Loading store_regions to Snowflake...")
    df = read_delta_from_s3("silver/store_regions/", "snowflake_temp/store_regions")
    load_dataframe_to_snowflake(df, "RAW_STORE_REGIONS")
    print(f"store_regions loaded — {df.shape[0]} rows")


def run_snowflake_loader():
    """
    Main entry point. Loads all 4 Silver sources into Snowflake RAW schema.
    Tables are truncated and reloaded on each run — dbt handles transformations downstream.
    """
    print("Starting TradeSphere Snowflake loader...")

    load_orders()
    load_returns()
    load_complaints()
    load_store_regions()

    shutil.rmtree("snowflake_temp", ignore_errors=True)
    print("All sources loaded into Snowflake successfully.")


if __name__ == "__main__":
    run_snowflake_loader()