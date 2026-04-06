import boto3
import pandas as pd
import os
import shutil
from io import StringIO
from datetime import datetime
from deltalake import DeltaTable
from deltalake.writer import write_deltalake

s3_client = boto3.client("s3", region_name="us-east-1")
RAW_BUCKET = "tradesphere-raw-feyisayo"


def read_delta_from_s3(s3_prefix, local_temp):
    """
    Downloads Delta Lake files from S3 to a local temp folder
    and reads them into a pandas DataFrame.
    """
    if os.path.exists(local_temp):
        shutil.rmtree(local_temp)
    os.makedirs(local_temp)

    # Download all files under the given S3 prefix
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=RAW_BUCKET, Prefix=s3_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            relative = os.path.relpath(key, s3_prefix).replace("/", os.sep)
            local_file = os.path.join(local_temp, relative)
            os.makedirs(os.path.dirname(local_file), exist_ok=True)
            s3_client.download_file(RAW_BUCKET, key, local_file)

    # Read the Delta table into pandas
    dt = DeltaTable(local_temp)
    return dt.to_pandas()


def read_exchange_rates():
    """
    Reads today's exchange rates JSON from S3.
    Falls back to hardcoded rates if file is not found.
    """
    today = datetime.today().strftime("%Y-%m-%d")
    s3_key = f"source/exchange_rates/exchange_rates_{today}.json"
    try:
        obj = s3_client.get_object(Bucket=RAW_BUCKET, Key=s3_key)
        import json
        data = json.loads(obj["Body"].read().decode("utf-8"))
        return data["rates"]
    except Exception:
        # Fallback rates if API file is missing
        return {"USD": 1.0, "GBP": 0.79, "EUR": 0.92}


def upload_delta_to_s3(local_path, s3_prefix):
    """
    Walks a local Delta folder and uploads every file to S3
    preserving the folder structure.
    """
    for root, dirs, files in os.walk(local_path):
        for file in files:
            local_file = os.path.join(root, file)
            relative_path = os.path.relpath(local_file, local_path)
            s3_key = f"{s3_prefix}/{relative_path}".replace("\\", "/")
            s3_client.upload_file(local_file, RAW_BUCKET, s3_key)
    print(f"Uploaded to s3://{RAW_BUCKET}/{s3_prefix}")


def transform_orders():
    """
    Reads orders from Bronze Delta Lake, applies transformations:
    - Deduplicates on Order_Id
    - Adds order_month, order_quarter, order_year, is_weekend
    - Converts Sales to USD, GBP, EUR using live exchange rates
    - Writes clean data to Silver Delta Lake
    """
    print("Reading orders from Bronze Delta Lake...")
    df = read_delta_from_s3("bronze/orders/", "spark_temp/bronze_orders")

    # Load exchange rates for currency conversion
    rates = read_exchange_rates()
    usd_rate = rates.get("USD", 1.0)
    gbp_rate = rates.get("GBP", 0.79)
    eur_rate = rates.get("EUR", 0.92)

    print("Applying transformations...")

    # Remove duplicate orders
    df = df.drop_duplicates(subset=["Order_Id"])

    # Parse order date and extract time dimensions
    df["order_date"] = pd.to_datetime(df["order_date_DateOrders"], errors="coerce")
    df["order_month"] = df["order_date"].dt.month
    df["order_quarter"] = df["order_date"].dt.quarter
    df["order_year"] = df["order_date"].dt.year
    df["is_weekend"] = df["order_date"].dt.dayofweek >= 5
    # Currency conversion columns
    if "Sales" in df.columns:
        df["order_value_usd"] = df["Sales"] * usd_rate
        df["order_value_gbp"] = df["Sales"] * gbp_rate
        df["order_value_eur"] = df["Sales"] * eur_rate

    # Timestamp for audit trail
    df["processed_at"] = datetime.utcnow().isoformat()

    local_output = "spark_temp/silver_orders"
    if os.path.exists(local_output):
        shutil.rmtree(local_output)

    write_deltalake(local_output, df, mode="overwrite")
    upload_delta_to_s3(local_output, "silver/orders")
    print(f"Orders written to Silver — {df.shape[0]} rows")


def transform_returns():
    """
    Reads order_returns from Bronze, deduplicates,
    calculates days_to_return, writes to Silver.
    """
    print("Reading order_returns from Bronze Delta Lake...")
    df = read_delta_from_s3("bronze/order_returns/", "spark_temp/bronze_returns")

    # Remove duplicate returns
    df = df.drop_duplicates(subset=["return_id"])

    # Parse dates and calculate how many days after order the return happened
    df["return_date"] = pd.to_datetime(df["return_date"], errors="coerce")
    df["days_to_return"] = (df["return_date"] - pd.Timestamp.now()).dt.days.abs()
    df["processed_at"] = datetime.utcnow().isoformat()

    local_output = "spark_temp/silver_returns"
    if os.path.exists(local_output):
        shutil.rmtree(local_output)

    write_deltalake(local_output, df, mode="overwrite")
    upload_delta_to_s3(local_output, "silver/order_returns")
    print(f"order_returns written to Silver — {df.shape[0]} rows")


def transform_complaints():
    """
    Reads customer_complaints from Bronze, deduplicates,
    casts resolved to boolean, writes to Silver.
    """
    print("Reading customer_complaints from Bronze Delta Lake...")
    df = read_delta_from_s3("bronze/customer_complaints/", "spark_temp/bronze_complaints")

    # Remove duplicate complaints
    df = df.drop_duplicates(subset=["complaint_id"])

    # Parse dates and ensure boolean type for resolved column
    df["complaint_date"] = pd.to_datetime(df["complaint_date"], errors="coerce")
    df["resolved"] = df["resolved"].astype(bool)
    df["processed_at"] = datetime.utcnow().isoformat()

    local_output = "spark_temp/silver_complaints"
    if os.path.exists(local_output):
        shutil.rmtree(local_output)

    write_deltalake(local_output, df, mode="overwrite")
    upload_delta_to_s3(local_output, "silver/customer_complaints")
    print(f"customer_complaints written to Silver — {df.shape[0]} rows")


def transform_store_regions():
    """
    Reads store_regions from Bronze, deduplicates,
    parses open_date, writes to Silver.
    """
    print("Reading store_regions from Bronze Delta Lake...")
    df = read_delta_from_s3("bronze/store_regions/", "spark_temp/bronze_store_regions")

    # Remove duplicate stores
    df = df.drop_duplicates(subset=["store_id"])

    # Parse open date
    df["open_date"] = pd.to_datetime(df["open_date"], errors="coerce")
    df["processed_at"] = datetime.utcnow().isoformat()

    local_output = "spark_temp/silver_store_regions"
    if os.path.exists(local_output):
        shutil.rmtree(local_output)

    write_deltalake(local_output, df, mode="overwrite")
    upload_delta_to_s3(local_output, "silver/store_regions")
    print(f"store_regions written to Silver — {df.shape[0]} rows")


def run_batch_transform():
    """
    Main entry point. Runs all 4 Silver transformations in sequence.
    Each source goes from Bronze Delta Lake to Silver Delta Lake.
    """
    print("Starting TradeSphere batch transformation...")

    transform_orders()
    transform_returns()
    transform_complaints()
    transform_store_regions()

    # Clean up local temp files after upload
    shutil.rmtree("spark_temp", ignore_errors=True)
    print("All sources transformed and written to Silver Delta Lake.")


if __name__ == "__main__":
    run_batch_transform()