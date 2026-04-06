import boto3
import pandas as pd
import os
import shutil
import json
import logging
from datetime import datetime
from deltalake import DeltaTable
from deltalake.writer import write_deltalake

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

s3_client = boto3.client("s3", region_name="us-east-1")
RAW_BUCKET = "tradesphere-raw-feyisayo"


def read_delta_from_s3(s3_prefix, local_temp):
    """
    Downloads Delta Lake files from S3 to a local temp folder
    and reads them into a pandas DataFrame.
    """
    try:
        if os.path.exists(local_temp):
            shutil.rmtree(local_temp)
        os.makedirs(local_temp)

        paginator = s3_client.get_paginator("list_objects_v2")
        file_count = 0
        for page in paginator.paginate(Bucket=RAW_BUCKET, Prefix=s3_prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                relative = os.path.relpath(key, s3_prefix).replace("/", os.sep)
                local_file = os.path.join(local_temp, relative)
                os.makedirs(os.path.dirname(local_file), exist_ok=True)
                s3_client.download_file(RAW_BUCKET, key, local_file)
                file_count += 1

        if file_count == 0:
            raise ValueError(f"No files found at S3 prefix: {s3_prefix}")

        dt = DeltaTable(local_temp)
        df = dt.to_pandas()
        logger.info(f"Read {len(df)} rows from {s3_prefix}")
        return df

    except Exception as e:
        logger.error(f"Failed to read Delta from S3 prefix {s3_prefix}: {e}")
        raise


def read_exchange_rates():
    """
    Reads today's exchange rates JSON from S3.
    Falls back to hardcoded rates if today's file is not found.
    """
    today = datetime.today().strftime("%Y-%m-%d")
    s3_key = f"source/exchange_rates/exchange_rates_{today}.json"
    try:
        obj = s3_client.get_object(Bucket=RAW_BUCKET, Key=s3_key)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        logger.info(f"Loaded exchange rates for {today}")
        return data["rates"]
    except Exception:
        logger.warning(f"Exchange rates file not found for {today} — using fallback rates")
        return {"USD": 1.0, "GBP": 0.79, "EUR": 0.92}


def upload_delta_to_s3(local_path, s3_prefix):
    """Uploads local Delta files to S3 preserving folder structure."""
    try:
        for root, dirs, files in os.walk(local_path):
            for file in files:
                local_file = os.path.join(root, file)
                relative_path = os.path.relpath(local_file, local_path)
                s3_key = f"{s3_prefix}/{relative_path}".replace("\\", "/")
                s3_client.upload_file(local_file, RAW_BUCKET, s3_key)
        logger.info(f"Uploaded Silver Delta to s3://{RAW_BUCKET}/{s3_prefix}")
    except Exception as e:
        logger.error(f"Failed to upload Silver Delta to {s3_prefix}: {e}")
        raise


def transform_orders():
    """
    Reads orders from Bronze Delta Lake and applies Silver transformations:
    - Deduplicates on Order_Id
    - Parses order date and extracts time dimensions
    - Converts Sales to USD, GBP, EUR using live exchange rates
    - Adds processed_at audit timestamp
    Writes clean data to Silver Delta Lake.
    """
    try:
        logger.info("Transforming orders...")
        df = read_delta_from_s3("bronze/orders/", "spark_temp/bronze_orders")

        rates = read_exchange_rates()

        # Deduplicate on Order_Id — one row per unique order
        before = len(df)
        df = df.drop_duplicates(subset=["Order_Id"])
        logger.info(f"Deduplicated orders: {before} -> {len(df)} rows")

        # Parse order date and extract time dimensions for analytics
        df["order_date"] = pd.to_datetime(df["order_date_DateOrders"], errors="coerce")
        df["order_month"] = df["order_date"].dt.month
        df["order_quarter"] = df["order_date"].dt.quarter
        df["order_year"] = df["order_date"].dt.year
        df["is_weekend"] = df["order_date"].dt.dayofweek >= 5

        # Currency conversion using live exchange rates
        if "Sales" in df.columns:
            df["order_value_usd"] = df["Sales"] * rates.get("USD", 1.0)
            df["order_value_gbp"] = df["Sales"] * rates.get("GBP", 0.79)
            df["order_value_eur"] = df["Sales"] * rates.get("EUR", 0.92)

        df["processed_at"] = datetime.utcnow().isoformat()

        local_output = "spark_temp/silver_orders"
        if os.path.exists(local_output):
            shutil.rmtree(local_output)

        write_deltalake(local_output, df, mode="overwrite")
        upload_delta_to_s3(local_output, "silver/orders")
        logger.info(f"Orders written to Silver — {df.shape[0]} rows")

    except Exception as e:
        logger.error(f"Failed to transform orders: {e}")
        raise


def transform_returns():
    """
    Reads order_returns from Bronze, deduplicates on return_id,
    calculates days_to_return, writes to Silver.
    """
    try:
        logger.info("Transforming order_returns...")
        df = read_delta_from_s3("bronze/order_returns/", "spark_temp/bronze_returns")

        df = df.drop_duplicates(subset=["return_id"])
        df["return_date"] = pd.to_datetime(df["return_date"], errors="coerce")
        df["days_to_return"] = (df["return_date"] - pd.Timestamp.now()).dt.days.abs()
        df["processed_at"] = datetime.utcnow().isoformat()

        local_output = "spark_temp/silver_returns"
        if os.path.exists(local_output):
            shutil.rmtree(local_output)

        write_deltalake(local_output, df, mode="overwrite")
        upload_delta_to_s3(local_output, "silver/order_returns")
        logger.info(f"order_returns written to Silver — {df.shape[0]} rows")

    except Exception as e:
        logger.error(f"Failed to transform order_returns: {e}")
        raise


def transform_complaints():
    """
    Reads customer_complaints from Bronze, deduplicates on complaint_id,
    casts resolved to boolean, writes to Silver.
    """
    try:
        logger.info("Transforming customer_complaints...")
        df = read_delta_from_s3("bronze/customer_complaints/", "spark_temp/bronze_complaints")

        df = df.drop_duplicates(subset=["complaint_id"])
        df["complaint_date"] = pd.to_datetime(df["complaint_date"], errors="coerce")
        df["resolved"] = df["resolved"].astype(bool)
        df["processed_at"] = datetime.utcnow().isoformat()

        local_output = "spark_temp/silver_complaints"
        if os.path.exists(local_output):
            shutil.rmtree(local_output)

        write_deltalake(local_output, df, mode="overwrite")
        upload_delta_to_s3(local_output, "silver/customer_complaints")
        logger.info(f"customer_complaints written to Silver — {df.shape[0]} rows")

    except Exception as e:
        logger.error(f"Failed to transform customer_complaints: {e}")
        raise


def transform_store_regions():
    """
    Reads store_regions from Bronze, deduplicates on store_id,
    parses open_date, writes to Silver.
    """
    try:
        logger.info("Transforming store_regions...")
        df = read_delta_from_s3("bronze/store_regions/", "spark_temp/bronze_store_regions")

        df = df.drop_duplicates(subset=["store_id"])
        df["open_date"] = pd.to_datetime(df["open_date"], errors="coerce")
        df["processed_at"] = datetime.utcnow().isoformat()

        local_output = "spark_temp/silver_store_regions"
        if os.path.exists(local_output):
            shutil.rmtree(local_output)

        write_deltalake(local_output, df, mode="overwrite")
        upload_delta_to_s3(local_output, "silver/store_regions")
        logger.info(f"store_regions written to Silver — {df.shape[0]} rows")

    except Exception as e:
        logger.error(f"Failed to transform store_regions: {e}")
        raise


def run_batch_transform():
    """
    Main entry point. Runs all 4 Silver transformations in sequence.
    Cleans up local temp files after all uploads complete.
    """
    logger.info("Starting TradeSphere batch transformation...")

    transform_orders()
    transform_returns()
    transform_complaints()
    transform_store_regions()

    shutil.rmtree("spark_temp", ignore_errors=True)
    logger.info("All sources transformed and written to Silver Delta Lake.")



if __name__ == "__main__":
    run_batch_transform()