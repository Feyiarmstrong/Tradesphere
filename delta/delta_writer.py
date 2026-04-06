import boto3
import pandas as pd
import os
import shutil
import logging
from io import StringIO
from deltalake.writer import write_deltalake

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

s3_client = boto3.client("s3", region_name="us-east-1")
RAW_BUCKET = "tradesphere-raw-feyisayo"


def read_csv_from_s3(s3_key):
    """Reads a CSV file from S3 into a pandas DataFrame. Uses latin-1 encoding for DataCo dataset."""
    try:
        obj = s3_client.get_object(Bucket=RAW_BUCKET, Key=s3_key)
        return pd.read_csv(StringIO(obj["Body"].read().decode("latin-1")))
    except Exception as e:
        logger.error(f"Failed to read CSV from S3 key {s3_key}: {e}")
        raise


def clean_column_names(df):
    """
    Removes special characters from column names that Delta Lake does not support.
    Characters removed: spaces, parentheses, commas, semicolons, tabs, equals, braces.
    """
    df.columns = [
        c.replace(" ", "_").replace("(", "").replace(")", "")
        .replace(",", "").replace(";", "").replace("\n", "")
        .replace("\t", "").replace("=", "").replace("{", "")
        .replace("}", "")
        for c in df.columns
    ]
    return df


def upload_delta_to_s3(local_path, s3_prefix):
    """
    Walks a local Delta folder and uploads every file to S3
    preserving the folder structure including _delta_log/.
    """
    try:
        for root, dirs, files in os.walk(local_path):
            for file in files:
                local_file = os.path.join(root, file)
                relative_path = os.path.relpath(local_file, local_path)
                s3_key = f"{s3_prefix}/{relative_path}".replace("\\", "/")
                s3_client.upload_file(local_file, RAW_BUCKET, s3_key)
        logger.info(f"Uploaded Delta files to s3://{RAW_BUCKET}/{s3_prefix}")
    except Exception as e:
        logger.error(f"Failed to upload Delta files to S3 prefix {s3_prefix}: {e}")
        raise


def write_bronze(s3_key, local_output, s3_prefix, label):
    """
    Reads a CSV from S3, cleans column names, writes as Delta Lake format locally,
    then uploads to S3 Bronze layer. Idempotent — overwrites existing Delta table on each run.
    """
    try:
        logger.info(f"Writing {label} to Bronze Delta Lake...")
        df = read_csv_from_s3(s3_key)
        df = clean_column_names(df)

        # Clean up local temp before writing to ensure idempotency
        if os.path.exists(local_output):
            shutil.rmtree(local_output)

        write_deltalake(local_output, df, mode="overwrite")
        upload_delta_to_s3(local_output, s3_prefix)
        logger.info(f"{label} written to Bronze — {df.shape[0]} rows")

    except Exception as e:
        logger.error(f"Failed to write {label} to Bronze: {e}")
        raise


def run_delta_writer():
    """
    Main entry point. Writes all 4 sources from S3 CSV to Bronze Delta Lake.
    Cleans up local temp files after each upload.
    """
    logger.info("Starting TradeSphere Delta Lake Bronze writer...")

    write_bronze(
        "source/orders/DataCoSupplyChainDataset.csv",
        "delta_temp/orders",
        "bronze/orders",
        "orders"
    )
    write_bronze(
        "source/order_returns/order_returns.csv",
        "delta_temp/order_returns",
        "bronze/order_returns",
        "order_returns"
    )
    write_bronze(
        "source/customer_complaints/customer_complaints.csv",
        "delta_temp/customer_complaints",
        "bronze/customer_complaints",
        "customer_complaints"
    )
    write_bronze(
        "source/store_regions/store_regions.csv",
        "delta_temp/store_regions",
        "bronze/store_regions",
        "store_regions"
    )

    # Clean up all local temp files after successful upload
    shutil.rmtree("delta_temp", ignore_errors=True)
    logger.info("All sources written to Bronze Delta Lake.")


if __name__ == "__main__":
    run_delta_writer()