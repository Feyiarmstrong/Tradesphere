
import boto3
import os
import logging
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

s3_client = boto3.client("s3", region_name="us-east-1")
RAW_BUCKET = "tradesphere-raw-feyisayo"


def upload_to_s3(local_path, s3_key):
    """Uploads a local file to S3. Idempotent — overwrites if already exists."""
    try:
        s3_client.upload_file(local_path, RAW_BUCKET, s3_key)
        logger.info(f"Uploaded {local_path} to s3://{RAW_BUCKET}/{s3_key}")
    except Exception as e:
        logger.error(f"Failed to upload {local_path} to S3: {e}")
        raise


def ingest_csv_files():
    """
    Ingests DataCo CSV file from local data folder into S3 source layer.
    Idempotent — S3 upload overwrites existing file on each run.
    """
    csv_files = {
        "data/DataCoSupplyChainDataset.csv": "source/orders/DataCoSupplyChainDataset.csv"
    }

    for local_path, s3_key in csv_files.items():
        if os.path.exists(local_path):
            upload_to_s3(local_path, s3_key)
        else:
            logger.warning(f"File not found: {local_path} — skipping")


if __name__ == "__main__":
    ingest_csv_files()