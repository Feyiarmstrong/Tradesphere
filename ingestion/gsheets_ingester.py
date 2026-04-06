
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import boto3
import logging
from io import StringIO

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

s3_client = boto3.client("s3", region_name="us-east-1")
RAW_BUCKET = "tradesphere-raw-feyisayo"


def get_gsheet_client():
    """Creates and returns an authenticated Google Sheets client."""
    try:
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = Credentials.from_service_account_file(
            "config/gcp_service_account.json",
            scopes=scopes
        )
        return gspread.authorize(creds)
    except Exception as e:
        logger.error(f"Failed to authenticate Google Sheets client: {e}")
        raise


def ingest_store_regions():
    """
    Reads store regions from Google Sheets and uploads to S3.
    Idempotent — S3 put_object overwrites existing file on each run.
    """
    try:
        client = get_gsheet_client()
        spreadsheet = client.open("tradesphere-store-regions")
        sheet = spreadsheet.sheet1

        data = sheet.get_all_records()
        df = pd.DataFrame(data)

        if len(df) == 0:
            logger.warning("Google Sheet is empty — skipping upload")
            return

        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        s3_client.put_object(
            Bucket=RAW_BUCKET,
            Key="source/store_regions/store_regions.csv",
            Body=csv_buffer.getvalue()
        )
        logger.info(f"Ingested store_regions to S3 — {len(df)} rows")

    except Exception as e:
        logger.error(f"Failed to ingest store regions: {e}")
        raise


if __name__ == "__main__":
    ingest_store_regions()