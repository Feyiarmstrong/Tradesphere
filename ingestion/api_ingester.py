import requests
import boto3
import json
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

s3_client = boto3.client("s3", region_name="us-east-1")
RAW_BUCKET = "tradesphere-raw-feyisayo"

CURRENCIES = ["USD", "GBP", "EUR", "CAD", "AUD", "NGN", "JPY", "ZAR"]
API_URL = "https://api.exchangerate-api.com/v4/latest/USD"


def ingest_exchange_rates():
    """
    Fetches live exchange rates from API and uploads to S3 as JSON.
    Idempotent — file is named by date so each day gets one clean file.
    """
    try:
        response = requests.get(API_URL, timeout=10)
        response.raise_for_status()
        data = response.json()

        # Filter to only the currencies we need
        filtered = {
            "base": data["base"],
            "date": data["date"],
            "rates": {k: v for k, v in data["rates"].items() if k in CURRENCIES}
        }

        today = datetime.today().strftime("%Y-%m-%d")
        s3_key = f"source/exchange_rates/exchange_rates_{today}.json"

        s3_client.put_object(
            Bucket=RAW_BUCKET,
            Key=s3_key,
            Body=json.dumps(filtered, indent=2)
        )
        logger.info(f"Ingested exchange rates to s3://{RAW_BUCKET}/{s3_key}")

    except requests.exceptions.Timeout:
        logger.error("Exchange rates API timed out")
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"Exchange rates API request failed: {e}")
        raise
    except Exception as e:
        logger.error(f"Failed to ingest exchange rates: {e}")
        raise


if __name__ == "__main__":
    ingest_exchange_rates()