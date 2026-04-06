import requests
import boto3
import json
from datetime import datetime

s3_client = boto3.client("s3", region_name="us-east-1")
RAW_BUCKET = "tradesphere-raw-feyisayo"

CURRENCIES = ["USD", "GBP", "EUR", "CAD", "AUD", "NGN", "JPY", "ZAR"]
API_URL = "https://api.exchangerate-api.com/v4/latest/USD"

def ingest_exchange_rates():
    response = requests.get(API_URL)
    data = response.json()

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
    print(f"Ingested exchange rates to s3://{RAW_BUCKET}/{s3_key}")

if __name__ == "__main__":
    ingest_exchange_rates()