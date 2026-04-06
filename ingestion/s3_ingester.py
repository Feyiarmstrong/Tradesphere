import boto3
import os
from dotenv import load_dotenv

load_dotenv()

s3_client = boto3.client("s3", region_name="us-east-1")
RAW_BUCKET = "tradesphere-raw-feyisayo"

def upload_to_s3(local_path, s3_key):
    s3_client.upload_file(local_path, RAW_BUCKET, s3_key)
    print(f"Uploaded {local_path} to s3://{RAW_BUCKET}/{s3_key}")

def ingest_csv_files():
    csv_files = {
        "data/DataCoSupplyChainDataset.csv": "source/orders/DataCoSupplyChainDataset.csv"
    }

    for local_path, s3_key in csv_files.items():
        if os.path.exists(local_path):
            upload_to_s3(local_path, s3_key)
        else:
            print(f"File not found: {local_path} — skipping")

if __name__ == "__main__":
    ingest_csv_files()