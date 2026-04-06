import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import boto3
from io import StringIO

s3_client = boto3.client("s3", region_name="us-east-1")
RAW_BUCKET = "tradesphere-raw-feyisayo"

def get_gsheet_client():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_file(
        "config/gcp_service_account.json",
        scopes=scopes
    )
    return gspread.authorize(creds)

def ingest_store_regions():
    client = get_gsheet_client()
    spreadsheet = client.open("tradesphere-store-regions")
    sheet = spreadsheet.sheet1

    data = sheet.get_all_records()
    df = pd.DataFrame(data)

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    s3_client.put_object(
        Bucket=RAW_BUCKET,
        Key="source/store_regions/store_regions.csv",
        Body=csv_buffer.getvalue()
    )
    print(f"Ingested store_regions to S3 — {len(df)} rows")

if __name__ == "__main__":
    ingest_store_regions()