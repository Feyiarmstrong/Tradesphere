import boto3
import pandas as pd
import os
import shutil
from io import StringIO
from deltalake.writer import write_deltalake

s3_client = boto3.client("s3", region_name="us-east-1")
RAW_BUCKET = "tradesphere-raw-feyisayo"

def read_csv_from_s3(s3_key):
    obj = s3_client.get_object(Bucket=RAW_BUCKET, Key=s3_key)
    return pd.read_csv(StringIO(obj["Body"].read().decode("latin-1")))

def clean_column_names(df):
    df.columns = [c.replace(" ", "_").replace("(", "").replace(")", "")
                  .replace(",", "").replace(";", "").replace("\n", "")
                  .replace("\t", "").replace("=", "").replace("{", "")
                  .replace("}", "") for c in df.columns]
    return df

def upload_delta_to_s3(local_path, s3_prefix):
    for root, dirs, files in os.walk(local_path):
        for file in files:
            local_file = os.path.join(root, file)
            relative_path = os.path.relpath(local_file, local_path)
            s3_key = f"{s3_prefix}/{relative_path}".replace("\\", "/")
            s3_client.upload_file(local_file, RAW_BUCKET, s3_key)
    print(f"Uploaded to s3://{RAW_BUCKET}/{s3_prefix}")

def write_bronze(s3_key, local_output, s3_prefix, label):
    print(f"Writing {label} to Bronze Delta Lake...")
    df = read_csv_from_s3(s3_key)
    df = clean_column_names(df)
    if os.path.exists(local_output):
        shutil.rmtree(local_output)
    write_deltalake(local_output, df, mode="overwrite")
    upload_delta_to_s3(local_output, s3_prefix)
    print(f"{label} written to Bronze — {df.shape[0]} rows")

def run_delta_writer():
    print("Starting TradeSphere Delta Lake Bronze writer...")

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

    shutil.rmtree("delta_temp", ignore_errors=True)
    print("All sources written to Bronze Delta Lake.")

if __name__ == "__main__":
    run_delta_writer()