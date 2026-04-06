import json
import os
import shutil
from kafka import KafkaConsumer
from deltalake.writer import write_deltalake
import pandas as pd
from datetime import datetime

# Local temp path for buffering messages before writing to Delta
BUFFER_PATH = "kafka_temp/streaming_orders"
RAW_BUCKET = "tradesphere-raw-feyisayo"

# How many messages to collect before writing a micro-batch to Delta
BATCH_SIZE = 100


def get_consumer():
    """
    Creates and returns a Kafka consumer subscribed to
    the tradesphere-orders topic.
    """
    return KafkaConsumer(
        "tradesphere-orders",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="tradesphere-streaming-group",
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )


def upload_delta_to_s3(local_path, s3_prefix):
    """
    Uploads local Delta Lake files to S3 after each micro-batch write.
    """
    import boto3
    s3_client = boto3.client("s3", region_name="us-east-1")
    for root, dirs, files in os.walk(local_path):
        for file in files:
            local_file = os.path.join(root, file)
            relative_path = os.path.relpath(local_file, local_path)
            s3_key = f"{s3_prefix}/{relative_path}".replace("\\", "/")
            s3_client.upload_file(local_file, RAW_BUCKET, s3_key)
    print(f"Micro-batch uploaded to s3://{RAW_BUCKET}/{s3_prefix}")


def process_batch(batch):
    """
    Converts a list of Kafka messages into a DataFrame
    and writes it as a Delta Lake micro-batch to S3.
    """
    df = pd.DataFrame(batch)
    df["stream_processed_at"] = datetime.utcnow().isoformat()

    # Write micro-batch to local Delta first
    write_deltalake(BUFFER_PATH, df, mode="append")

    # Upload to S3 silver streaming path
    upload_delta_to_s3(BUFFER_PATH, "silver/streaming/orders")
    print(f"Micro-batch of {len(df)} messages written to Silver Delta Lake")


def run_consumer():
    """
    Main streaming loop. Consumes messages from tradesphere-orders topic,
    buffers them into micro-batches of BATCH_SIZE, then writes each
    batch to Delta Lake every time the buffer fills up.
    """
    print("Starting TradeSphere Kafka consumer...")
    consumer = get_consumer()

    buffer = []
    total_processed = 0

    for message in consumer:
        try:
            # Add message to buffer
            buffer.append(message.value)

            # When buffer reaches BATCH_SIZE write a micro-batch
            if len(buffer) >= BATCH_SIZE:
                process_batch(buffer)
                total_processed += len(buffer)
                buffer = []
                print(f"Total messages processed: {total_processed}")

        except Exception as e:
            print(f"Error processing message: {e}")
            continue


if __name__ == "__main__":
    run_consumer()