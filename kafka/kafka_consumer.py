import json
import os
import shutil
import logging
from kafka import KafkaConsumer
from deltalake.writer import write_deltalake
import pandas as pd
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BUFFER_PATH = "kafka_temp/streaming_orders"
RAW_BUCKET = "tradesphere-raw-feyisayo"

# Number of messages to buffer before writing a micro-batch to Delta
BATCH_SIZE = 100


def get_consumer():
    """Creates and returns a Kafka consumer subscribed to tradesphere-orders topic."""
    try:
        return KafkaConsumer(
            "tradesphere-orders",
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="tradesphere-streaming-group",
            value_deserializer=lambda v: json.loads(v.decode("utf-8"))
        )
    except Exception as e:
        logger.error(f"Failed to create Kafka consumer: {e}")
        raise


def upload_delta_to_s3(local_path, s3_prefix):
    """Uploads local Delta Lake files to S3 after each micro-batch write."""
    import boto3
    s3_client = boto3.client("s3", region_name="us-east-1")
    try:
        for root, dirs, files in os.walk(local_path):
            for file in files:
                local_file = os.path.join(root, file)
                relative_path = os.path.relpath(local_file, local_path)
                s3_key = f"{s3_prefix}/{relative_path}".replace("\\", "/")
                s3_client.upload_file(local_file, RAW_BUCKET, s3_key)
        logger.info(f"Micro-batch uploaded to s3://{RAW_BUCKET}/{s3_prefix}")
    except Exception as e:
        logger.error(f"Failed to upload micro-batch to S3: {e}")
        raise


def deduplicate_batch(batch):
    """
    Deduplicates messages within a micro-batch using Order_Id.
    Prevents duplicate rows from entering Delta Lake if producer retries.
    """
    df = pd.DataFrame(batch)
    before = len(df)
    if "Order Id" in df.columns:
        df = df.drop_duplicates(subset=["Order Id"])
    after = len(df)
    if before != after:
        logger.info(f"Deduplicated batch: {before} -> {after} rows")
    return df


def process_batch(batch):
    """
    Converts a list of Kafka messages into a deduplicated DataFrame
    and writes it as a Delta Lake micro-batch to S3.
    Uses append mode — each micro-batch adds to the streaming table.
    """
    try:
        df = deduplicate_batch(batch)
        df["stream_processed_at"] = datetime.utcnow().isoformat()

        write_deltalake(BUFFER_PATH, df, mode="append")
        upload_delta_to_s3(BUFFER_PATH, "silver/streaming/orders")
        logger.info(f"Micro-batch of {len(df)} messages written to Silver Delta Lake")

    except Exception as e:
        logger.error(f"Failed to process batch: {e}")
        raise


def run_consumer():
    """
    Main streaming loop. Consumes messages from tradesphere-orders topic,
    buffers them into micro-batches of BATCH_SIZE, then writes each
    batch to Delta Lake and uploads to S3.
    """
    logger.info("Starting TradeSphere Kafka consumer...")
    consumer = get_consumer()

    buffer = []
    total_processed = 0

    for message in consumer:
        try:
            buffer.append(message.value)

            if len(buffer) >= BATCH_SIZE:
                process_batch(buffer)
                total_processed += len(buffer)
                buffer = []
                logger.info(f"Total messages processed: {total_processed}")

        except Exception as e:
            logger.error(f"Error processing message: {e}")
            continue

if __name__ == "__main__":
    run_consumer()