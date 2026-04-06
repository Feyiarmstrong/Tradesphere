import boto3
import pandas as pd
import json
import time
import logging
from io import StringIO
from kafka import KafkaProducer
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

s3_client = boto3.client("s3", region_name="us-east-1")
RAW_BUCKET = "tradesphere-raw-feyisayo"


def get_producer():
    """Creates and returns a Kafka producer with JSON serialization."""
    try:
        return KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8")
        )
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        raise


def read_orders_from_s3():
    """Reads the DataCo orders CSV from S3 and returns a pandas DataFrame."""
    try:
        obj = s3_client.get_object(
            Bucket=RAW_BUCKET,
            Key="source/orders/DataCoSupplyChainDataset.csv"
        )
        return pd.read_csv(StringIO(obj["Body"].read().decode("latin-1")))
    except Exception as e:
        logger.error(f"Failed to read orders from S3: {e}")
        raise


def stream_orders():
    """
    Reads orders row by row and sends each as a JSON message to tradesphere-orders topic.
    Simulates real-time order events from a live e-commerce system.
    Failed messages are sent to the dead letter queue (tradesphere-dlq).
    """
    logger.info("Starting TradeSphere Kafka producer...")
    producer = get_producer()
    df = read_orders_from_s3()
    logger.info(f"Loaded {len(df)} orders from S3")

    sent = 0
    failed = 0

    for index, row in df.iterrows():
        try:
            message = row.to_dict()
            message["kafka_ingested_at"] = datetime.utcnow().isoformat()

            producer.send("tradesphere-orders", value=message)
            sent += 1

            if sent % 1000 == 0:
                logger.info(f"Sent {sent} messages to tradesphere-orders...")

            # Simulate real-time streaming with small delay
            time.sleep(0.01)

        except Exception as e:
            logger.error(f"Failed to send message at index {index}: {e}")
            producer.send("tradesphere-dlq", value={"error": str(e), "index": index})
            failed += 1

    producer.flush()
    logger.info(f"Streaming complete — sent: {sent}, failed: {failed}")


if __name__ == "__main__":
    stream_orders()