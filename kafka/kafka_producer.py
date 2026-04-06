import boto3
import pandas as pd
import json
import time
from io import StringIO
from kafka import KafkaProducer
from datetime import datetime

# S3 client to read the orders CSV
s3_client = boto3.client("s3", region_name="us-east-1")
RAW_BUCKET = "tradesphere-raw-feyisayo"

# Kafka producer config
# value_serializer converts Python dict to JSON bytes before sending
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8")
)


def read_orders_from_s3():
    """Reads the DataCo orders CSV from S3 and returns a pandas DataFrame."""
    obj = s3_client.get_object(
        Bucket=RAW_BUCKET,
        Key="source/orders/DataCoSupplyChainDataset.csv"
    )
    return pd.read_csv(StringIO(obj["Body"].read().decode("latin-1")))


def stream_orders():
    """
    Reads orders row by row and sends each one as a JSON message
    to the tradesphere-orders Kafka topic.
    Simulates real-time order events from a live system.
    """
    print("Starting TradeSphere Kafka producer...")
    df = read_orders_from_s3()
    print(f"Loaded {len(df)} orders from S3")

    for index, row in df.iterrows():
        try:
            # Convert row to dict and add ingestion timestamp
            message = row.to_dict()
            message["kafka_ingested_at"] = datetime.utcnow().isoformat()

            # Send to tradesphere-orders topic
            producer.send("tradesphere-orders", value=message)

            if index % 1000 == 0:
                print(f"Sent {index} messages to tradesphere-orders...")

            # Sleep 0.01s to simulate real-time streaming
            time.sleep(0.01)

        except Exception as e:
            # Send failed messages to dead letter queue
            print(f"Failed to send message at index {index}: {e}")
            producer.send("tradesphere-dlq", value={"error": str(e), "index": index})

    producer.flush()
    print(f"All {len(df)} orders streamed to Kafka successfully.")


if __name__ == "__main__":
    stream_orders()