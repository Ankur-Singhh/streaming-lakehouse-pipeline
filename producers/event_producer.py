import json
import random
import time
from datetime import datetime, timezone
import boto3
from botocore.client import Config
from faker import Faker
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

fake = Faker()

KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "financial_transactions"
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"
BUCKET_NAME = "warehouse"

def ensure_minio_bucket():
    """Ensures that the Lakehouse storage bucket exists in MinIO."""
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1"
    )
    try:
        s3.head_bucket(Bucket=BUCKET_NAME)
        print(f"[MinIO] Bucket '{BUCKET_NAME}' already exists.")
    except Exception:
        s3.create_bucket(Bucket=BUCKET_NAME)
        print(f"[MinIO] Created bucket '{BUCKET_NAME}'.")

def get_kafka_producer(retries=10, delay=2):
    """Attempts connection to Kafka broker with retry backoff."""
    for i in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None
            )
            print(f"[Kafka] Connected successfully to {KAFKA_BROKER}")
            return producer
        except NoBrokersAvailable:
            print(f"[Kafka] Broker not ready yet, retrying in {delay}s ({i+1}/{retries})...")
            time.sleep(delay)
    raise RuntimeError("Failed to connect to Kafka broker after multiple attempts.")

def generate_transaction():
    """Generates a transaction payload with occasional anomalies for DLQ testing."""
    is_anomaly = random.random() < 0.05  # 5% chance of malformed data

    transaction_id = fake.uuid4()
    user_id = f"USR_{random.randint(1000, 9999)}"
    merchant_id = f"MERCH_{random.randint(100, 500)}"
    timestamp = datetime.now(timezone.utc).isoformat()
    
    if is_anomaly:
        amount = -round(random.uniform(10.0, 500.0), 2)
        currency = None
        status = "CORRUPT"
    else:
        amount = round(random.uniform(5.0, 1500.0), 2)
        currency = random.choice(["USD", "EUR", "INR", "GBP"])
        status = random.choice(["COMPLETED", "COMPLETED", "COMPLETED", "PENDING", "FAILED"])

    return {
        "transaction_id": transaction_id,
        "user_id": user_id,
        "merchant_id": merchant_id,
        "amount": amount,
        "currency": currency,
        "payment_method": random.choice(["CREDIT_CARD", "UPI", "BANK_TRANSFER", "DEBIT_CARD"]),
        "status": status,
        "timestamp": timestamp
    }

def main():
    print("Initializing Streaming Pipeline Producer...")
    ensure_minio_bucket()
    producer = get_kafka_producer()

    print(f"Streaming transactions to topic '{TOPIC_NAME}' (Press Ctrl+C to stop)...")
    count = 0
    try:
        while True:
            tx = generate_transaction()
            producer.send(TOPIC_NAME, key=tx["user_id"], value=tx)
            count += 1
            if count % 10 == 0:
                print(f"[Produced {count} events] Last: tx_id={tx['transaction_id'][:8]} user={tx['user_id']} amt={tx['amount']} {tx['currency']}")
            time.sleep(0.2)
    except KeyboardInterrupt:
        print(f"\nStopped producer. Total events sent: {count}")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()
