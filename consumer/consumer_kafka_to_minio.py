import os, json, time, signal
import logging
from datetime import datetime

import boto3
import pandas as pd
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

# kill kafka-python noise
logging.getLogger("kafka").setLevel(logging.CRITICAL)
logging.getLogger("kafka.conn").setLevel(logging.CRITICAL)
logging.getLogger("kafka.client").setLevel(logging.CRITICAL)

TOPICS = [t.strip() for t in os.getenv("TOPICS","").split(",") if t.strip()]
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
FLUSH_SECONDS = int(os.getenv("FLUSH_SECONDS", "15"))
HEARTBEAT_SECONDS = int(os.getenv("HEARTBEAT_SECONDS", "10"))

def extract_after(event: dict):
    if not isinstance(event, dict):
        return None
    # Case 1: wrapper payload
    if isinstance(event.get("payload"), dict):
        return event["payload"].get("after")
    # Case 2: no wrapper (schemas.enable=false)
    return event.get("after")

consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP"),
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    group_id=os.getenv("KAFKA_GROUP"),
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("MINIO_ENDPOINT"),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
)

bucket = os.getenv("MINIO_BUCKET")
if bucket not in [b["Name"] for b in s3.list_buckets().get("Buckets", [])]:
    s3.create_bucket(Bucket=bucket)
    print(f"✅ Created bucket: {bucket}")
else:
    print(f"✅ Using bucket: {bucket}")

buffers = {t: [] for t in TOPICS}
last_flush = {t: time.time() for t in TOPICS}
running = True

raw_count = {t: 0 for t in TOPICS}
kept_count = {t: 0 for t in TOPICS}
last_hb = time.time()

def flush(topic):
    records = buffers[topic]
    if not records:
        return

    table = topic.split(".")[-1]
    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    ts = datetime.utcnow().strftime("%H%M%S%f")

    local_path = f"./tmp_{table}_{ts}.parquet"
    key = f"{table}/date={date_str}/{table}_{ts}.parquet"

    df = pd.DataFrame(records)
    df.to_parquet(local_path, index=False)
    s3.upload_file(local_path, bucket, key)

    os.remove(local_path)
    buffers[topic] = []
    last_flush[topic] = time.time()

    consumer.commit()
    print(f"✅ Uploaded {len(df)} records -> s3://{bucket}/{key}")

def stop(sig, frame):
    global running
    running = False

signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)

print(f"✅ Consumer started. Kafka={os.getenv('KAFKA_BOOTSTRAP')} Topics={len(TOPICS)}")

try:
    while running:
        polled = consumer.poll(timeout_ms=1000)
        now = time.time()

        for tp, msgs in polled.items():
            for msg in msgs:
                event = msg.value or {}

                raw_count[msg.topic] += 1
                after = extract_after(event)

                if after:
                    kept_count[msg.topic] += 1
                    buffers[msg.topic].append(after)

                if len(buffers[msg.topic]) >= BATCH_SIZE:
                    flush(msg.topic)

        for topic in TOPICS:
            if buffers[topic] and (now - last_flush[topic] >= FLUSH_SECONDS):
                flush(topic)

        if now - last_hb >= HEARTBEAT_SECONDS:
            print("📊 Heartbeat:")
            for t in TOPICS:
                print(f"  {t}: raw={raw_count[t]} kept={kept_count[t]} buffered={len(buffers[t])}")
            last_hb = now

except Exception as e:
    print(f"❌ Consumer error: {e}")

finally:
    print("👋 Stopping… flushing remaining buffers.")
    for topic in TOPICS:
        if buffers[topic]:
            flush(topic)
    consumer.close()
    print("✅ Consumer exited cleanly.")
