from kafka import KafkaConsumer
import json
import psycopg2
import time

# -----------------------------
# Wait for Postgres
# -----------------------------
while True:
    try:
        conn = psycopg2.connect(
            host="postgres",
            database="ingestion_db",
            user="ingestion",
            password="ingestion"
        )
        break
    except Exception:
        print("Waiting for Postgres...")
        time.sleep(2)

cursor = conn.cursor()

# -----------------------------
# Ensure table exists
# -----------------------------
cursor.execute("""
CREATE TABLE IF NOT EXISTS file_metadata (
    id SERIAL PRIMARY KEY,
    upload_id VARCHAR(100),
    file_name TEXT,
    file_path TEXT,
    file_size BIGINT,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (upload_id, file_name)
);
""")
conn.commit()

print("Postgres is ready. Table ensured.")

# -----------------------------
# Kafka Consumer
# -----------------------------
consumer = KafkaConsumer(
    "upload-progress",
    bootstrap_servers="redpanda:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True
)

print("Processor service started. Waiting for events...")

# -----------------------------
# Consume events
# -----------------------------
for message in consumer:
    event = message.value

    if event.get("status") != "unzipping":
        continue

    upload_id = event["upload_id"]
    processed = event["processed_files"]

    file_name = f"file_{processed}"
    file_path = f"/tmp/uploads/{file_name}"
    file_size = 0

    cursor.execute(
        """
        INSERT INTO file_metadata (upload_id, file_name, file_path, file_size)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (upload_id, file_name) DO NOTHING
        """,
        (upload_id, file_name, file_path, file_size)
    )
    conn.commit()

    print(f"Inserted metadata for {file_name}")
