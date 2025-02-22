from kafka import KafkaConsumer
import json
import sqlite3
import pandas as pd
import os

# ✅ Define Paths
DB_PATH = "db/twitch_data.db"
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)
STREAMS_CSV = os.path.join(DATA_DIR, "twitch_streams.csv")

# ✅ Kafka Consumer Setup
consumer = KafkaConsumer(
    "twitch_streams",
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

def store_in_db(data):
    """Insert or update received streamers data into SQLite database."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # ✅ Convert keys to lowercase
    data = {k.lower(): v for k, v in data.items()}

    try:
        # ✅ Insert data into the streamers_data table
        cursor.execute("""
            INSERT OR REPLACE INTO streamers_data (timestamp, category, stream_title, channel, viewers_count, tags)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            data["timestamp"],
            data["category"],
            data["stream title"],  # Match key
            data["channel"],
            data["viewers"],  # Correct the key name
            data["tags"]
        ))

        # Commit and close connection
        conn.commit()
    except Exception as e:
        print(f"❌ Error storing data in DB: {e}")
    finally:
        conn.close()

# ✅ Save Data to CSV
def save_to_csv(data):
    """Save received Kafka data into CSV."""
    df = pd.DataFrame([data])
    file_exists = os.path.exists(STREAMS_CSV)
    df.to_csv(STREAMS_CSV, mode='a', header=not file_exists, index=False, encoding="utf-8")
    print(f"✅ Data saved to `{STREAMS_CSV}`.")

# ✅ Start Kafka Consumer
print("📥 Listening for messages from Kafka...")
for message in consumer:
    stream_data = message.value
    print(f"📩 Received: {stream_data}")

    # Check if all necessary keys exist
    if all(key in stream_data for key in ['timestamp', 'category', 'stream title', 'channel', 'viewers', 'tags']):
        store_in_db(stream_data)  # ✅ Store in SQLite
        save_to_csv(stream_data)  # ✅ Save in CSV
    else:
        print("⚠️ Incomplete data received, skipping...")
