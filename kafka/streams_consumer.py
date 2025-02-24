from kafka import KafkaConsumer
import json
import mysql.connector
from datetime import datetime

# ✅ Kafka Configuration
TOPIC_NAME = "streams"
KAFKA_BROKER = "localhost:9092"

# ✅ MySQL Configuration
MYSQL_CONFIG = {
    "host": "localhost",
    "user": "hamzabji",
    "password": "4753",  # Change this if needed
    "database": "streamers",
}

# ✅ Initialize Kafka Consumer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print(f"📥 Listening for messages on topic: {TOPIC_NAME}")

def insert_into_database(data):
    """Insert received Kafka message into MySQL database."""
    try:
        timestamp = datetime.strptime(data["Timestamp"], "%Y-%m-%d %H:%M:%S")
        category = data["Category"]
        stream_title = data["Stream Title"]
        channel = data["Channel"]
        viewers = data["Viewers"]
        tags = data["Tags"]

        # ✅ Connect to MySQL
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()

        # ✅ Insert Query
        sql = "INSERT INTO streamers (timestamp, category, stream_title, channel, viewers, tags) VALUES (%s, %s, %s, %s, %s, %s)"
        cursor.execute(sql, (timestamp, category, stream_title, channel, viewers, tags))
        conn.commit()
        print(f"✅ Data saved: {stream_title} - {viewers} viewers")

    except mysql.connector.Error as err:
        print(f"❌ MySQL Error: {err}")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
            print("🔹 MySQL connection closed.")

# ✅ Start Kafka Consumer
for message in consumer:
    try:
        print(f"📩 Received message from Kafka: {message.value}")  
        if message.value:
            insert_into_database(message.value)  
        else:
            print("⚠️ Received an empty message. Skipping.")
    except Exception as e:
        print(f"❌ Error processing message: {e}")
