from kafka import KafkaConsumer
import json
import mysql.connector
from datetime import datetime

# ✅ Kafka Configuration
TOPIC_NAME = "twitch-categories"
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
        viewers = data["Viewers"]
        tags = data["Tags"]
        image_url = data["Image_URL"]

        # ✅ Connect to MySQL
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()

        # ✅ Debugging: Print SQL before execution
        sql = "INSERT INTO categories (timestamp, category, viewers, tags, image_url) VALUES (%s, %s, %s, %s, %s)"
        values = (timestamp, category, viewers, tags, image_url)
        print(f"🔹 DEBUG: SQL Query - {sql} \n🔹 Values - {values}")  # ✅ Print the SQL query

        cursor.execute(sql, values)
        conn.commit()

        print(f"✅ Data saved: {category} - {viewers} viewers")
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
        print(f"📩 Received message from Kafka: {message.value}")  # Debug print
        if message.value:
            insert_into_database(message.value)  # Store in MySQL
        else:
            print("⚠️ Received an empty message. Skipping.")
    except Exception as e:
        print(f"❌ Error processing message: {e}")
