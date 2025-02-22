import time
import pandas as pd
import json
from kafka import KafkaProducer
import schedule
import os

# ✅ Kafka Producer Setup
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks=1  # Ensure message delivery
)

# ✅ Paths for Data Files
DATA_DIR = "data"
last_streams_file = os.path.join(DATA_DIR, "last_streams.csv")
last_categories_file = os.path.join(DATA_DIR, "last_categories.csv")

# ✅ Helper Function to Read CSV Files
def read_last_data():
    """Read last streams and categories data from CSV."""
    last_streams = pd.read_csv(last_streams_file)
    last_categories = pd.read_csv(last_categories_file)
    
    # Convert DataFrame to list of dictionaries
    streams_data = last_streams.to_dict(orient='records')
    categories_data = last_categories.to_dict(orient='records')

    return streams_data, categories_data

# ✅ Function to Send Data to Kafka
def send_data_to_kafka():
    print("📡 Sending the latest data to Kafka...")
    streams_data, categories_data = read_last_data()

    # Send Last Streams Data to Kafka
    for stream in streams_data:
        producer.send("twitch_streams", value=stream)
        print(f"📤 Sent stream to Kafka: {stream}")

    # Send Last Categories Data to Kafka
    for category in categories_data:
        producer.send("twitch_categories", value=category)
        print(f"📤 Sent category to Kafka: {category}")

# ✅ Schedule Task to Run Every Hour
schedule.every().hour.at(":00").do(send_data_to_kafka)

# ✅ Keep the script running and waiting for the next schedule
print("⏰ Kafka producer running, sending data every hour...")
while True:
    schedule.run_pending()
    time.sleep(60)  # Wait for the next scheduled task
