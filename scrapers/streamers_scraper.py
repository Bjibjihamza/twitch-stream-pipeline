import os
import pandas as pd
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from kafka import KafkaProducer
import sqlite3
import json

# ✅ Paths for Data Storage
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)
CATEGORY_CSV = os.path.join(DATA_DIR, "last_categories.csv")  # Read categories from here
STREAMS_CSV = os.path.join(DATA_DIR, "twitch_streams.csv")  # Append new streamers
LAST_STREAMS_CSV = os.path.join(DATA_DIR, "last_streams.csv")  # Overwrite with latest

# ✅ Kafka Producer Setup
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks=1  # Ensure message delivery
)

# ✅ Load Categories from `last_categories.csv`
if not os.path.exists(CATEGORY_CSV):
    print("⚠️ No `last_categories.csv` found. Run categories_scraper.py first.")
    exit()

try:
    categories_df = pd.read_csv(CATEGORY_CSV)
    categories = categories_df['Category'].dropna().unique().tolist()
except Exception as e:
    print(f"❌ Error reading `last_categories.csv`: {e}")
    exit()

# ✅ Create category URLs
category_urls = {
    category: f"https://www.twitch.tv/directory/category/{category.lower().replace(' ', '-')}?sort=VIEWER_COUNT"
    for category in categories
}

# ✅ SQLite Database Setup
DB_PATH = "db/twitch_data.db"
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

cursor.execute("""
    CREATE TABLE IF NOT EXISTS streamers_data (
        timestamp TEXT,
        category TEXT,
        stream_title TEXT,
        channel TEXT,
        viewers_count INTEGER,
        tags TEXT,
        PRIMARY KEY (timestamp, category, channel)
    )
""")
conn.commit()
conn.close()

# ✅ WebDriver Setup
options = webdriver.ChromeOptions()
options.add_argument("--headless")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

# ✅ Helper Functions
def save_to_csv(data, file_path, overwrite=False):
    """Save data to CSV, overwriting or appending."""
    df = pd.DataFrame(data, columns=['Timestamp', 'Category', 'Stream Title', 'Channel', 'Viewers', 'Tags'])
    mode = 'w' if overwrite else 'a'
    header = overwrite or not os.path.exists(file_path)
    df.to_csv(file_path, mode=mode, header=header, index=False, encoding="utf-8")
    print(f"✅ Data saved to `{file_path}`.")

def store_in_db(data):
    """Batch insert streamers data into SQLite."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.executemany("""
        INSERT OR REPLACE INTO streamers_data (timestamp, category, stream_title, channel, viewers_count, tags)
        VALUES (?, ?, ?, ?, ?, ?)
    """, [(d["Timestamp"], d["Category"], d["Stream Title"], d["Channel"], d["Viewers"], d["Tags"]) for d in data])
    conn.commit()
    conn.close()
    print("✅ Data saved in SQLite database.")

def convert_viewers_count(viewers_text):
    """Convert Twitch viewers count from '299K' or '20.7000' format to an integer."""
    viewers_text = viewers_text.replace(" viewers", "").replace(",", "").strip()  # Clean text
    if "K" in viewers_text:
        return int(float(viewers_text.replace("K", "")) * 1000)  # Convert '299K' to 299000
    if "." in viewers_text:  # ✅ Handle formats like '20.7000'
        return int(float(viewers_text) * 1000)
    return int(viewers_text) if viewers_text.isdigit() else 0

# ✅ Scraper Function
def scrape_twitch_category(category, url):
    """Scrape streamers from a Twitch category page and store the data."""
    print(f"📡 Scraping {category} - {url}")
    driver.get(url)
    time.sleep(3)

    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//h3[contains(@class, "CoreText")]'))
        )

        # ✅ Scrape Streamers
        streamers = driver.find_elements(By.XPATH, '//h3[contains(@class, "CoreText")]')
        streamer_names = [s.text for s in streamers if s.text.strip()]

        channels = driver.find_elements(By.XPATH, '//div[contains(@class, "Layout-sc-1xcs6mc-0 bQImNn")]')
        channel_names = [c.text for c in channels if c.text.strip()]

        viewers = driver.find_elements(By.XPATH, '//div[contains(@class, "ScMediaCardStatWrapper")]')
        viewers_counts = [convert_viewers_count(v.text) for v in viewers if v.text.strip()]

        tags = driver.find_elements(By.XPATH, '//button[contains(@class, "ScTag")]')
        tags_list = [t.text for t in tags if t.text.strip()]

        # ✅ Ensure Minimum Length
        min_length = min(len(streamer_names), len(channel_names), len(viewers_counts), len(tags_list))
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # ✅ Structure Data
        data = [{
            "Timestamp": timestamp,
            "Category": category,
            "Stream Title": streamer_names[i],
            "Channel": channel_names[i],
            "Viewers": viewers_counts[i],
            "Tags": tags_list[i]
        } for i in range(min_length)]

        # ✅ Store Data
        save_to_csv(data, STREAMS_CSV, overwrite=False)  # Append to `twitch_streams.csv`
        save_to_csv(data, LAST_STREAMS_CSV, overwrite=True)  # Overwrite `last_streams.csv`

        # ✅ Send to Kafka
        for item in data:
            producer.send("twitch_streams", value=item)
            print(f"📤 Sent to Kafka: {item}")

        return data

    except Exception as e:
        print(f"❌ Error scraping {category}: {e}")
        return []

# ✅ Scrape All Categories
all_last_streams = []
for category, url in category_urls.items():
    all_last_streams.extend(scrape_twitch_category(category, url))

# 🔴 Close WebDriver
driver.quit()
print("✅ Streamers extraction completed!")
