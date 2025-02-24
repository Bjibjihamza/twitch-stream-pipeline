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
import json

# ✅ Kafka Configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'streams'

# ✅ Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# ✅ Paths for Data Storage
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)
LAST_CATEGORIES_CSV = os.path.join(DATA_DIR, "last_categories.csv")  

# ✅ Load Categories from `last_categories.csv`
if not os.path.exists(LAST_CATEGORIES_CSV):
    print("⚠️ No `last_categories.csv` found. Please run the categories scraper first.")
    exit()

try:
    categories_df = pd.read_csv(LAST_CATEGORIES_CSV)
    categories = categories_df['Category'].dropna().unique().tolist()
except Exception as e:
    print(f"❌ Error reading `last_categories.csv`: {e}")
    exit()

# ✅ WebDriver Setup
options = webdriver.ChromeOptions()
options.add_argument("--headless")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

# ✅ Helper Functions
def convert_viewers_count(viewers_text):
    """Convert Twitch viewers count from '299K' or '20.7000' format to an integer."""
    viewers_text = viewers_text.replace(" viewers", "").replace(",", "").strip()
    if "K" in viewers_text:
        return int(float(viewers_text.replace("K", "")) * 1000)
    if "." in viewers_text:
        return int(float(viewers_text) * 1000)
    return int(viewers_text) if viewers_text.isdigit() else 0

def send_to_kafka(data):
    """Send streamer data to Kafka."""
    producer.send(TOPIC_NAME, value=data)
    producer.flush()  # Ensure message delivery
    print(f"📤 Sent to Kafka: {data}")

# ✅ Scraper Function for streamers
def scrape_twitch_category(category):
    """Scrape streamers from a Twitch category page and send the data to Kafka."""
    category_url = f"https://www.twitch.tv/directory/category/{category.lower().replace(' ', '-')}?sort=VIEWER_COUNT"
    print(f"📡 Scraping {category} - {category_url}")
    
    driver.get(category_url)
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

        # ✅ Structure Data & Send to Kafka
        for i in range(min_length):
            stream_data = {
                "Timestamp": timestamp,
                "Category": category,
                "Stream Title": streamer_names[i],
                "Channel": channel_names[i],
                "Viewers": viewers_counts[i],
                "Tags": tags_list[i]
            }
            send_to_kafka(stream_data)

    except Exception as e:
        print(f"❌ Error scraping {category}: {e}")

# ✅ Infinite Scraping Loop
try:
    while True:
        # ✅ Scrape All Categories from `last_categories.csv`
        for category in categories:
            scrape_twitch_category(category)

        print("⏳ Waiting for the next scrape cycle...")
        time.sleep(60)  # Sleep for 1 min before the next scrape cycle (adjustable)

except KeyboardInterrupt:
    print("🛑 Scraping stopped manually.")
finally:
    # 🔴 Close WebDriver after the infinite loop ends (or on keyboard interrupt)
    driver.quit()
    print("✅ WebDriver closed.")
