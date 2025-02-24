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
TOPIC_NAME = 'twitch-categories'

# ✅ Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# ✅ Paths for Data Storage
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)
CATEGORY_CSV = os.path.join(DATA_DIR, "twitch_categories.csv")
LAST_CATEGORIES_CSV = os.path.join(DATA_DIR, "last_categories.csv")

# ✅ Twitch Directory URL
DIRECTORY_URL = "https://www.twitch.tv/directory?sort=VIEWER_COUNT"

# ✅ WebDriver Setup
options = webdriver.ChromeOptions()
options.add_argument("--headless")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

def convert_viewers_count(viewers_text):
    """Convert Twitch viewers count from '299K' or '20.7000' format to an integer."""
    viewers_text = viewers_text.replace(" viewers", "").replace(",", "").strip()

    if "K" in viewers_text:
        return int(float(viewers_text.replace("K", "")) * 1000)

    if "." in viewers_text:
        return int(float(viewers_text) * 1000)

    return int(viewers_text) if viewers_text.isdigit() else 0

def save_to_csv(data, file_path, overwrite=False):
    """Save data to CSV, overwriting or appending."""
    df = pd.DataFrame(data, columns=['Timestamp', 'Category', 'Viewers', 'Tags', 'Image_URL'])
    mode = 'w' if overwrite else 'a'
    header = overwrite or not os.path.exists(file_path)
    df.to_csv(file_path, mode=mode, header=header, index=False, encoding="utf-8")
    print(f"✅ Data saved to {file_path}.")

def send_to_kafka(data):
    """Send Twitch category data to Kafka"""
    producer.send(TOPIC_NAME, value=data)
    producer.flush()  # Ensure message delivery
    print(f"📤 Sent to Kafka: {data}")

def scroll_to_load_more():
    """Scroll down the page until no more categories are loaded."""
    last_category_count = 0
    scroll_attempts = 0
    max_scroll_attempts = 40

    while scroll_attempts < max_scroll_attempts:
        category_elements = driver.find_elements(By.XPATH, '//div[contains(@class, "game-card")]')
        current_category_count = len(category_elements)

        print(f"Attempt {scroll_attempts + 1}: {current_category_count} categories found.")
        
        if current_category_count == last_category_count:
            print(f"No new categories loaded. Stopping scroll.")
            break
        
        driver.execute_script("window.scrollBy(0, 1000);")
        time.sleep(6)

        last_category_count = current_category_count
        scroll_attempts += 1
        print(f"Scrolling... attempt {scroll_attempts}/{max_scroll_attempts}")
    
    print(f"Finished scrolling. Total categories loaded: {last_category_count}")

def scrape_twitch_categories():
    print("📡 Loading Twitch categories page...")
    driver.get(DIRECTORY_URL)

    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//div[contains(@class, "game-card")]'))
        )
        time.sleep(3)

        scroll_to_load_more()

        category_elements = driver.find_elements(By.XPATH, '//div[contains(@class, "game-card")]')
        print(f"Found {len(category_elements)} categories after scrolling.")

        categories_data = []

        for category_element in category_elements:
            try:
                category_name_element = category_element.find_elements(By.XPATH, './/h2')
                category_name = category_name_element[0].text.strip() if category_name_element else "Unknown"

                viewers_element = category_element.find_elements(By.XPATH, './/p')
                viewers_text = convert_viewers_count(viewers_element[0].text) if viewers_element else 0

                tag_elements = category_element.find_elements(By.XPATH, './/button[contains(@class, "tw-tag")]//span')
                tags = ", ".join([tag.text.strip() for tag in tag_elements if tag.text.strip()]) if tag_elements else "No Tags"

                image_element = category_element.find_elements(By.XPATH, './/img')
                image_url = image_element[0].get_attribute("src") if image_element else "No Image"

                if category_name != "Unknown" and viewers_text > 0:
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                    category_data = {
                        "Timestamp": timestamp,
                        "Category": category_name,
                        "Viewers": viewers_text,
                        "Tags": tags,
                        "Image_URL": image_url
                    }
                    
                    categories_data.append(category_data)

                    # ✅ Send data to Kafka
                    send_to_kafka(category_data)

            except Exception as e:
                print(f"⚠️ Error scraping category: {e}")

        if categories_data:
            save_to_csv(categories_data, CATEGORY_CSV, overwrite=False)
            save_to_csv(categories_data, LAST_CATEGORIES_CSV, overwrite=True)

        print(f"✅ Scraping completed. {len(categories_data)} categories scraped.")

    except Exception as e:
        print(f"❌ Error: {e}")


# ✅ Infinite Scraping Loop
try:
    while True:
        scrape_twitch_categories()
        print("⏳ Waiting for the next scrape cycle...")
        time.sleep(1800)  # Sleep for 30 min before the next scrape cycle (adjustable)

except KeyboardInterrupt:
    print("🛑 Scraping stopped manually.")
finally:
    # 🔴 Close WebDriver after the infinite loop ends (or on keyboard interrupt)
    driver.quit()
    print("WebDriver closed.")

