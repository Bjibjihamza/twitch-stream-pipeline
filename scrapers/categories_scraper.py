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

# ✅ Paths for Data Storage
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)
CATEGORY_CSV = os.path.join(DATA_DIR, "twitch_categories.csv")
last_categories_CSV = os.path.join(DATA_DIR, "last_categories.csv")

# ✅ Kafka Producer Setup
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks=1  # Ensure message delivery
)

# ✅ Twitch Directory URL
DIRECTORY_URL = "https://www.twitch.tv/directory?sort=VIEWER_COUNT"

# ✅ WebDriver Setup
options = webdriver.ChromeOptions()
options.add_argument("--headless")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

# ✅ Helper Functions
def scroll_to_load_more():
    """Scroll down the page to load more categories."""
    last_height = driver.execute_script("return document.body.scrollHeight")
    for _ in range(5):  # Scroll 5 times to load more content
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

def convert_viewers_count(viewers_text):
    """Convert Twitch viewers count from '299K' or '20.7000' format to an integer."""
    viewers_text = viewers_text.replace(" viewers", "").replace(",", "").strip()  # Clean text

    if "K" in viewers_text:
        return int(float(viewers_text.replace("K", "")) * 1000)  # Convert '299K' to 299000

    if "." in viewers_text:  # ✅ Handle formats like '20.7000'
        return int(float(viewers_text) * 1000)

    return int(viewers_text) if viewers_text.isdigit() else 0
 

def save_to_csv(data, file_path, overwrite=False):
    """Save data to CSV, overwriting or appending."""
    df = pd.DataFrame(data, columns=['Timestamp', 'Category', 'Viewers', 'Tags'])
    mode = 'w' if overwrite else 'a'
    header = overwrite or not os.path.exists(file_path)
    df.to_csv(file_path, mode=mode, header=header, index=False, encoding="utf-8")
    print(f"✅ Data saved to `{file_path}`.")

# ✅ Scraper Function
def scrape_twitch_categories():
    print("📡 Loading Twitch categories page...")
    driver.get(DIRECTORY_URL)

    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//div[contains(@class, "game-card")]'))
        )
        time.sleep(3)

        # Scroll to load more categories
        scroll_to_load_more()

        # ✅ Scrape Categories
        category_elements = driver.find_elements(By.XPATH, '//div[contains(@class, "game-card")]')

        categories_data = []

        for category_element in category_elements:
            try:
                category_name_element = category_element.find_elements(By.XPATH, './/h2')
                category_name = category_name_element[0].text.strip() if category_name_element else "Unknown"

                viewers_element = category_element.find_elements(By.XPATH, './/p')
                viewers_text = convert_viewers_count(viewers_element[0].text) if viewers_element else 0

                tag_elements = category_element.find_elements(By.XPATH, './/button[contains(@class, "tw-tag")]//span')
                tags = ", ".join([tag.text.strip() for tag in tag_elements if tag.text.strip()]) if tag_elements else "No Tags"

                if category_name != "Unknown" and viewers_text > 0:
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                    category_data = {
                        "Timestamp": timestamp,
                        "Category": category_name,
                        "Viewers": viewers_text,
                        "Tags": tags
                    }
                    
                    categories_data.append(category_data)

                    # ✅ Send data to Kafka
                    producer.send("twitch_categories", value=category_data)
                    print(f"📤 Sent to Kafka: {category_data}")

            except Exception as e:
                print(f"⚠️ Error scraping category: {e}")

        # ✅ Store Data
        if categories_data:
            save_to_csv(categories_data, CATEGORY_CSV, overwrite=False)  # Append to `twitch_categories.csv`
            save_to_csv(categories_data, last_categories_CSV, overwrite=True)  # Overwrite `last_categories.csv`

        print("✅ Scraping completed.")

    except Exception as e:
        print(f"❌ Error: {e}")

# ✅ Run the scraper
scrape_twitch_categories()

# 🔴 Close WebDriver
driver.quit()
