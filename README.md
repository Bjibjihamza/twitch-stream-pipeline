# Twitch Analytics Platform

A comprehensive data pipeline for collecting, processing, and visualizing Twitch streaming data in real-time.

## Project Overview
This platform scrapes Twitch categories and streams, processes the data through Kafka, stores it in MySQL, and visualizes it using a Streamlit dashboard.

## Architecture
```
┌─────────────┐    ┌─────────┐    ┌─────────┐    ┌───────────────┐
│  Scrapers   │───>│  Kafka  │───>│  MySQL  │───>│  Dashboard    │
└─────────────┘    └─────────┘    └─────────┘    └───────────────┘
```

## Project Structure
```
twitch-kafka-analytics/
│
├── scrapers/
│   ├── data/                       # Storage for scraped data (CSV)
│   ├── categories_scraper.py       # Scrapes Twitch categories
│   └── streamer_info_scraper.py    # Scrapes streamer information
│
├── kafka/
│   ├── categories_consumer.py      # Consumes category data from Kafka
│   └── streams_consumer.py         # Consumes stream data from Kafka
│
├── dashboard/
│   └── main.py                     # Streamlit dashboard app
│
├── README.md                       # Project documentation
└── requirements.txt                # Dependencies
```

## Components
### 1. Scrapers

#### Categories Scraper:
- Collects top Twitch categories sorted by viewer count
- Stores data in CSV files
- Sends data to Kafka topic `twitch-categories`
- Runs on a 30-minute interval

#### Streamer Info Scraper:
- Collects active streams for each category
- Uses categories from the categories scraper
- Sends data to Kafka topic `streams`
- Runs on a 1-minute interval

### 2. Kafka Consumers

#### Categories Consumer:
- Processes category data from Kafka
- Stores data in MySQL `categories` table

#### Streams Consumer:
- Processes stream data from Kafka
- Stores data in MySQL `streamers` table

### 3. Dashboard

Interactive Streamlit dashboard with:
- Overview statistics
- Category analysis with heatmaps
- Streamer analytics
- Temporal analysis (by hour/day)

## Setup Instructions
### Prerequisites
- Python 3.8+
- MySQL
- Kafka

### Installation
Clone the repository:
```sh
git clone https://github.com/Bjibjhamza/twitch-kafka-analytics.git
cd twitch-kafka-analytics
```

Create a virtual environment:
```sh
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

Install dependencies:
```sh
pip install -r requirements.txt
```

### Set up MySQL
Create a database named `streamers` and create tables:
```sql
CREATE TABLE categories (
  id INT AUTO_INCREMENT PRIMARY KEY,
  timestamp DATETIME,
  category VARCHAR(255),
  viewers INT,
  tags TEXT,
  image_url TEXT
);

CREATE TABLE streamers (
  id INT AUTO_INCREMENT PRIMARY KEY,
  timestamp DATETIME,
  category VARCHAR(255),
  stream_title TEXT,
  channel VARCHAR(255),
  viewers INT,
  tags TEXT
);
```

### Configure Kafka
Create topics:
```sh
kafka-topics.sh --create --topic twitch-categories --bootstrap-server localhost:9092
kafka-topics.sh --create --topic streams --bootstrap-server localhost:9092
```

## Usage
Start the scrapers:
```sh
cd scrapers
python categories_scraper.py
python streamer_info_scraper.py
```

Start the Kafka consumers:
```sh
cd kafka
python categories_consumer.py
python streams_consumer.py
```

Launch the dashboard:
```sh
cd dashboard
streamlit run main.py
```

Access the dashboard at [http://localhost:8501](http://localhost:8501)

## Data Flow
1. `categories_scraper.py` collects Twitch categories and sends them to Kafka.
2. `categories_consumer.py` stores this data in MySQL.
3. `streamer_info_scraper.py` uses the categories to scrape streams and sends to Kafka.
4. `streams_consumer.py` stores stream data in MySQL.
5. The Streamlit dashboard queries MySQL to visualize the data.

## Customization
- Edit scraping intervals in the respective scraper files.
- Modify Kafka topics in the configuration sections.
- Adjust MySQL connection details in the consumer files.
- Customize dashboard visualizations in `dashboard/main.py`.

## Technologies Used
- **Selenium**: Web scraping
- **Kafka**: Message streaming
- **MySQL**: Data storage
- **Pandas**: Data processing
- **Streamlit**: Dashboard UI
- **Plotly**: Interactive visualizations
