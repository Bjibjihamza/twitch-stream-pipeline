# Twitch Stream Data Pipeline

This project is a real-time data collection and processing pipeline for Twitch stream data. It uses **web scraping**, **Kafka**, and **SQLite** to collect, process, and store data from Twitch categories and live streams. The system scrapes data from the Twitch website, stores it locally, and sends it to a Kafka topic for further analysis or consumption.

## Features

- **Data Scraping**: Uses Selenium to scrape Twitch categories and live stream data, including stream titles, channels, viewers, and tags.
- **Kafka Integration**: Data is sent to Kafka topics in real-time for further processing or storage.
- **Local Storage**: Scraped data is stored in **SQLite** and **CSV** format for persistence and easy access.
- **Real-time Processing**: Stream data is collected every hour and sent to Kafka to simulate a real-time processing environment.

## Components

1. **Web Scraping**: Scrapes live data from Twitch categories using Selenium and stores it in a local file or database.
2. **Kafka Producer**: Sends the scraped data to Kafka topics for further processing.
3. **SQLite Database**: Stores the streamed data for persistence.
4. **CSV Files**: Stores the most recent stream and category data in CSV files (`last_streams.csv` and `last_categories.csv`).
5. **Real-time Data Pipeline**: Utilizes a Kafka-based pipeline for continuous data flow.

## Setup

### Requirements

- Python 3.x
- Selenium
- Kafka
- SQLite
- WebDriver (ChromeDriver)

### Installation

1. **Clone the repository**:
    ```bash
    git remote add origin https://github.com/Bjibjihamza/twitch-stream-pipeline.git
    cd twitch-stream-pipeline
    ```

2. **Install the required dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

3. **Install and start Kafka** (Follow the Kafka installation guide to set up Kafka on your machine).

4. **Run the Scraper**:
    The scraper can be run manually to collect data from Twitch:
    ```bash
    python scrapers/scraper.py
    ```

5. **Start the Kafka Consumer**:
    Start the consumer to send data from Kafka to SQLite and CSV files:
    ```bash
    python kafka/consumer.py
    ```

### Automating the Scraper with `cron` (Optional)

To automate the scraping and Kafka data sending every hour, you can use a **cron job** on Linux.

1. Open your crontab editor:
    ```bash
    crontab -e
    ```

2. Add the following lines to run the scraper and Kafka producer hourly:
    ```bash
    0 * * * * /usr/bin/python3 /path/to/your/scraper.py
    5 * * * * /usr/bin/python3 /path/to/your/kafka_producer.py
    ```

### Structure

twitch-stream-pipeline/ │ ├── scrapers/ │ ├── scraper.py # Main scraper for collecting data from Twitch │ └── streamers_scraper.py # Scrapes live stream data │ ├── kafka/ │ ├── consumer.py # Consumer for sending data to SQLite and CSV │ └── producer.py # Producer for sending data to Kafka topics │ ├── data/ # Stores CSV files for stream data │ ├── twitch_categories.csv │ ├── last_categories.csv │ ├── last_streams.csv │ └── twitch_streams.csv │ ├── db/ # SQLite database for storing Twitch data │ └── twitch_data.db # The database file │ ├── requirements.txt # Python dependencies └── README.md # Project documentation

markdown
Copier
Modifier

## Usage

Once the project is set up, the following actions can be performed:

- **Scraping**: Run the `scraper.py` to collect data from Twitch categories and live streams.
- **Sending Data to Kafka**: Run the `kafka_producer.py` to send scraped data to Kafka for further processing.
- **Data Storage**: Data will be saved in both the **SQLite database** and **CSV files**. The `streamers_data` and `categories_data` tables store stream and category data respectively.
- **Consumer**: The Kafka consumer script listens to Kafka topics and processes the incoming stream data, saving it into the SQLite database and CSV files.

## Contributing

If you'd like to contribute, feel free to fork the repository and create pull requests. Please make sure your contributions are well-documented and tested.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

Happy streaming!