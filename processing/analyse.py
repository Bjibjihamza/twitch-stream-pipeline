import pandas as pd
from sqlalchemy import create_engine

# MySQL connection configuration
MYSQL_CONFIG = {
    "host": "localhost",
    "user": "hamzabji",
    "password": "4753",  # Replace with your password
    "database": "streamers",
}

# Create a connection URL
connection_url = f"mysql+mysqlconnector://{MYSQL_CONFIG['user']}:{MYSQL_CONFIG['password']}@{MYSQL_CONFIG['host']}/{MYSQL_CONFIG['database']}"

# Create an engine to connect to the MySQL database
engine = create_engine(connection_url)

# Load categories table into pandas DataFrame
categories_df = pd.read_sql("SELECT * FROM categories", con=engine)

# Load streamers table into pandas DataFrame
streamers_df = pd.read_sql("SELECT * FROM streamers", con=engine)

# Display basic info about the datasets
print("Categories DataFrame Info:")
print(categories_df.info())
print("\nStreamers DataFrame Info:")
print(streamers_df.info())

# Clean up data
categories_df['timestamp'] = pd.to_datetime(categories_df['timestamp'])
streamers_df['timestamp'] = pd.to_datetime(streamers_df['timestamp'])

# Fill missing values in 'tags' column with 'No Tags'
categories_df['tags'] = categories_df['tags'].fillna('No Tags')
streamers_df['tags'] = streamers_df['tags'].fillna('No Tags')

# 1. Aggregate viewers by category in categories DataFrame
category_viewers = categories_df.groupby('category')['viewers'].sum().reset_index()
category_viewers_sorted = category_viewers.sort_values(by='viewers', ascending=False)

# 2. Filter streamers with more than 10,000 viewers
top_streamers = streamers_df[streamers_df['viewers'] > 10000]

# 3. Count streamers per category
streamer_count_by_category = streamers_df.groupby('category')['id'].count().reset_index()
streamer_count_by_category = streamer_count_by_category.rename(columns={'id': 'streamer_count'})

# 4. Merge streamers count with categories data
categories_streamers_df = pd.merge(category_viewers_sorted, streamer_count_by_category, on='category', how='left')

# 5. Calculate average viewers per streamer in each category
categories_streamers_df['avg_viewers_per_streamer'] = categories_streamers_df['viewers'] / categories_streamers_df['streamer_count']
categories_streamers_df = categories_streamers_df.fillna(0)

# 6. Create a new column for the day of the week the stream started
streamers_df['day_of_week'] = streamers_df['timestamp'].dt.day_name()

# 7. Count streamers by day of the week
streamers_by_day = streamers_df.groupby('day_of_week')['id'].count().reset_index()
streamers_by_day = streamers_by_day.rename(columns={'id': 'streamer_count'})

# Sort the days of the week in proper order
days_of_week = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
streamers_by_day['day_of_week'] = pd.Categorical(streamers_by_day['day_of_week'], categories=days_of_week, ordered=True)
streamers_by_day = streamers_by_day.sort_values('day_of_week')

# 8. Merge top streamers with categories data
top_streamers_categories = pd.merge(top_streamers, categories_df, on='category', how='left')

# 9. Save processed data for Streamlit visualization

# Save the category viewers data to a new table in MySQL
categories_streamers_df.to_sql('category_viewers_summary', con=engine, if_exists='replace', index=False)

# Save the top streamers data to a new table in MySQL
top_streamers_categories.to_sql('top_streamers_with_categories', con=engine, if_exists='replace', index=False)

# Save the streamers by day data to a new table in MySQL
streamers_by_day.to_sql('streamers_by_day', con=engine, if_exists='replace', index=False)

# Save the data to CSV files for Streamlit if preferred
categories_streamers_df.to_csv("category_viewers_summary.csv", index=False)
top_streamers_categories.to_csv("top_streamers_with_categories.csv", index=False)
streamers_by_day.to_csv("streamers_by_day.csv", index=False)

print("\n✅ Processed data saved back to the database and as CSV files.")
