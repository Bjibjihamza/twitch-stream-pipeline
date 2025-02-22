import pandas as pd
import os

# ✅ Load Data
STREAMS_CSV = "data/twitch_streams.csv"

if not os.path.exists(STREAMS_CSV):
    print("⚠️ No data found! Make sure the Kafka Consumer is running.")
    exit()

df = pd.read_csv(STREAMS_CSV)

# ✅ Clean Data
df.dropna(inplace=True)  # Remove missing values
df.drop_duplicates(inplace=True)  # Remove duplicate rows

# ✅ Convert Viewers to Integer
df["Viewers"] = df["Viewers"].astype(int)

# ✅ Save Cleaned Data
CLEANED_CSV = "data/twitch_streams_cleaned.csv"
df.to_csv(CLEANED_CSV, index=False)

print(f"✅ Data cleaned and saved in `{CLEANED_CSV}`")

# ✅ Show Basic Analysis
print("\n📊 Top 5 Streamers by Viewers:")
print(df.nlargest(5, "Viewers")[["Stream Title", "Channel", "Viewers"]])

print("\n🔍 Most Popular Categories:")
print(df["Category"].value_counts())
