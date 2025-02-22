import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# ✅ Load Data
STREAMS_CSV = "data/twitch_streams.csv"
df = pd.read_csv(STREAMS_CSV)

# ✅ Set Page Title
st.title("📊 Twitch Streamers Dashboard")

# ✅ Sidebar Filters
st.sidebar.header("Filters")
categories = df['Category'].unique().tolist()
selected_category = st.sidebar.selectbox("Select Category", ["All Categories"] + categories)

# Filter by Category
if selected_category != "All Categories":
    df = df[df["Category"] == selected_category]

# ✅ Streamers Overview Section
st.subheader("Top 5 Streamers by Viewers")
top_streamers = df.nlargest(5, "Viewers")[["Stream Title", "Channel", "Viewers"]]
st.write(top_streamers)

# ✅ Streamers Visualization: Bar Chart
st.subheader("Top 5 Streamers by Viewers (Bar Chart)")
fig, ax = plt.subplots()
sns.barplot(x="Viewers", y="Stream Title", data=top_streamers, ax=ax)
ax.set_xlabel("Viewers")
ax.set_ylabel("Stream Title")
st.pyplot(fig)

# ✅ Most Popular Categories Section
st.subheader("Most Popular Categories")
category_counts = df["Category"].value_counts().reset_index()
category_counts.columns = ["Category", "Count"]

# Category Count Visualization
st.subheader("Most Popular Categories (Bar Chart)")
fig2, ax2 = plt.subplots()
sns.barplot(x="Count", y="Category", data=category_counts, ax=ax2)
ax2.set_xlabel("Number of Streams")
ax2.set_ylabel("Category")
st.pyplot(fig2)

# ✅ Live Data Preview Section
st.subheader("Live Data Preview")
# Show the latest 10 rows in the dataset
st.dataframe(df.tail(10))

# ✅ Date Filter Section (Optional)
st.sidebar.header("Date Range Filter")
date_range = st.sidebar.date_input(
    "Select Date Range",
    [df["Timestamp"].min(), df["Timestamp"].max()]
)

# Filter based on the selected date range
start_date, end_date = date_range
df["Timestamp"] = pd.to_datetime(df["Timestamp"])
df = df[(df["Timestamp"] >= start_date) & (df["Timestamp"] <= end_date)]

# ✅ Updated Live Data Preview based on the Date Filter
st.subheader(f"Live Data (From {start_date} to {end_date})")
st.dataframe(df.tail(10))

# ✅ Save Data Option (Optional)
if st.button("Download CSV"):
    df_to_download = df.to_csv(index=False)
    st.download_button(
        label="Download Filtered Data",
        data=df_to_download,
        file_name="filtered_twitch_data.csv",
        mime="text/csv"
    )
