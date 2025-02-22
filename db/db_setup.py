import sqlite3

# Path to SQLite database
DB_PATH = "db/twitch_data.db"

# Connect to SQLite database (it will be created if it doesn't exist)
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Create categories_data table
cursor.execute("""
    CREATE TABLE IF NOT EXISTS categories_data (
        category TEXT PRIMARY KEY,
        viewers_count INTEGER,
        tags TEXT
    )
""")

# Create streamers_data table
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

# Insert initial values into categories_data table
categories_data = [
    ('Just Chatting', 420000, 'IRL, Chat'),
    ('VALORANT', 286000, 'FPS, Shooter, Action'),
    ('League of Legends', 125000, 'RPG, Strategy, MOBA'),
    ('Counter-Strike', 191000, 'FPS, Shooter, Action'),
    ('Minecraft', 36000, 'Simulation, Adventure Game')
]

cursor.executemany("""
    INSERT OR REPLACE INTO categories_data (category, viewers_count, tags)
    VALUES (?, ?, ?)
""", categories_data)

# Insert initial values into streamers_data table
streamers_data = [
    ('2025-02-22 16:35:53', 'Just Chatting', 'Marathon Batman Arkham Asylum', 'KaiCenat', 53900, 'English'),
    ('2025-02-22 16:36:15', 'VALORANT', 'High-Level Valorant Game', 'Shroud', 45000, 'FPS, Action'),
    ('2025-02-22 16:37:07', 'League of Legends', 'Epic LoL Match', 'Faker', 57000, 'RPG, Strategy'),
    ('2025-02-22 16:38:01', 'Counter-Strike', 'CS:GO Tournament', 'NiKo', 62000, 'FPS, Shooter'),
    ('2025-02-22 16:39:12', 'Minecraft', 'Survival Minecraft Stream', 'Dream', 46000, 'Simulation')
]

cursor.executemany("""
    INSERT OR REPLACE INTO streamers_data (timestamp, category, stream_title, channel, viewers_count, tags)
    VALUES (?, ?, ?, ?, ?, ?)
""", streamers_data)

# Commit changes and close the connection
conn.commit()
conn.close()

print("✅ Database and tables created successfully with initial data.")
