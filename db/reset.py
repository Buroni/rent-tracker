import sys
import sqlite3
import os

os.chdir(os.path.dirname(os.path.abspath(__file__)))

con = sqlite3.connect("uk-rent.db")
cur = con.cursor()

action = sys.argv[1]

def create_timeline_table(name):
    cur.execute(f"DROP TABLE IF EXISTS {name}")
    cur.execute(f"""
               CREATE TABLE IF NOT EXISTS {name}(
                   address TEXT,
                   postcode TEXT,
                   price_pcm REAL,
                   num_bedrooms INTEGER,
                   num_bathrooms INTEGER,
                   property_type TEXT,
                   scrape_date TEXT,
                   scrape_epoch REAL,
                   url TEXT,
                   id TEXT PRIMARY KEY
               )
       """)

if action == "rightmove_postcode_map":
    cur.execute("DROP TABLE IF EXISTS rightmove_postcode_map")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS rightmove_postcode_map(
            postcode TEXT PRIMARY KEY, 
            location_id TEXT
        )
    """)

elif action == "timeline":
    create_timeline_table("timeline")
    create_timeline_table("timeline_staging")
