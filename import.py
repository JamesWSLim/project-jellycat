from playwright.sync_api import Playwright, sync_playwright, expect
from playwright_stealth import stealth_sync
from jellycat_sizes import *
from jellycat_all import *
import json
import psycopg2
import pandas as pd
from sqlalchemy import create_engine

def scrape_main_page():
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=False)
        page = browser.new_page()
        stealth_sync(page)
        page.goto('https://www.jellycat.com/us/all-animals/?sort=422&page=30')
        df = scrape_all_jellycats(page)
        return df

df = scrape_main_page()

# insert data to postgresql
df["date_created"] = pd.to_datetime(df["date_created"])
engine = create_engine('postgresql://root:root@localhost:5432/jellycat_db')
df.to_sql('jellycat', engine, if_exists='append', index=False)

# read data from postgresql for primary keys
# Connect to your postgres DB
conn = psycopg2.connect(
    host="localhost",
    database="jellycat_db",
    user="root",
    password="root")

# Retrieve query results
sql = 'select jellycat_id, jellycat_name, information, link, max(date_created) as "most_recent_date" from jellycat group by jellycat_id, jellycat_name'
df = pd.read_sql_query(sql, conn)
print(df)
df.to_csv("./data/jellycat_with_primary.csv")

df_primary = df.reset_index()[["jellycat_id", "jellycat_name", "link", "information"]]

def jellycat_sizes_by_id(df):
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        df_sizes = pd.DataFrame(columns =['jellycat_id','size','price', 'stock'])

        for index, row in df.iterrows():
            page = browser.new_page()
            stealth_sync(page)
            jellycat_id = row['jellycat_id']
            link = row['link']
            print(row['jellycat_id'], row['link'])
            page.goto(f'https://www.jellycat.com{link}')
            df_size = scrape_size_and_stock(jellycat_id, page)
            df_sizes = pd.concat([df_sizes, df_size])
            print(f"Index {index} Done :)")
            page.close()
        return df_sizes
            
# jellycat_id = 'a'
# link = '/us/blossom-cream-bunny-bl3cbn/'
# df = jellycat_sizes_by_id(jellycat_id, link)
# print(df)

df_sizes = jellycat_sizes_by_id(df_primary)
df_sizes.to_csv("jellycat_sizes_with_primary.csv")

