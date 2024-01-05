from playwright.sync_api import Playwright, sync_playwright, expect, TimeoutError as PlaywrightTimeoutError
from playwright_stealth import stealth_sync
from scrape_size import *
from scrape_jellycat import *
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from datetime import date
import requests

def scrape_main_page():
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=False)
        page = browser.new_page()
        stealth_sync(page)
        page.goto('https://www.jellycat.com/us/all-animals/?sort=422&page=30')
        df = scrape_all_jellycats(page)
        return df

def jellycat_sizes_by_id(df):
    ### run playwright
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=False)
        df_sizes = pd.DataFrame(columns =['jellycat_id','size','price','stock'])

        ### loop through jellycat_ids
        for index, row in df.iterrows():
            while True:
                try:
                    page = browser.new_page()
                    stealth_sync(page)
                    jellycat_id = row['jellycat_id']
                    link = row['link']

                    page.goto(f'https://www.jellycat.com{link}')
                    df_size = scrape_size_and_stock(jellycat_id, page)
                    df_sizes = pd.concat([df_sizes, df_size])
                    if index % 50 == 0:
                        print(f"{index} Done :)")
                    page.close()
                ### error handling    
                except PlaywrightTimeoutError:
                    print(row["link"])
                    continue
                break
        return df_sizes

### scrape main page
df = scrape_main_page()
df = df[["jellycat_name", "price", "information", "link", "image_link", "date_created"]]

### Connect to your postgres DB
conn = psycopg2.connect(
    host="localhost",
    database="jellycat_db",
    user="root",
    password="root")

### drop jellycat table if exist with cascade
sql = """DROP TABLE IF EXISTS jellycat CASCADE"""
cursor = conn.cursor()
cursor.execute(sql)
conn.commit()

### insert jellycat data to postgresql
engine = create_engine('postgresql://root:root@localhost:5432/jellycat_db')
df.to_sql('jellycat', engine, if_exists='append', index=False)

### Retrieve query results (to get primary keys)
sql = '''select * 
    from jellycat'''
df = pd.read_sql_query(sql, conn)
print(df.head(10))

### create a csv file with today's date for tracking
date_today = date.today()
df.to_csv(f"./data/jellycat_with_primary_{date_today}.csv", index=False)

### retrieve needed columns
df_primary = df.reset_index()[["jellycat_id", "jellycat_name", "link", "information"]]

### scrape jellycat sizes by jellycat_id
df_sizes = jellycat_sizes_by_id(df_primary)
### reset index
df_sizes.index = [x for x in range(1, len(df_sizes.values)+1)]

### change price column into float
df_sizes["price"] = df_sizes["price"].str.replace('$','')
df_sizes["price"] = df_sizes["price"].str.replace(' USD','')
df_sizes["price"] = df_sizes["price"].astype(float)
print(df_sizes.head(10))

### create a csv file with today's date for tracking
df_sizes.to_csv(f"./data/jellycat_sizes_with_primary_{date_today}.csv", index=True)

### drop size table if exist with cascade (dropping all the foreign tables)
sql = """DROP TABLE IF EXISTS size CASCADE"""
cursor = conn.cursor()
cursor.execute(sql)
conn.commit()

### insert sizes data into postgresql
df_sizes.to_sql('size', engine, if_exists='append', index=False)