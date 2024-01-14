from playwright.sync_api import Playwright, sync_playwright, expect, TimeoutError as PlaywrightTimeoutError
from playwright_stealth import stealth_sync
from scraping.scrape_size import *
from scraping.scrape_jellycat import *
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from datetime import date

def scrape_main_page():
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page()
        stealth_sync(page)
        page.goto('https://www.jellycat.com/us/all-animals/?sort=422&page=30')
        df = scrape_all_jellycats(page)
        return df

def jellycat_sizes_by_id(df):
    ### run playwright
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        df_sizes = pd.DataFrame(columns =['jellycatid','jellycatname','size','price','stock'])

        ### loop through jellycat_ids
        for index, row in df.iterrows():
            while True:
                try:
                    page = browser.new_page()
                    stealth_sync(page)
                    jellycatid = row['jellycatid']
                    jellycatname = row['jellycatname']
                    link = row['link']

                    page.goto(f'https://www.jellycat.com{link}')
                    df_size = scrape_size_and_stock(jellycatid, jellycatname, page)
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
    
def data_cleaning(df):
    ### reset index
    df.index = [x for x in range(1, len(df.values)+1)]
    ### change price column into float
    df["price"] = df["price"].str.replace('$','')
    df["price"] = df["price"].str.replace(' USD','')
    df["price"] = df["price"].astype(float)
    ### split size and measurement
    df[["size", "measurement"]] = df["size"].str.split(' - ', n=1, expand=True)
    ### split height and width
    df[["height", "width"]] = df["measurement"].str.split(' X ', n=1, expand=True)
    ### clean height and width
    df["height"] = df["height"].str.replace("H", "").str.replace("\"", "")
    df["width"] = df["width"].str.replace("W", "").str.replace("\"", "")
    df = df[["jellycatid","jellycatname","size","price","stock","datecreated","height","width"]]
    return df

