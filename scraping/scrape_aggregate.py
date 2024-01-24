from playwright.sync_api import Playwright, sync_playwright, expect, TimeoutError as PlaywrightTimeoutError
from playwright_stealth import stealth_sync
from scraping.scrape_size import *
from scraping.scrape_jellycat import *
from scraping.scrape_stock import *
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
        browser.close()
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
                    page.set_default_timeout(3000)
                    stealth_sync(page)
                    jellycatid = row['jellycatid']
                    jellycatname = row['jellycatname']
                    link = row['link']

                    page.goto(f'https://www.jellycat.com{link}')
                    df_size = scrape_size_and_stock(jellycatid, jellycatname, page)
                    df_sizes = pd.concat([df_sizes, df_size])
                    if index % 50 == 0:
                        print(f"size {index} Done :)")
                    page.close()
                ### error handling    
                except PlaywrightTimeoutError:
                    print(f'https://www.jellycat.com{link}')
                    page.close()
                    continue
                break
        browser.close()
        return df_sizes

def scrape_stock_count_by_sizes(df_sizes):

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        df_stocks = pd.DataFrame(columns =['jellycatsizeid','stockcount'])

        for index, row in df_sizes.iterrows():
            jellycatsizeid = row['jellycatsizeid']
            size = row['size']
            height = row['height']
            width = row['width']
            
            if math.isnan(width):
                if not math.isnan(height):
                    size = f'{size} - H{int(height)}"'
            else:
                size = f'{size} - H{int(height)}" X W{int(width)}"'
            link = row['link']

            i=0
            while True:
                ### if loop more than 10 times, assign stock as 0. Some jellycats might sold out during the process
                if i < 10:
                    try:
                        page = browser.new_page()
                        page.set_default_timeout(3000)
                        stealth_sync(page)
                        page.goto(f'https://www.jellycat.com{link}')
                        df_stock = scrape_stock_count(jellycatsizeid, size, page)
                        df_stocks = pd.concat([df_stocks, df_stock])
                        if index % 50 == 0:
                            print(f"stock {index} Done :)")
                        page.close()

                    ### error handling    
                    except PlaywrightTimeoutError:
                        print(f'https://www.jellycat.com{link}')
                        page.close()
                        i+=1
                        continue
                else:
                    df_stock = pd.DataFrame({"jellycatsizeid": [jellycatsizeid],
                        'stockcount': [0]})
                    df_stocks = pd.concat([df_stocks, df_stock])
                break
        browser.close()
        return df_stocks
    
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