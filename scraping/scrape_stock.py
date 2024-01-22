from playwright.sync_api import Playwright, sync_playwright, expect, TimeoutError as PlaywrightTimeoutError
from playwright_stealth import stealth_sync
import pandas as pd
import time
from datetime import date
import math

def click_newletter_popup(page):
    if page.locator("#ajaxNewsletter").get_by_text("Close X").is_visible(timeout=10000):
            page.locator("#ajaxNewsletter").get_by_text("Close X").click(timeout=10000)

def scrape_stock_count(jellycatsizeid, jellycatname, size, page):

    jellycatsizeid_list = []
    jellycatname_list = []
    size_list = []
    stock_count_list = []

    size_button = page.get_by_text(size)
    size_button.click()
    buy_me_button = page.get_by_role("button", name="Buy Me")
    buy_me_button.click()
    buy_me_popup = page.locator(".pad")
    check_out_button = buy_me_popup.get_by_text("Check Out")
    check_out_button.click()
    page.locator(".bg-xlight.bd-xlight.width2.align-center").fill("1000")
    page.keyboard.press("Enter")
    time.sleep(0.5)
    stock_count = page.locator(".header-itemcount").text_content()
    
    jellycatsizeid_list.append(jellycatsizeid)
    jellycatname_list.append(jellycatname)
    size_list.append(size)
    stock_count_list.append(stock_count)

    df = pd.DataFrame({"jellycatsizeid": jellycatsizeid_list,
                        "jellycatname": jellycatname_list, 
                        'size': size_list, 
                        'stockcount': stock_count_list})
    return df

def scrape_stock_count_by_sizes(df_sizes):

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=False)
        df_stocks = pd.DataFrame(columns =['jellycatid','jellycatname','size','stockcount'])

        for index, row in df_sizes.iterrows():
            jellycatsizeid = row['jellycatsizeid']
            jellycatname = row['jellycatname_x']
            size = row['size']
            height = row['height']
            width = row['width']
            if math.isnan(width):
                if not math.isnan(height):
                    size = f'{size} - H{int(height)}"'
            else:
                size = f'{size} - H{int(height)}" X W{int(width)}"'
            link = row['link']

            while True:
                try:
                    page = browser.new_page()
                    page.set_default_timeout(3000)
                    stealth_sync(page)
                    page.goto(f'https://www.jellycat.com{link}')
                    df_stock = scrape_stock_count(jellycatsizeid, jellycatname, size, page)
                    df_stocks = pd.concat([df_stocks, df_stock])
                    if index % 50 == 0:
                        print(f"stock {index} Done :)")
                    page.close()

                ### error handling    
                except PlaywrightTimeoutError:
                    print(row["link"])
                    page.close()
                    continue
                break
        browser.close()
        return df_stocks

df_jellycat = pd.read_csv("./data/jellycat_with_primary_2024-01-21.csv")
df_size = pd.read_csv("./data/jellycat_sizes_with_primary_2024-01-21.csv")
df_joined = pd.merge(df_jellycat, df_size, on="jellycatid")
df_joined = df_joined[df_joined["stock"]=="In Stock"]
df = scrape_stock_count_by_sizes(df_joined)
date_today = date.today()
df.to_csv(f"./data/jellycat_stock_{date_today}.csv", index=False)
