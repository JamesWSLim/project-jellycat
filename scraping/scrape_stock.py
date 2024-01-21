from playwright.sync_api import Playwright, sync_playwright, expect
from playwright_stealth import stealth_sync
import pandas as pd
import time
from datetime import datetime

def scrape_stock_count(jellycatid, jellycatname, size, page):
    size_measurement_list = []
    price_list = []
    stock_status_list = []
    sizes = []

    sizes_element_outer = page.locator(".f-13.nogaps")
    sizes_element = sizes_element_outer.locator(".pointer.width6.height6.inline-block.mr0-5.mb0-5.f-upper").all()

    for size in sizes_element:
        sizes.append(size.locator(".ptb1-5.plr0-5.align-center.f-capi").text_content())

    for i, size in enumerate(sizes):
        div = sizes_element_outer.get_by_text(size)
        div.click()
        size_measurement_list.append(div.text_content())
        price = page.get_by_text("USD").first.text_content()
        price_list.append(price)
        stock = page.locator(".mt0-25").first.text_content()
        stock_status_list.append(stock)

    df = pd.DataFrame({"jellycatid": jellycatid,
                        "jellycatname": jellycatname, 
                        'size': size_measurement_list, 
                        'price': price_list, 
                        'stock': stock_status_list})
    
    # insert date created
    now = datetime.now()
    timestamp = now.strftime("%Y/%m/%d %H:%M:%S")
    df["datecreated"] = timestamp
    return df


with sync_playwright() as playwright:
    browser = playwright.chromium.launch(headless=True)
    link = https://www.jellycat.com/us/bashful-bunny-with-daffodil-bb6df/
    scrape_stock_count(1, "Bashful Bunny With Daffodil", "SMALL", page)
    browser.close()