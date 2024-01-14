from playwright.sync_api import Playwright, sync_playwright, expect
from playwright_stealth import stealth_sync
import pandas as pd
import time
from datetime import datetime

def click_newletter_popup(page):
    if page.locator("#ajaxNewsletter").get_by_text("Close X").is_visible(timeout=10000):
            page.locator("#ajaxNewsletter").get_by_text("Close X").click(timeout=10000)

def scrape_size_and_stock(jellycatid, jellycatname, page):
    size_measurement_list = []
    price_list = []
    stock_status_list = []
    sizes = []

    sizes_element_outer = page.locator(".f-13.nogaps")
    sizes_element = sizes_element_outer.locator(".pointer.width6.height6.inline-block.mr0-5.mb0-5.f-upper").all()

    ### for sizes more than 5, wait 3 seconds and close popup
    if len(sizes_element) > 5:
        time.sleep(3)
        click_newletter_popup(page)

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