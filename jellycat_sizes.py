from playwright.sync_api import Playwright, sync_playwright, expect
from playwright_stealth import stealth_sync
import pandas as pd
import time

def click_newletter_popup(page):
    if page.locator("#ajaxNewsletter").get_by_text("Close X").is_visible(timeout=10000):
            page.locator("#ajaxNewsletter").get_by_text("Close X").click(timeout=10000)

def scrape_size_and_stock(jellycat_id, page):
    size_measurement_list = []
    price_list = []
    stock_status_list = []
    sizes = []

    sizes_element_outer = page.locator(".f-13.nogaps")
    sizes_element = sizes_element_outer.locator(".pointer.width6.height6.inline-block.mr0-5.mb0-5.f-upper").all()
    for size in sizes_element:
        sizes.append(size.locator(".ptb1-5.plr0-5.align-center.f-capi").text_content())
    
    if len(sizes) > 3:
        time.sleep(2)
        click_newletter_popup(page)

    for i, size in enumerate(sizes):

        click_newletter_popup(page)

        div = sizes_element_outer.get_by_text(size)
        div.click()
        size_measurement_list.append(div.text_content())
        price = page.get_by_text("USD").first.text_content()
        price_list.append(price)
        stock = page.locator(".mt0-25").first.text_content()
        stock_status_list.append(stock)

    df = pd.DataFrame({'jellycat_id': jellycat_id, 'size': size_measurement_list, 'price': price_list, 'stock': stock_status_list})
    
    return df
