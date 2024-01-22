from playwright.sync_api import Playwright, sync_playwright, expect, TimeoutError as PlaywrightTimeoutError
from playwright_stealth import stealth_sync
import pandas as pd
import time
from datetime import date
import math

def click_newletter_popup(page):
    if page.locator("#ajaxNewsletter").get_by_text("Close X").is_visible(timeout=10000):
            page.locator("#ajaxNewsletter").get_by_text("Close X").click(timeout=10000)

def scrape_stock_count(jellycatsizeid, size, page):

    jellycatsizeid_list = []
    size_list = []
    stock_count_list = []

    size_button = page.get_by_text(size)
    size_button.click()
    buy_me_button = page.get_by_role("button", name="Buy Me")
    click_newletter_popup(page)
    buy_me_button.click()
    buy_me_popup = page.locator(".pad")
    check_out_button = buy_me_popup.get_by_text("Check Out")
    check_out_button.click()
    page.locator(".bg-xlight.bd-xlight.width2.align-center").fill("1000")
    page.keyboard.press("Enter")
    time.sleep(0.5)
    stock_count = page.locator(".header-itemcount").text_content()
    
    jellycatsizeid_list.append(jellycatsizeid)
    size_list.append(size)
    stock_count_list.append(stock_count)

    df = pd.DataFrame({"jellycatsizeid": jellycatsizeid_list,
                        'size': size_list, 
                        'stockcount': stock_count_list})
    return df
