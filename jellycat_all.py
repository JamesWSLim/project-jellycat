from playwright.sync_api import Playwright, sync_playwright, expect
from playwright_stealth import stealth_sync
from selectolax.parser import HTMLParser
import pandas as pd
from datetime import datetime
import time
from jellycat_sizes import *

def go_through_all_pages(page):
    for i in range(50):
        try:
            if page.get_by_text("LOAD PREVIOUS").is_visible(timeout=2000):
                page.get_by_text("LOAD PREVIOUS").click(timeout=2000)
        except:
            break

def scrape_all_jellycats(page):
    time.sleep(3)

    if page.locator("#ajaxNewsletter").get_by_text("Close X").is_visible(timeout=10000):
        page.locator("#ajaxNewsletter").get_by_text("Close X").click(timeout=10000)
        
    go_through_all_pages(page)

    html = HTMLParser(page.content())

    information = []
    names = []
    prices = []
    links = []
    images = []
    categories = []

    webpages = html.css("div#productDataOnPagex > div > div > div")

    for i, webpage in enumerate(webpages):
        if ('data-nq-product-price' in webpage.child.attributes) & ('data-nq-product-category' in webpage.child.attributes):
            information.append(webpage.text(strip=False))
            price = webpage.child.attributes
            prices.append(price['data-nq-product-price'])
            link = webpage.child.child.child.attributes
            links.append(link['href'])
            img = webpage.child.child.child.child.attributes
            images.append(img['src'])
            name = webpage.child.child.child.child.attributes
            names.append(name['alt'])
            category = webpage.child.attributes
            categories.append(category['data-nq-product-category'])

    df = pd.DataFrame({'jellycat_name': names, 'price':prices, 'information': information, 'link': links, 'image_link': images})

    # insert date created
    now = datetime.now()
    timestamp = now.strftime("%d/%m/%Y %H:%M:%S")
    df["date_created"] = timestamp
    df.to_csv("./data/jellycat.csv", index=False)

    return df