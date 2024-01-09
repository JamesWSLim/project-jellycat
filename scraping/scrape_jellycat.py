from playwright.sync_api import Playwright, sync_playwright, expect
from playwright_stealth import stealth_sync
from selectolax.parser import HTMLParser
import pandas as pd
from datetime import datetime
import time
from scrape_size import *

def go_through_all_pages(page):
    click = 0
    for i in range(50):
        if click <= 29:
            try:
                if page.get_by_text("LOAD PREVIOUS").is_visible(timeout=2000):
                    page.get_by_text("LOAD PREVIOUS").click(timeout=2000)
                    time.sleep(0.5)
                    click += 1
            except:
                break

def scrape_all_jellycats(page):

    time.sleep(3)

    if page.locator("#ajaxNewsletter").get_by_text("Close X").is_visible(timeout=10000):
        page.locator("#ajaxNewsletter").get_by_text("Close X").click(timeout=10000)

    go_through_all_pages(page)

    html = HTMLParser(page.content())
    
    names = []
    links = []
    images = []
    categories = []

    webpages = html.css("div#productDataOnPagex > div > div > div")
    print(len(webpages))

    for i, webpage in enumerate(webpages):
        if ('data-nq-product-price' in webpage.child.attributes) & ('data-nq-product-category' in webpage.child.attributes):
            name = webpage.child.child.child.child.attributes
            names.append(name['alt'])
            category = webpage.child.attributes
            categories.append(category['data-nq-product-category'])
            link = webpage.child.child.child.attributes
            links.append(link['href'])
            img = webpage.child.child.child.child.attributes
            images.append(img['src'])

    df = pd.DataFrame({'jellycatname': names,
                        'category': categories,
                        'link': links, 
                        'imagelink': images
                        })

    # insert date created
    now = datetime.now()
    timestamp = now.strftime("%Y/%m/%d %H:%M:%S")
    df["datecreated"] = timestamp

    # rename duplicated jellycatname (add index after duplicated names)
    df.loc[df['jellycatname'].duplicated(keep=False), 'jellycatname'] = df['jellycatname'] + df.groupby('jellycatname').cumcount().map({0:' 1', 1:' 2', 2: ' 3'})

    return df