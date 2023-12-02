from playwright.sync_api import Playwright, sync_playwright, expect
from playwright_stealth import stealth_sync
from selectolax.parser import HTMLParser
import pandas as pd
from datetime import datetime
# import jellycat_sizes

def go_through_all_pages(page):
    for i in range(50):
        try:
            if page.get_by_text("LOAD PREVIOUS").is_visible(timeout=2000):
                page.get_by_text("LOAD PREVIOUS").click(timeout=2000)
        except:
            break

def run(playwright: Playwright) -> None:
    try:
        browser = playwright.chromium.launch(headless=False)
        page = browser.new_page()
        stealth_sync(page)
        page.goto('https://www.jellycat.com/us/all-animals/?sort=422&page=30')

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

        df = pd.DataFrame({'name': names, 'price': prices, 'information': information, 'link': links, 'image_link': images})
        
        # insert date created
        now = datetime.now()
        timestamp = now.strftime("%d/%m/%Y %H:%M:%S")
        df["date_created"] = timestamp
        print(df.head())

    finally:
        page.close()
        browser.close()

with sync_playwright() as playwright:
    run(playwright)