from playwright.sync_api import Playwright, sync_playwright, expect
from playwright_stealth import stealth_sync

def scrape_sizes_and_prices(website):

    try:
        browser = playwright.chromium.launch(headless=False)
        page = browser.new_page()
        stealth_sync(page)
        page.goto(website)
        
        sizes = ["VERY BIG -", "REALLY BIG -", "HUGE -", "LARGE -", "MEDIUM -", "SMALL -", "TINY -", "ONE SIZE -"]
        size_list = []
        price_list = []

        for size in sizes:
            div = page.get_by_text(size)
            if div.all_text_contents() != []:
                div.click()
                
                size_list.append(size[:-2])

                price = page.get_by_text("USD").all_text_contents()
                price_list.append(price)
        
        print(size_list, price_list)

    finally:
        page.close()
        browser.close()

website = '/us/amore-dog-am2d/'

with sync_playwright() as playwright:
    scrape_sizes_and_prices(f'https://www.jellycat.com{website}')