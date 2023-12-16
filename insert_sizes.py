from jellycat_sizes import *

website = '/us/amore-corgi-am2cor/'

def scrape_main_page():
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=False)
        page = browser.new_page()
        stealth_sync(page)
        website = '/us/amore-corgi-am2cor/'
        page.goto('https://www.jellycat.com/us/all-animals/?sort=422&page=30')
        df = scrape_size_and_stock(page)
        return df
    
