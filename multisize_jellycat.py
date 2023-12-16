from jellycat_sizes import *
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

df_primary = pd.read_csv("./data/jellycat_with_primary.csv", index_col=False)
df_primary = df_primary.reset_index()[["jellycat_id", "jellycat_name", "link", "information"]]

def jellycat_sizes_by_id(df):
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        df_sizes = pd.DataFrame(columns =['jellycat_id','size','price', 'stock'])

        for index, row in df.iterrows():
            page = browser.new_page()
            stealth_sync(page)
            jellycat_id = row['jellycat_id']
            link = row['link']
            print(row['jellycat_id'], row['link'])
            page.goto(f'https://www.jellycat.com{link}')
            df_size = scrape_size_and_stock(jellycat_id, page)
            df_sizes = pd.concat([df_sizes, df_size])
            print(f"Index {index} Done :)")
            page.close()
        return df_sizes
            

# jellycat_id = 'a'
# link = '/us/blossom-cream-bunny-bl3cbn/'
# df = jellycat_sizes_by_id(jellycat_id, link)
# print(df)

df_sizes = jellycat_sizes_by_id(df_primary)
df_sizes.to_csv("./data/jellycat_sizes_with_primary.csv")