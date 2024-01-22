from datetime import date

from daily_scraping import *

try:
    daily_scraping()
    print("scraping & ETL done!")
    print(date.today())
except:
    print("Error while scraping!")