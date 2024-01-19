import datetime

from daily_scraping import *

try:
    daily_scraping()
    print("scraping & ETL done!")
    print(datetime.datetime.now())
except:
    print("Error while scraping!")
