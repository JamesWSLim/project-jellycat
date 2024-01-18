import subprocess
import datetime
import time
import logging

from daily_scraping import *

try:
    daily_scraping()
    print("scraping & ETL done!")
except:
    print("Error while scraping!")

time.sleep(30)

