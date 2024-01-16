import subprocess
import datetime
import git
import time
import logging

from daily_scraping import *

try:
    daily_scraping()
    print("scraping & ETL done!")
except:
    print("Error while scraping!")

time.sleep(30)

### push updates to git
try:
    repo = git.Repo("/Users/jameslim/Downloads/projects/jellycat-scraping/")
    repo.git.add('-A')
    repo.git.commit('-m', f"scraped and updated new data at {datetime.datetime.now()}")
    origin_master = repo.remote(name='origin')
    origin_master.push()
except:
    print("Error while pushing code!")