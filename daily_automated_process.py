import subprocess
import datetime
import git
import time
import logging

# from daily_scraping import *

# ### go to path and start virtualenv
# subprocess.Popen('cd /Users/jameslim/Downloads/projects/jellycat-scraping/', shell=True)
# subprocess.Popen('source jellycat-env/bin/activate', shell=True)

# try:
#     daily_scraping()
# except:
#     print("Error while scraping!")

### push updates to git
try:
    repo = git.Repo("/Users/jameslim/Downloads/projects/jellycat-scraping/")
    repo.git.add('-A')
    repo.git.commit('-m', f"scraped and updated new data on {datetime.datetime.now()}")
    origin_master = repo.remote(name='origin')
    origin_master.push()
except:
    print("Error while pushing code!")