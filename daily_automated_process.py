import subprocess
import datetime
import git
import time
import logging

from daily_scraping import *

### go to path and start virtualenv
subprocess.Popen('cd /Users/jameslim/Downloads/projects/jellycat-scraping/', shell=True)
subprocess.Popen('source jellycat-env/bin/activate', shell=True)

### run docker compose
try:
    with open("/tmp/output.log", "a") as output:
        subprocess.call("docker compose up -d", shell=True, stdout=output, stderr=output)
except:
    print("Error while starting docker!")

try:
    daily_scraping()
except:
    print("Error while scraping!")

try:
    with open("/tmp/output.log", "a") as output:
        subprocess.call("docker compose down", shell=True, stdout=output, stderr=output)
    time.sleep(10)
except:
    print("Error while closing docker!")

### push updates to git
try:
    repo = git.Repo("/Users/jameslim/Downloads/projects/jellycat-scraping/")
    repo.git.add('-A')
    repo.git.commit('-m', f"scraped and updated new data on {datetime.datetime.now()}")
    origin_master = repo.remote(name='origin')
    origin_master.push()
except:
    print("Error while pushing code!")