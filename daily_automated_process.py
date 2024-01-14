import subprocess
import datetime
import git

from daily_scraping import *
subprocess.Popen('cd /Users/jameslim/Downloads/projects/jellycat-scraping/', shell=True)
subprocess.Popen('source jellycat-env/bin/activate', shell=True)

### run docker compose
with open("/tmp/output.log", "a") as output:
    subprocess.call("docker compose up -d", shell=True, stdout=output, stderr=output)

daily_scraping()

with open("/tmp/output.log", "a") as output:
    subprocess.call("docker compose down", shell=True, stdout=output, stderr=output)

### push updates to git
try:
    repo = git.Repo("/Users/jameslim/Downloads/projects/jellycat-scraping/")
    repo.git.add('-A')
    repo.git.commit('-m', f"scraped and updated new data {datetime.datetime.now()}")
    origin_master = repo.remote(name='origin')
    origin_master.push()
except:
    print("Error while pushing code!")

