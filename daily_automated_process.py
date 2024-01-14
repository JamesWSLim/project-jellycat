import subprocess
import datetime

from daily_scraping import *

### run docker compose
with open("/tmp/output.log", "a") as output:
    subprocess.call("docker compose up -d", shell=True, stdout=output, stderr=output)

### run daily scraping
daily_scraping()

### git commands to push delta lake data to github
subprocess.call(["git", "add", "."])
subprocess.call(["git", "commit", "-m", ""])

