import git
import datetime

try:
    repo = git.Repo("/Users/jameslim/Downloads/projects/jellycat-scraping/")
    repo.git.add('-A')
    repo.git.commit('-m', f"scraped and updated new data{datetime.datetime.now()}")
    origin_master = repo.remote(name='origin/master')
    origin_master.push()
except:
    print("Error while pushing code!")