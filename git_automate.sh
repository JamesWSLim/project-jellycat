cd /Users/jameslim/Downloads/projects/jellycat-scraping
git add .
DATE=$(date)
git commit -m "daily data on $DATE"
git push origin master
osascript -e "display notification 'pushed to remote' with title 'SUCCESS'"