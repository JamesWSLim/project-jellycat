from proxyscrape import create_collector, get_collector
import requests

collector = create_collector('my-collector', 'http')

# Retrieve any http proxy
proxy = collector.get_proxy()

proxy = f'{proxy.host}:{proxy.port}'
print(proxy)

try:
    r = requests.get('https://www.google.com/us/', proxies={'http':proxy, 'https':proxy}, timeout=3)
    print(r.json())
except:
    print("failed")