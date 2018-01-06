
import requests
import json
import pandas as pd
import time

datestr = time.strftime("%Y%m%d")

# create function to return .json results from carmax hidden API
def vroom(pagenum,per_page=25):
    options = dict(
        offset = pagenum*per_page-per_page,
        limit = per_page,
        )
    
#https://invsearch.vroomapi.com/v2/inventory?brand=vroom&sort=&offset=75&limit=25
    
    json_url = "https://invsearch.vroomapi.com/v2/inventory?                brand=vroom&                sort=&                offset={offset}&                limit={limit}".format(**options).replace(" ","")
      
    raw = requests.get(json_url).text
    data = json.loads(raw)
    
    return data

print("Beginning Vroom.com scrape...")

listings = vroom(1)['meta']['count'] # return results for first page
print("# Listings:",listings)

pages = listings//vroom(1)['meta']['limit']
print("# Pages:",pages)

cars = pd.DataFrame.from_dict(vroom(1)['data'][0]['attributes'], orient='index').T
cars = cars.iloc[0:0]
for page in range(0,pages): # pages
    print("Scraping page #",page)
    for listing in range(0,vroom(1)['meta']['limit']): # per page
        df = pd.DataFrame.from_dict(vroom(page+1)['data'][listing]['attributes'], orient='index').T
        cars = pd.concat([cars,df])
    print(cars.shape[0],'cars')

path = os.getcwd()

# pickle results
cars.to_pickle(path[0]+'/data/vroom/'+datestr+'.pkl')

