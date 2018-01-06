import requests
import json
import pandas as pd
from random import randint
import time
import timeit
import os

datestr = time.strftime("%Y%m%d")

# create function to return .json results from carmax hidden API
def carmax(pagenum,per_page=50):
    options = dict(
        page = pagenum,
        perpage = per_page,
        startindex = pagenum*per_page-per_page,
        zipcode = 10022,
        key = "adfb3ba2-b212-411e-89e1-35adab91b600")
    
# https://api.carmax.com/v1/api/vehicles?Distance=50&PerPage=20&SortKey=0&StartIndex=40&ExposedDimensions=249+250+1001+1000+265+999+772&ExposedCategories=249+250+1001+1000+265+999+772&Refinements=&Page=3&Zip=40003&platform=carmax.com&apikey=adfb3ba2-b212-411e-89e1-35adab91b600
    
    json_url = "https://api.carmax.com/v1/api/vehicles?                Distance=All&                PerPage={perpage}&                StartIndex={startindex}&                SortKey=0&                ExposedDimensions=249+250+1001+1000+265+999+772&                ExposedCategories=249+250+1001+1000+265+999+772&                Refinements=&                Page={page}&                Zip={zipcode}&                platform=carmax.com&                apikey={key}".format(**options).replace(" ","")
      
    raw = requests.get(json_url).text
    data = json.loads(raw)
    
    return data

print("Beginning CarMax.com scrape...")

listings = carmax(1)['ResultCount'] # return results for first page
print("# Listings:",listings)

pages = listings//50+2
print("# Pages:",pages)

# create cars dataframe to store scraped results
cars = pd.DataFrame(carmax(1)['Results'])

t0 = timeit.default_timer() # start timer
for i in range(1,pages):
    t1 = timeit.default_timer()
    
    df = pd.DataFrame(carmax(i)['Results'])
    cars = pd.concat([cars,df])
#     df.to_json('data/'+datestr+'_'+str(i).zfill(4)+'.json')   
#     time.sleep(randint(1,10)/10) # add random delay
    
    print("Page",i,"scraped:",round(timeit.default_timer()-t1,2),"sec. elapsed;",round(i/pages*100,2),"% complete...")  
print("Scrape complete;",round(timeit.default_timer() - t0,2),"sec. elapsed.")

path = os.getcwd()

# pickle results
cars.to_pickle(path+'/data/carmax/'+datestr+'.pkl')
