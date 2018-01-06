import requests
import json
import pandas as pd
import time
import os
import sys

datestr = time.strftime("%Y%m%d")

zipcodes = pd.read_csv('https://gist.githubusercontent.com/erichurst/7882666/raw/5bdc46db47d9515269ab12ed6fb2850377fd869e/US%2520Zip%2520Codes%2520from%25202013%2520Government%2520Data')
zipcodes.ZIP = [str(x).zfill(5) for x in zipcodes.ZIP]

def getCoords(zipcode):
    lat = zipcodes[zipcodes.ZIP==zipcode].LAT.values[0]
    lng = zipcodes[zipcodes.ZIP==zipcode].LNG.values[0]
    return [lat,lng]

# create function to return .json results from cargurus API
def cargurus(zipcode,dist='all',page_num=1):
    coords = getCoords(zipcode)
    options = dict(
        lat = coords[0],
        lng = coords[1],
        pagenum = page_num,
        dist = dist
        )
    
    json_url = 'https://www.cargurus.com/Cars/inventorylisting/ajaxFetchSubsetInventoryListing.action?    sourceContext=forSaleTab_false_0/    zip=&    address=&    latitude={lat}&    longitude={lng}&    distance={dist}&    selectedEntity=&    entitySelectingHelper.selectedEntity2=&    minPrice=&    maxPrice=&    minMileage=&    maxMileage=&    transmission=ANY&    bodyTypeGroup=&    serviceProvider=&    page={pagenum}&    filterBySourcesString=&    filterFeaturedBySourcesString=&    displayFeaturedListings=true&    searchSeoPageType=&    inventorySearchWidgetType=AUTO&    allYearsForTrimName=false&    daysOnMarketMin=&    daysOnMarketMax=&    vehicleDamageCategoriesRaw=&    minCo2Emission=&    maxCo2Emission=&    vatOnly=false&    minEngineDisplacement=&    maxEngineDisplacement=&    minMpg=&    maxMpg=&    isRecentSearchView=false'.format(**options).replace(' ','')
      
    raw = requests.get(json_url).text
    data = json.loads(raw)
    
    return data

def getData(min_zip='00000',max_zip='99999',pages=1,dist='all'):
    ziplist = zipcodes[(zipcodes.ZIP > min_zip) & (zipcodes.ZIP <= max_zip)].ZIP.values
    
    df = pd.DataFrame(cargurus('10543',dist=dist,page_num=1)['listings'])
    df = df.iloc[0:0]
    for zipcode in ziplist: #zipcodes.ZIP.values:
        for i in range(0,pages):
            print("Zipcode: "+zipcode+'; Page #'+str(i+1))
            df = pd.concat([df,pd.DataFrame(cargurus(zipcode,dist=dist,page_num=1+i)['listings'])])
            # COUNT ADDITIONAL NEW CARS PER PAGE; ITERATE UNTIL NO NEW CARS
        df.drop_duplicates(inplace=True,subset=['vehicleIdentifier'])
        print(df.shape[0],'cars scraped')
    
    path = os.getcwd()
    df.to_pickle(path+'/data/cargurus/'+datestr+'_'+min_zip+'_'+max_zip+'_'+str(pages)+'_'+str(dist)+'dist'+'.pkl')
        
    return df

print("Beginning CarGurus.com scrape...")

getData(min_zip=sys.argv[1],max_zip=sys.argv[2],pages=3,dist='10')
