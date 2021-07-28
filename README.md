<h1> Foodhoover </h1>

Project to extract and visualise public delivery coverage from the main UK food delivery services (UberEats, JustEat, Foodhub, Deliveroo)  
https://foodhoover.patrickwyatt.org/

## Population coverage of aggregators
What is the overall population coverage of each of the aggregators? How many restaurants do they have in a particular area? Zoom the map to see the data at postcode sector level (around 5000 people).  
https://foodhoover.patrickwyatt.org/aggregator

![Alt text](/foodhoover_app/static/info/aggregator.png?raw=true "Aggregator view")

## Delivery areas and population coverage of restaurants
Where do individial restaurants deliver? How do the delivery areas and population coverage of the different aggregators vary? Add restaurants to the map by searching by name.  
https://foodhoover.patrickwyatt.org/restaurant?place_id=ChIJZ1Qa0G8QdkgRaUG_c76GXiA

![Alt text](/foodhoover_app/static/info/restaurant.png?raw=true "Restaurant view")

It's also possible to do a real-time query for a small area! Click on the lightning symbol to start the process. This will take around 30 seconds.

![Alt text](/foodhoover_app/static/info/restaurant-flash.png?raw=true "Real time view")

## Delivery areas and population coverage of restaurant chains
For restaurant chains (i.e. Pizza Hut), we want to see the delivery coverage of all their restaurants taken together. Search by a keyword to find all the restaurants with matching names.  
https://foodhoover.patrickwyatt.org/chain?chain=German+Doner+Kebab

![Alt text](/foodhoover_app/static/info/chain.png?raw=true "Chains view")

## Limitations and things to know

* Data is queried by postcode, at the postcode that is closest to the centroid of each of 9,232 GB postcode sectors (Northern Ireland is not yet included)
* Full imports for all sectors run daily at 6PM UK time
* Often, the same restaurant will appear on multiple aggregators. It may also appear on the same aggregator under different names (i.e. if there are multiple brands operating from the same site). Foodhoover attempts to link together these entities, principally by associating them with a Google Places ID. This can occasionally go wrong!
* Restaurant counts are, following the logic above, counts of unique sites, not aggregator listings (which is a slightly higher number)
* JustEat's McDonald's restaurants do not appear in Foodhoover

## Technicalities and data

This project contains four modules
1. **Loader:** 
* Prepares the static data (postcode lookup and shape files)
* Uploads this data to BigQuery
2. **Func:**
* Cloud Function that scrapes data for a given postocde or lat-lng, and streams it to BigQuery
3. **Scrape:**
* Orchestrates the scrapes of vendor sites on a daily schedule by calling Func
* Batch processes to normalise and process data
* Export of aggregate data to Cloud SQL 
4. **App:**
* Visualises coverage and delivery areas
* Front end for triggering real-time scrapes

Static files (postocde lookup and shapefiles) which belong inside the Loader module can be downloaded from: 
https://storage.googleapis.com/foodhoover-static/
