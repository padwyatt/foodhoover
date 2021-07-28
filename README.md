<h1> Foodhoover </h1>

Project to extract and visualise coverage data from food delivery services (UberEats, JustEat, Foodhub, Deliveroo)
http://rooscrape.nw.r.appspot.com/

## Population coverage of aggregators

![Alt text](/foodhoover_app/static/info/aggregator.png?raw=true "Aggregator view")

## Delivery areas and population coverage of restaurants

![Alt text](/foodhoover_app/static/info/restaurant.png?raw=true "Restaurant view")

It's also possible to do a real-time scrape for a small area! Click on the lightning symbol to start the process. This will take around 30 seconds.

![Alt text](/foodhoover_app/static/info/restaurant-flash.png?raw=true "Real time view")

## Delivery areas and population coverage of restaurant chains

![Alt text](/foodhoover_app/static/info/chain.png?raw=true "Chains view")


## Technicalities

This project contains four modules
1. **Loader:** 
* Prepares the static data (postcode lookup and shape files)
* Uploads this data to BigQuery and CloudSQL
2. **Func:**
* Cloud Function that scrapes data for a given postocde or lat-lng, and streams it to BigQuery
3. **Scrape:**
* Orchestrates the scrapes of vendor sites on a daily schedule by calling Func
* Batch processes to normalise and process data
* Export of data to Cloud SQL 
4. **App:**
* Visualises coverage and delivery areas
* Front end for triggering real-time scrapes

Static files (postocde lookup and shapefiles) which belong inside the Loader module can be downloaded from: 
https://storage.googleapis.com/foodhoover-static/

