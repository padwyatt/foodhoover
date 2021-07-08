<h1> Foodhoover </h1>

Project to extract and visualise coverage data from food delivery services (UberEats, JustEat, Foodhub, Deliveroo)

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


