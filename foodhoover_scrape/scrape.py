from connections import get_bq_client, get_bq_storage
import json
import time
import asyncio
from aiohttp import ClientSession
from google.cloud import bigquery

def get_scrape_candidates(vendors_to_crawl, postcode=None):

    client = get_bq_client()
    bqstorageclient =get_bq_storage()

    if postcode != None:
        postcode_clause ="AND postcode='"+postcode+"' "
    else:
        postcode_clause = ""

    sql = "SELECT \
            postcode_sector as sector,\
            array_agg(postcode_area ORDER BY RAND() LIMIT 1)[offset(0)] as postcode_area,\
            array_agg(postcode_district ORDER BY RAND() LIMIT 1)[offset(0)] as postcode_district,\
            array_agg(postcode ORDER BY RAND() LIMIT 1)[offset(0)] as postcode,\
            array_agg(longitude ORDER BY RAND() LIMIT 1)[offset(0)] as longitude,\
            array_agg(latitude ORDER BY RAND() LIMIT 1)[offset(0)] as latitude,\
            array_agg(postcode_geohash ORDER BY RAND() LIMIT 1)[offset(0)] as postcode_geohash\
        FROM rooscrape.foodhoover_store.postcode_lookup\
        WHERE latitude is not null and longitude is not null and postcode_geohash is not null and postcode_sector is not null and postcode_district is not null and postcode is not null and postcode_area is not null\
        "+postcode_clause+"\
        GROUP BY postcode_sector\
        ORDER BY rand()\
    "
    
    postcodes_to_crawl = (
        client.query(sql)
        .result()
        .to_dataframe(bqstorage_client=bqstorageclient)
    )

    fetch_datas = []
    for index, row in postcodes_to_crawl.iterrows():
        fetch_data = {
            'postcode':row['postcode'], 
            'lat':row['latitude'],
            'lng':row['longitude'],
            'geohash':row['postcode_geohash'],
            'vendors':vendors_to_crawl
        }
        fetch_datas.append(fetch_data)

    return fetch_datas

def bq_run_scrape_new(fetch_datas, run_id, mode=None):
    
    ###this is the async function called inside URL batch that gets the data, and returns results
    ###if falling back to old foodhoover (old_uri), then comment out the part that creates loads jobs here
    async def scrape_fetch_function(fetch_data):
        def make_scrape_uri(fetch_data):
            vendor_string = '&vendors='.join(fetch_data['vendors'])
            #old_uri = "https://europe-west2-rooscrape.cloudfunctions.net/foodhoover?mode=availability&postcode={postcode}&postcode_area={postcode}&lat={lat}}&lng={lng}&geohash={geohash}&vendors={vendor_string}&run_id={run_id}".format(
            uri = "https://europe-west2-rooscrape.cloudfunctions.net/foodhoover_get?mode=scrape&postcode={postcode}&lat={lat}&lng={lng}&geohash={geohash}&vendors={vendor_string}&run_id={run_id}".format(
                postcode=fetch_data['postcode'],
                lat=fetch_data['lat'], 
                lng=fetch_data['lng'], 
                geohash=fetch_data['geohash'], 
                vendor_string=vendor_string,
                run_id = run_id
                )
        
            return uri

        uri = make_scrape_uri(fetch_data)

        async with ClientSession() as session:
            try:
                async with session.get(uri) as response:
                    response = await response.read()
                    results = json.loads(response.decode('utf8'))
                    return results
            except Exception as e:
                results = {
                    'open_set':[],
                    'scrape_status': [{
                        'run_id':run_id,
                        'scrape_time':time.time(),
                        'cx_postcode':fetch_data['postcode'],
                        'vendor': 'ALL',
                        'rx_open': None,
                        'status': 'CRASH: '+str(e)
                    }]
                }
                print(str(e))
                return results
                
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy()) ##on windows, this is to stop an error that the loop isn't closed
    except:
        pass

    def load_scrape(open_set, scrape_status):

        #load the results to bigquery
        client = get_bq_client()
        #write the raw data
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("scrape_time", bigquery.enums.SqlTypeNames.TIMESTAMP),
                bigquery.SchemaField("vendor", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("cx_postcode", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("rx_postcode", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("rx_lat", bigquery.enums.SqlTypeNames.FLOAT),
                bigquery.SchemaField("rx_lng", bigquery.enums.SqlTypeNames.FLOAT),
                bigquery.SchemaField("rx_name", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("run_id", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("rx_slug", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("rx_menu", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("eta", bigquery.enums.SqlTypeNames.FLOAT),
                bigquery.SchemaField("fee", bigquery.enums.SqlTypeNames.FLOAT),
                bigquery.SchemaField("rx_meta", bigquery.enums.SqlTypeNames.STRING)
                ],
            write_disposition="WRITE_APPEND",
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        )
        job = client.load_table_from_json(open_set, "rooscrape.foodhoover_store.rx_cx_results_raw",job_config=job_config)
        job.result()

        #write the scrape_log
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("run_id", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("scrape_time", bigquery.enums.SqlTypeNames.TIMESTAMP),
                bigquery.SchemaField("cx_postcode", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("vendor", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("rx_open", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("status", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("payload_size", bigquery.enums.SqlTypeNames.INTEGER),
                ],
            write_disposition="WRITE_APPEND",
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        )
        job = client.load_table_from_json(scrape_status, "rooscrape.foodhoover_store.scrape_event",job_config=job_config)
        job.result()

        return len(open_set)
    
    total_records = 0
    chunk_size = 800
    results_stored = 0
    fetch_data_chunks = [fetch_datas[x:x+chunk_size] for x in range(0, len(fetch_datas), chunk_size)]
    for fetch_data in fetch_data_chunks:
        scrape_results = asyncio.run(url_batch(fetch_data, scrape_fetch_function, 30))
        open_set = []
        scrape_status = []
        for scrape_result in scrape_results:
            print(scrape_result)
            results_stored = results_stored+1
            for status in scrape_result['scrape_status']:
                scrape_status.append(status)
            for rx in scrape_result['open_set']:
                open_set.append(rx)
            
        yield json.dumps(results_stored)

        total_records += load_scrape(open_set, scrape_status)

    yield json.dumps(total_records)

async def url_batch(fetch_datas, fetch_function, batch_size):

    async def bound_fetch(sem, fetch_data, fetch_function):
        # Getter function with semaphore.
        async with sem:
            response = await fetch_function(fetch_data)      
            return response

    tasks = []
    # create instance of Semaphore
    sem = asyncio.Semaphore(batch_size)

    # Create client session that will ensure we dont open new connection
    # per each request.
    async with ClientSession() as session:
        for fetch_data in fetch_datas:
            # pass Semaphore and session to every GET request
            task = asyncio.ensure_future(bound_fetch(sem, fetch_data, fetch_function))
            tasks.append(task)

        responses = await asyncio.gather(*tasks)
        
    return responses