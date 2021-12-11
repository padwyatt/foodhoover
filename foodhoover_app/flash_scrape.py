from connections import get_bq_client, get_sql_client
from get_data import get_restaurant_details
import json
import time
import asyncio
from aiohttp import ClientSession
from google.cloud import bigquery
from sqlalchemy.sql import text
import gzip

def prepare_flash_scrape(bounds, place_ids):
    ##get the sectors in bounds
    geojson = {
        "type":"LineString",
        "coordinates": bounds
    }
    engine = get_sql_client('foodhoover_cache')
    conn = engine.connect()
    sql = text("SELECT sector FROM sectors \
            WHERE ST_Contains(ST_MakePolygon(ST_GeomFromGeoJSON(:geojson)), ST_SetSRID(geometry,4326));")
    result = conn.execute(sql, geojson=json.dumps(geojson))
    sectors_to_crawl = [r['sector'] for r in result]

    ##get the places in bounds
    sql = text("SELECT place_id FROM places \
        WHERE ST_Contains(ST_MakePolygon(ST_GeomFromGeoJSON(:geojson)), ST_SetSRID(ST_Point(place_lng, place_lat), 4326)) AND place_id IN :place_ids;")
    result = conn.execute(sql, geojson=json.dumps(geojson), place_ids=tuple(place_ids))
    places_to_crawl = [r['place_id'] for r in result]

    ##handle and errors and exit
    if len(sectors_to_crawl)>250:
        return {'status': 'Error', 'message': 'TOO MANY SECTORS'}
    elif len(sectors_to_crawl)<1:
        return {'status': 'Error', 'message': 'TOO FEW SECTORS'}
    elif len(places_to_crawl)<1:
        return {'status': 'Error', 'message': 'TOO FEW PLACES'}

    ##prepare the data for scraping
    place_details = get_restaurant_details(places_to_crawl)
    vendors_to_crawl = set()
    rx_uids = set()
    for place in place_details:
        for entity in place_details[place]['entities']:
            rx_uids.add(entity['rx_uid'])
            vendors_to_crawl.add(entity['vendor'])

    fetch_datas = get_flash_candidates(sectors_to_crawl, vendors_to_crawl)
    postcodes_to_scrape = [fetch_data['postcode'] for fetch_data in fetch_datas]

    return {'status' : 'OK', 'fetch_datas': fetch_datas, 'postcodes_to_scrape': postcodes_to_scrape, 'rx_uids':rx_uids}

def get_flash_candidates(sectors_to_crawl, vendors_to_crawl):

    engine = get_sql_client('foodhoover_cache')
    conn = engine.connect()

    sql = text("SELECT\
            postcode_sector as sector,\
            (array_agg(postcode_area ORDER BY RANDOM()))[1] as postcode_area,\
            (array_agg(postcode_district ORDER BY RANDOM()))[1] as postcode_district,\
            (array_agg(postcode ORDER BY RANDOM()))[1] as postcode,\
            (array_agg(longitude ORDER BY RANDOM()))[1] as longitude,\
            (array_agg(latitude ORDER BY RANDOM()))[1] as latitude,\
            (array_agg(postcode_geohash ORDER BY RANDOM()))[1] as postcode_geohash\
        FROM postcode_lookup\
        WHERE status<>'terminated' and latitude is not null and longitude is not null and postcode_geohash is not null and postcode_sector is not null and postcode_district is not null and postcode is not null and postcode_area is not null\
        AND postcode_sector IN :sectors\
        GROUP BY postcode_sector")

    sql = sql.bindparams(sectors=tuple(sectors_to_crawl))
    result = conn.execute(sql)

    fetch_datas = []
    for row in result:
        fetch_data = {
            'postcode':row['postcode'], 
            'lat':row['latitude'],
            'lng':row['longitude'],
            'geohash':row['postcode_geohash'],
            'vendors':vendors_to_crawl
        }
        fetch_datas.append(fetch_data)

    return fetch_datas

async def scrape_fetch_function(fetch_data, run_id):

        def make_scrape_uri(fetch_data, run_id):
            vendor_string = '&vendors='.join(fetch_data['vendors'])
            uri = "https://europe-west2-rooscrape.cloudfunctions.net/foodhoover_get?mode=scrape&postcode={postcode}&lat={lat}&lng={lng}&geohash={geohash}&vendors={vendor_string}&run_id={run_id}".format(
                postcode=fetch_data['postcode'],
                lat=fetch_data['lat'], 
                lng=fetch_data['lng'], 
                geohash=fetch_data['geohash'], 
                vendor_string=vendor_string,
                run_id = run_id
                )
        
            return uri

        uri = make_scrape_uri(fetch_data, run_id)

        try:
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy()) ##on windows, this is to stop an error that the loop isn't closed
        except:
            pass

        async with ClientSession() as session:
            try:
                async with session.get(uri) as response:
                    response = await response.read()
                    results = json.loads(gzip.decompress(response).decode('utf8'))
                    return results
            except Exception as e:
                results = [{
                    'open_set':[],
                    'scrape_status': [{
                        'run_id':run_id,
                        'scrape_time':time.time(),
                        'cx_postcode':fetch_data['postcode'],
                        'vendor': 'ALL',
                        'rx_open': None,
                        'status': 'CRASH: '+str(e)
                    }]
                }]
                print(str(e))
                return results

async def flash_url_batch(fetch_datas, rx_uids, fetch_function, batch_size, postcodes_to_scrape, run_id):
    output_postcodes = {rx_uid: [] for rx_uid in rx_uids}
    
    async def bound_fetch(sem, fetch_data, fetch_function, run_id):
        # Getter function with semaphore.
        async with sem:
            response = await fetch_function(fetch_data, run_id)    
            return response
    
    tasks = []
    sem = asyncio.Semaphore(batch_size)

    open_set = []
    scrape_status = []
    last_yield_time = time.time()

    for fetch_data in fetch_datas:
        task = asyncio.ensure_future(bound_fetch(sem, fetch_data, fetch_function, run_id))
        tasks.append(task)
    postcodes_scraped = set()
    for done_task in asyncio.as_completed(tasks):
        scrape_results =  await done_task
        print(scrape_results)
        for status in scrape_results['scrape_status']:
            scrape_status.append(status)
            postcodes_scraped.add(status['cx_postcode'])
        for rx in scrape_results['open_set']:
            open_set.append(rx)
            rx_uid = rx['rx_uid']
            if  rx_uid in rx_uids:
                postcode_set = set(output_postcodes[rx_uid])
                postcode_set.add(rx['cx_postcode'])
                output_postcodes[rx_uid] = list(postcode_set)
                
        output = []
        for rx_uid, postcodes_seen in output_postcodes.items():
            if postcodes_seen == []:
                postcodes_seen = ['None']
            row = {
                'rx_uid' : rx_uid,
                'sectors_seen' : postcodes_seen,
            }
            output.append(row)
        
        if ((time.time()-last_yield_time)>2) & (postcodes_seen != ['None']):
            last_yield_time = time.time()
            final_display = query_flash(json.dumps(output))

            flash_results = {
                'status': 'OK',
                'postcodes_scraped': list(postcodes_scraped),
                'postcodes_to_scrape' : postcodes_to_scrape,
                'flash_result': final_display
            }

            yield json.dumps(flash_results) + '\n'

    final_display = query_flash(json.dumps(output)) #last result
    flash_results = {
        'status': 'OK',
        'postcodes_scraped': list(postcodes_scraped),
        'postcodes_to_scrape' : postcodes_to_scrape,
        'flash_result': final_display
    }

    yield json.dumps(flash_results) + '\n'

    load_scrape(open_set, scrape_status)

def to_sync_generator(ait):
    loop = asyncio.new_event_loop()
    try:
        while True:
            try:
                coro = ait.__anext__()
                res = loop.run_until_complete(coro)
            except StopAsyncIteration:
                return
            else:
                yield res
    finally:
        coro = loop.shutdown_asyncgens()
        loop.run_until_complete(coro)

def query_flash(json):

    sql = text("\
        SELECT\
            final.place_id,\
            jsonb_build_object(\
                'place_id', final.place_id,\
                'place_name', max(final.place_name),\
                'place_lat', max(final.place_lat),\
                'place_lng', max(final.place_lng),\
                'entities', array_to_json(\
                    array_agg(\
                        jsonb_build_object('place_vendor_id',final.place_vendor_id,'rx_name',final.place_name,'vendor',final.vendor)\
                    )\
                )\
            ) as place_details,\
            jsonb_build_object(\
                'type',     'FeatureCollection',\
                'features', jsonb_agg(final.feature)\
            ) as geometry\
        FROM (\
            SELECT\
                places.place_id,\
                MAX(places.place_name) as place_name,\
                MAX(places.place_lng) as place_lng,\
                MAX(places.place_lat) as place_lat,\
                MAX(rx_ref.vendor) as vendor,\
                CONCAT(places.place_id,'-',rx_ref.vendor) as place_vendor_id,\
                jsonb_build_object(\
                        'type', 'Feature',\
                        'id', CONCAT(places.place_id,'-',rx_ref.vendor),\
                        'geometry', ST_AsGeoJSON(ST_CollectionExtract(ST_SIMPLIFY(ST_ForcePolygonCW(ST_UNION(sectors.geometry)),0.001),3))::jsonb,\
                        'properties', to_jsonb(\
                            jsonb_build_object(\
                                'place_vendor_id', CONCAT(places.place_id,'-',MAX(rx_ref.vendor)),\
                                'vendor', MAX(vendor),\
                                'delivery_area',  COALESCE(ST_AREA(ST_TRANSFORM(ST_SetSRID(ST_UNION(sectors.geometry),4326), 31467))/1000000,0),\
                                'delivery_population', COALESCE(SUM(sectors.population),0),\
                                'place_id', places.place_id\
                            )\
                        )\
                    ) as feature\
            FROM\
            (SELECT rx_uid, UNNEST(scrape.sectors_seen) as postcode_seen\
            FROM json_to_recordset(:json)\
            AS scrape (\"rx_uid\" text, \"sectors_seen\" text[])) scrape\
            LEFT JOIN postcode_lookup pc on pc.postcode=scrape.postcode_seen\
            LEFT JOIN sectors on sectors.sector = pc.postcode_sector\
            LEFT JOIN rx_ref on rx_ref.rx_uid = scrape.rx_uid\
            LEFT JOIN places on places.place_id = rx_ref.place_id\
            GROUP BY places.place_id, place_vendor_id) final\
        GROUP BY final.place_id\
    ")

    engine = get_sql_client('foodhoover_cache')
    conn = engine.connect()

    sql = sql.bindparams(json=json)

    result = conn.execute(sql)
    
    place_boundary = {}
    for row in result: 
        place_boundary[row['place_id']] = {
            'place_details':  row['place_details'],
            'place_map' : row['geometry']
            }

    return place_boundary

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
    client.load_table_from_json(open_set, "rooscrape.foodhoover_store.rx_cx_results_raw",job_config=job_config)

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
    client.load_table_from_json(scrape_status, "rooscrape.foodhoover_store.scrape_event",job_config=job_config)

    return len(open_set)
            