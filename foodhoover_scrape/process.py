import time
import urllib
import json
from datetime import timedelta

import asyncio
from aiohttp import ClientSession
from sqlalchemy.sql import text

from flask import jsonify
from connections import get_sql_client, get_bq_client, get_bq_storage, get_api_client, get_gcs_client

from google.cloud import bigquery

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

def bq_post_process(steps, run_id):
    results = []
    if steps == ['ALL']:
        steps = ['INSERT','UPDATE-ROO','UPDATE-UE','MISSING-POSTCODES','UPDATE-GEOS','GET-PLACES','PROC-PLACES','CREATE-PLACES','AGG-RESULTS-DISTRICT','AGG-RESULTS-SECTOR','AGG-RESULTS-COUNTRY-POP','RX-RESULTS-FAST','AGG-RX-CX','EXPORT-PLACES','EXPORT-RX-REF','EXPORT-AGG-SECTOR-RUN','EXPORT-AGG-DISTRICT-RUN','EXPORT-AGG-COUNTRY-RUN-POP','EXPORT-AGG-RX-CX']
    for step in steps:
        if step == 'INSERT':
            result = step+": "+str(bq_insert_new_rx(run_id))
            results.append(result)
            yield result
        elif step == 'UPDATE-ROO': 
            result = step+": "+str(bq_crawl_roo(run_id))
            results.append(result)
            yield result
        elif step == 'UPDATE-UE':
            result = step+": "+str(bq_crawl_ue(run_id))
            results.append(result)
            yield result
        elif step == 'MISSING-POSTCODES':
            result = step+": "+str(bq_update_missing_postcodes(run_id))
            results.append(result)
            yield result        
        elif step == 'UPDATE-GEOS':
            result = step+": "+str(bq_update_geos(run_id))
            results.append(result)
            yield result
        elif step == 'GET-PLACES':
            result = step+": "+str(bq_get_places(run_id))
            results.append(result)
            yield result
        elif step == 'PROC-PLACES':
            result = step+": "+str(bq_places_proc(run_id))
            results.append(result)
            yield result
        elif step == 'CREATE-PLACES':
            result = step+": "+str(bq_places_table(run_id))
            results.append(result)  
            yield result 
        elif step == 'AGG-RESULTS-DISTRICT':
            result = step+": "+str(bq_agg_results_district(run_id))
            results.append(result)
            yield result
        elif step == 'AGG-RESULTS-SECTOR':
            result = step+": "+str(bq_agg_results_sector(run_id))
            results.append(result)
            yield result
        elif step == 'RX-RESULTS-FAST':
            result = step+": "+str(bq_agg_rx_results_fast(run_id))
            results.append(result)
            yield result
        elif step == 'AGG-RESULTS-COUNTRY-POP':
            result = step+": "+str(bq_agg_results_country_pop(run_id))
            results.append(result)
            yield result
        elif step == 'AGG-RX-CX':
            result = step+": "+str(bq_agg_rx_cx(run_id))
            results.append(result)
            yield result
        elif step == 'EXPORT-PLACES':
            result = step+": "+str(bq_export_places(run_id))
            results.append(result)   
            yield result 
        elif step == 'EXPORT-RX-CX-RESULTS':
            result = step+": "+str(bq_export_rx_cx_results(run_id))
            results.append(result)
            yield result
        elif step == 'EXPORT-RX-REF':
            result = step+": "+str(bq_export_rx_ref(run_id))
            results.append(result)
            yield result
        elif step == 'EXPORT-AGG-SECTOR-RUN':
            result = step+": "+str(bq_export_agg_sector_run(run_id))
            results.append(result)
            yield result
        elif step == 'EXPORT-AGG-DISTRICT-RUN':
            result = step+": "+str(bq_export_agg_district_run(run_id))
            results.append(result)
            yield result
        elif step == 'EXPORT-AGG-COUNTRY-RUN':
            result = step+": "+str(bq_export_agg_country_run(run_id))
            results.append(result)
            yield result
        elif step == 'EXPORT-AGG-COUNTRY-RUN-POP':
            result = step+": "+str(bq_export_agg_country_run_pop(run_id))
            results.append(result)
            yield result
        elif step == 'EXPORT-AGG-RX-CX':
            result = step+": "+str(bq_export_agg_rx_cx(run_id))
            results.append(result)
            yield result
        else:
            result = step+ ": Step not found"
            results.append(result)
            yield result
    
    yield "DONE PROCESS"

def bq_step_logger(run_id, step,status, result):

    results = [{'run_id':run_id, 'step':step, 'status':status, 'result':result, 'scrape_time':'AUTO'}]

    client = get_bq_client()
    dataset = client.dataset('foodhoover_store')
    table_ref = dataset.table('scrape_log')
    table = client.get_table(table_ref)  # API call
    client.insert_rows_json(table, results)

    return "logged"

def bq_insert_new_rx(run_id):
    try:
        client = get_bq_client()
        table_id = 'rooscrape.foodhoover_store.rx_ref'

        table = client.get_table(table_id)
        num_rows_begin = table.num_rows

        if run_id=='full': ## this is the tag we to remove the where clause and compute for all runids
            where_clause = ""
            sql = "DELETE FROM "+table_id+" WHERE 1=1" ##empty the current table
            query_job = client.query(sql)  # API request
            query_job.result()
        else:
            where_clause = "AND run_id = '"+run_id+"'"

        sql = " \
        MERGE "+table_id+" ref \
        USING \
            (select CONCAT(rx_slug,'-',vendor) as rx_uid, max(rx_slug) as rx_slug, max(vendor) as vendor, max(rx_name) as rx_name,max(rx_postcode) as rx_postcode, \
            max(rx_lat) as rx_lat, max(rx_lng) as rx_lng, max(rx_menu) as rx_menu, max(rx_meta) as rx_meta \
            FROM rooscrape.foodhoover_store.rx_cx_results_raw \
            WHERE rx_slug is not null \
            "+ where_clause + "\
            AND vendor is not null group by rx_uid) results \
        ON ref.rx_uid = results.rx_uid \
        WHEN MATCHED THEN \
        UPDATE SET ref.rx_slug = coalesce(results.rx_slug,ref.rx_slug), ref.vendor = coalesce(results.vendor,ref.vendor), ref.rx_name = coalesce(results.rx_name,ref.rx_name), ref.rx_postcode=coalesce(results.rx_postcode,ref.rx_postcode), ref.rx_lat=coalesce(results.rx_lat,ref.rx_lat), ref.rx_lng=coalesce(results.rx_lng,ref.rx_lng),ref.rx_menu=coalesce(results.rx_menu,ref.rx_menu),ref.rx_meta=coalesce(results.rx_meta,ref.rx_meta) \
        WHEN NOT MATCHED THEN \
        INSERT (rx_uid, rx_slug, vendor, rx_name, rx_postcode, rx_lat, rx_lng, rx_menu, rx_meta) VALUES(results.rx_uid, results.rx_slug, results.vendor, results.rx_name, results.rx_postcode, results.rx_lat,results.rx_lng, results.rx_menu, results.rx_meta) \
        "
        query_job = client.query(sql)  # API request
        rows = query_job.result()

        table = client.get_table(table_id)
        num_rows_added = table.num_rows - num_rows_begin

        bq_step_logger(run_id, 'INSERT', 'SUCESS', num_rows_added)

        return num_rows_added
    except Exception as e:
        bq_step_logger(run_id, 'INSERT', 'FAIL', str(e))
        return e    

def bq_update_missing_postcodes(run_id):
    try:
        client = get_bq_client()
        table_id = 'rooscrape.foodhoover_store.rx_ref'

        sql = " \
        MERGE "+table_id+" ref \
        USING \
        (SELECT a.rx_slug, a.vendor, string_agg(b.postcode ORDER BY ST_DISTANCE(a.rx_point, b.postcode_point) ASC LIMIT 1) as closest_postcode \
        FROM (SELECT rx_slug, vendor, ST_GEOGPOINT(rx_lng,rx_lat) as rx_point from rooscrape.foodhoover_store.rx_ref WHERE rx_postcode IS null AND (rx_lat IS NOT null AND rx_lng is NOT null)) a, rooscrape.foodhoover_store.postcode_lookup b \
        WHERE ST_DISTANCE(a.rx_point, b.postcode_point)<5000 \
        GROUP BY a.rx_slug, a.vendor) locations \
        ON locations.rx_slug=ref.rx_slug and locations.vendor=ref.vendor  \
        WHEN MATCHED THEN \
        UPDATE SET ref.rx_postcode=locations.closest_postcode"

        query_job = client.query(sql)
        query_job.result() 
    
        num_rows_added = query_job.num_dml_affected_rows
        bq_step_logger(run_id, 'MISSING-POSTCODES', 'SUCESS', num_rows_added)
        return "Done UE"
    except Exception as e:
        bq_step_logger(run_id, 'MISSING-POSTCODES', 'FAIL', str(e))
        return e   

def bq_crawl_roo(run_id):
    try:
        client = get_bq_client()
        ###get the roo ones missing a postcode from rx_ref, but have an id
        missing_sql = "select rx_meta from rooscrape.foodhoover_store.rx_ref where vendor='ROO' and rx_postcode is null and rx_meta is not null limit 1000"
        query_job = client.query(missing_sql)

        fetch_datas = []
        for row in query_job.result():
            fetch_datas.append(row['rx_meta'])

        num_roo_to_crawl = len(fetch_datas)

        async def roo_fetch_function(fetch_data):
            uri = "https://europe-west2-rooscrape.cloudfunctions.net/foodhoover_get?mode=roo&rx_id="+fetch_data

            async with ClientSession() as session:
                try:
                    async with session.get(uri) as response:
                        response = await response.read()
                        results = json.loads(response.decode('utf8'))
                        return results
                except Exception as e:
                        print(str(e))

        chunk_size = 10
        fetch_data_chunks = ['&rx_id='.join(fetch_datas[x:x+chunk_size]) for x in range(0, len(fetch_datas), chunk_size)]
        scrape_results = asyncio.run(url_batch(fetch_data_chunks, roo_fetch_function, 30))

        roo_rxs = []
        for result_batch in scrape_results:
            for roo_rx in result_batch:
                if roo_rx['scrape_status']=='OK':
                    roo_rxs.append(roo_rx['roo_details'])

        client = bigquery.Client.from_service_account_json('rooscrape-gbq.json')   
        #write the roo_data
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("scrape_time", bigquery.enums.SqlTypeNames.TIMESTAMP),
                bigquery.SchemaField("rx_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("rx_name", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("rx_slug", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("rx_neighbourhood", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("rx_city", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("rx_lat", bigquery.enums.SqlTypeNames.FLOAT),
                bigquery.SchemaField("rx_lng", bigquery.enums.SqlTypeNames.FLOAT),
                bigquery.SchemaField("rx_prep_time", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("rx_address", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("rx_postcode", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("rx_fulfillment_type", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("rx_menu_page", bigquery.enums.SqlTypeNames.STRING),
                ],
            write_disposition="WRITE_APPEND",
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        )
        job = client.load_table_from_json(roo_rxs, "rooscrape.foodhoover_store.roo_cache",job_config=job_config)
        job.result()

        num_roo_cache_added = len(roo_rxs)

        ##query to merge rx_ref with the most recent row in roo_cache (we run this twice, before and after the scrape)
        sql = " \
            MERGE rooscrape.foodhoover_store.rx_ref ref \
            USING \
            (SELECT\
                rx_slug,\
                ARRAY_AGG(rx_name ORDER BY scrape_time DESC LIMIT 1)[offset(0)] as rx_name,\
                ARRAY_AGG(rx_postcode ORDER BY scrape_time DESC LIMIT 1)[offset(0)] as rx_postcode,\
                ARRAY_AGG(rx_lat ORDER BY scrape_time DESC LIMIT 1)[offset(0)] as rx_lat,\
                ARRAY_AGG(rx_lng ORDER BY scrape_time DESC LIMIT 1)[offset(0)] as rx_lng,\
            FROM rooscrape.foodhoover_store.roo_cache \
            GROUP BY rx_slug) roo_cache \
            ON ref.rx_slug = roo_cache.rx_slug and ref.vendor='ROO' \
            WHEN MATCHED THEN \
            UPDATE SET\
                ref.rx_postcode=COALESCE(roo_cache.rx_postcode, ref.rx_postcode),\
                ref.rx_name=COALESCE(roo_cache.rx_name,ref.rx_name),\
                ref.rx_lat=COALESCE(roo_cache.rx_lat,ref.rx_lat),\
                ref.rx_lng=COALESCE(roo_cache.rx_lng,ref.rx_lng)\
            "
        query_job = client.query(sql) 
        query_job.result()

        num_roo_rx_updated = query_job.num_dml_affected_rows

        status = {
            'Rx crawled' : num_roo_to_crawl ,
            'Rx added to cache' : num_roo_cache_added,
            'Rx updated' : num_roo_rx_updated
        }

        bq_step_logger(run_id, 'UPDATE-ROO', 'SUCESS',num_roo_cache_added)
        return json.dumps(status)
    except Exception as e:
        bq_step_logger(run_id, 'UPDATE-ROO', 'FAIL', str(e))
        return e        

def bq_crawl_ue(run_id):
    try:
        client = get_bq_client()
        ###get the ue ones missing a postcode from rx_ref, but have an id
        missing_sql = "select rx_slug from rooscrape.foodhoover_store.rx_ref where vendor='UE' and rx_postcode is null and rx_slug is not null limit 1000"
        query_job = client.query(missing_sql)

        fetch_datas = []
        for row in query_job.result():
            fetch_datas.append(row['rx_slug'])

        num_ue_to_crawl = len(fetch_datas)

        async def ue_fetch_function(fetch_data):
            uri = "https://europe-west2-rooscrape.cloudfunctions.net/foodhoover_get?mode=ue&rx_id="+fetch_data

            async with ClientSession() as session:
                try:
                    async with session.get(uri) as response:
                        response = await response.read()
                        results = json.loads(response.decode('utf8'))
                        return results
                except Exception as e:
                        print(str(e))

        chunk_size = 10
        fetch_data_chunks = ['&rx_id='.join(fetch_datas[x:x+chunk_size]) for x in range(0, len(fetch_datas), chunk_size)]
        scrape_results = asyncio.run(url_batch(fetch_data_chunks, ue_fetch_function, 30))

        ue_rxs = []
        for result_batch in scrape_results:
            for ue_rx in result_batch:
                if ue_rx['scrape_status']=='OK':
                    ue_rxs.append(ue_rx['ue_details'])

        client = bigquery.Client.from_service_account_json('rooscrape-gbq.json')   
        #write the ue_data
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("scrape_time", bigquery.enums.SqlTypeNames.TIMESTAMP),
                bigquery.SchemaField("rx_id", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("rx_name", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("rx_slug", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("rx_neighbourhood", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("rx_city", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("rx_lat", bigquery.enums.SqlTypeNames.FLOAT),
                bigquery.SchemaField("rx_lng", bigquery.enums.SqlTypeNames.FLOAT),
                bigquery.SchemaField("rx_prep_time", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("rx_address", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("rx_postcode", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("rx_fulfillment_type", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("rx_menu_page", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("rx_blob", bigquery.enums.SqlTypeNames.STRING)
                ],
            write_disposition="WRITE_APPEND",
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        )
        job = client.load_table_from_json(ue_rxs, "rooscrape.foodhoover_store.ue_cache",job_config=job_config)
        job.result()

        num_ue_cache_added = len(ue_rxs)

        ##query to merge rx_ref with the most recent row in roo_cache (we run this twice, before and after the scrape)
        sql = " \
            MERGE rooscrape.foodhoover_store.rx_ref ref \
            USING \
            (SELECT\
                rx_slug,\
                ARRAY_AGG(rx_name ORDER BY scrape_time DESC LIMIT 1)[offset(0)] as rx_name,\
                ARRAY_AGG(rx_postcode ORDER BY scrape_time DESC LIMIT 1)[offset(0)] as rx_postcode,\
                ARRAY_AGG(rx_lat ORDER BY scrape_time DESC LIMIT 1)[offset(0)] as rx_lat,\
                ARRAY_AGG(rx_lng ORDER BY scrape_time DESC LIMIT 1)[offset(0)] as rx_lng,\
            FROM rooscrape.foodhoover_store.ue_cache \
            GROUP BY rx_slug) ue_cache \
            ON ref.rx_slug = ue_cache.rx_slug and ref.vendor='UE' \
            WHEN MATCHED THEN \
            UPDATE SET\
                ref.rx_postcode=COALESCE(ue_cache.rx_postcode, ref.rx_postcode),\
                ref.rx_name=COALESCE(ue_cache.rx_name,ref.rx_name),\
                ref.rx_lat=COALESCE(ue_cache.rx_lat,ref.rx_lat),\
                ref.rx_lng=COALESCE(ue_cache.rx_lng,ref.rx_lng)\
            "
        query_job = client.query(sql) 
        query_job.result()

        num_ue_rx_updated = query_job.num_dml_affected_rows

        status = {
            'Rx crawled' : num_ue_to_crawl ,
            'Rx added to cache' : num_ue_cache_added,
            'Rx updated' : num_ue_rx_updated
        }

        bq_step_logger(run_id, 'UPDATE-UE', 'SUCESS',num_ue_cache_added)
        return json.dumps(status)
    except Exception as e:
        bq_step_logger(run_id, 'UPDATE-UE', 'FAIL', str(e))
        return e   


def bq_update_geos(run_id):
    try:
        client = get_bq_client()
        table_id = 'rooscrape.foodhoover_store.rx_ref'

        sql = " \
        MERGE "+table_id+" ref \
        USING rooscrape.foodhoover_store.postcode_lookup postcodes \
        ON postcodes.postcode=ref.rx_postcode \
        WHEN MATCHED THEN UPDATE \
        SET ref.rx_district=postcodes.postcode_district, ref.rx_sector=postcodes.postcode_sector, ref.rx_lat = COALESCE(ref.rx_lat,postcodes.latitude), ref.rx_lng = COALESCE(ref.rx_lng, postcodes.longitude)"

        query_job = client.query(sql) 
        query_job.result()
    
        num_rows_added = query_job.num_dml_affected_rows
        bq_step_logger(run_id, 'UPDATE-GEOS', 'SUCESS', num_rows_added)
        return str(num_rows_added)
    except Exception as e:
        bq_step_logger(run_id, 'UPDATE-GEOS', 'FAIL', str(e))
        return e   

def write_bq(results, table_name):
    client = bigquery.Client.from_service_account_json('rooscrape-gbq.json')
    dataset = client.dataset('foodhoover_store')

    table_ref = dataset.table(table_name)
    table = client.get_table(table_ref)  # API call
    bq_blob = client.insert_rows_json(table, results)

    client.close()
    print(bq_blob)
    return (bq_blob)

def bq_get_places(run_id):

    f = open('secrets.json')
    secrets = json.load(f)
    api_key = secrets['map_key']

    try: 
        client = get_bq_client()
        ##query to merge rx_ref with the most recent row in places_cache (we run this twice, before and after the scrape)
        merge_sql = " \
        MERGE rooscrape.foodhoover_store.rx_ref ref \
        USING \
        (SELECT rx_uid, string_agg(place_id ORDER BY scrape_time DESC LIMIT 1) as place_id \
        FROM rooscrape.foodhoover_store.places_cache \
        GROUP BY rx_uid) places_cache \
        ON ref.rx_uid = places_cache.rx_uid \
        WHEN MATCHED THEN \
        UPDATE SET ref.place_id=COALESCE(ref.place_id,places_cache.place_id)"

        query_job = client.query(merge_sql) 
        query_job.result()

        ##get the places still to scrape
        client = get_bq_client()
        bqstorageclient = get_bq_storage()

        sql = "SELECT * from rooscrape.foodhoover_store.rx_ref WHERE \
        rx_lat is not null and rx_lng is not null and rx_name is not null and place_id is null limit 2000"

        place_results = (
            client.query(sql)
            .result()
            .to_dataframe(bqstorage_client=bqstorageclient)
        )

        fetch_datas = []
        for index, row in place_results.iterrows():
            fetch_data = {'rx_uid':row['rx_uid'], 'rx_name':row['rx_name'],'rx_lat':row['rx_lat'],'rx_lng':row['rx_lng'], 'api_key':api_key}
            fetch_datas.append(fetch_data)
            
        ###this is the async function called inside URL batch that gets the data, handles the fallback, and outputs the results   
        async def places_fetch_function(fetch_data):
            def make_places_uri(rx_name, lat, lng, api_key):
                keyword = urllib.parse.quote_plus(rx_name)
                uri = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json?input={keyword}&inputtype=textquery&locationbias=circle:500@{lat},{lng}&key={api_key}".format(lat=lat, lng=lng, keyword=keyword, api_key=api_key)
                return uri

            uri = make_places_uri(fetch_data['rx_name'], fetch_data['rx_lat'], fetch_data['rx_lng'], fetch_data['api_key'])
            async with ClientSession() as session:
                try:
                    async with session.get(uri) as response:
                        response = await response.read()
                        response = json.loads(response.decode('utf8'))
                        if response['status']=='OK':
                            place_id = response['candidates'][0]['place_id']
                            return {'rx_uid':fetch_data['rx_uid'],'place_id':place_id,'scrape_time':'AUTO'}
                        elif response['status']=='ZERO_RESULTS':
                            filtered_name = ''.join([x if (x.isalpha() or x.isspace()) else '' for x in fetch_data['rx_name']])
                            uri = make_places_uri(filtered_name, fetch_data['rx_lat'], fetch_data['rx_lng'], fetch_data['api_key'])
                            async with session.get(uri) as response:
                                response = await response.read()
                                response = json.loads(response.decode('utf8'))
                                if response['status']=='OK':
                                    place_id = response['candidates'][0]['place_id']
                                    return {'rx_uid':fetch_data['rx_uid'],'place_id':place_id,'scrape_time':'AUTO'}
                                else:
                                     return {'rx_uid':fetch_data['rx_uid'],'place_id':'not found','scrape_time':'AUTO'}
                        else:
                            return {'rx_uid':fetch_data['rx_uid'],'place_id':'fetch error','scrape_time':'AUTO'}
                except:
                    return {'rx_uid':fetch_data['rx_uid'],'place_id':'fetch error','scrape_time':'AUTO'}

        try:
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy()) ##on windows, this is to stop an error that the loop isn't closed
        except:
            pass

        scrape_results = asyncio.run(url_batch(fetch_datas, places_fetch_function, 50))
        
        if len(scrape_results)>0: ###write to BQ
            print('Append: '+str(len(scrape_results)))
            write_bq(scrape_results, 'places_cache')
            
        ##now do the merge again
        query_job = client.query(merge_sql)
        query_job.result()
        
        num_rows_added = query_job.num_dml_affected_rows
        bq_step_logger(run_id, 'GET-PLACES', 'SUCESS', num_rows_added)
        return num_rows_added
    except Exception as e:
        bq_step_logger(run_id, 'GET-PLACES', 'FAIL', str(e))
        return e        


def bq_places_proc(run_id):
    try:
        sql="\
            UPDATE rooscrape.foodhoover_store.rx_ref u\
            SET u.hoover_place_id=v.final_place_id\
            FROM \
            (SELECT\
                r.rx_uid,\
                CASE WHEN\
                    ST_DWITHIN(ST_GEOGPOINT(rx_lng,rx_lat),ST_GEOGPOINT(place_lng,place_lat),400)\
                THEN\
                    r.place_id\
                ELSE\
                    r.rx_uid\
                END as final_place_id\
            FROM rooscrape.foodhoover_store.rx_ref r\
            LEFT JOIN (\
                SELECT rx_uid,\
                PERCENTILE_CONT(rx_lat, 0.5) OVER(PARTITION BY place_id) AS place_lat,\
                PERCENTILE_CONT(rx_lng, 0.5) OVER(PARTITION BY place_id) AS place_lng\
                FROM rooscrape.foodhoover_store.rx_ref) q\
            ON q.rx_uid=r.rx_uid\
            ORDER BY place_id asc) v\
            WHERE u.rx_uid=v.rx_uid\
        "
        client = get_bq_client()
        query_job = client.query(sql)
        query_job.result()
                
        num_rows_added = query_job.num_dml_affected_rows
        bq_step_logger(run_id, 'PROC-PLACES', 'SUCESS', num_rows_added)
        return num_rows_added
    except Exception as e:
        bq_step_logger(run_id, 'PROC-PLACES', 'FAIL', str(e))
        return e   

def bq_places_table(run_id):
    try:
        client = get_bq_client()
        query_job = client.query("DELETE FROM rooscrape.foodhoover_store.places WHERE 1=1")
        query_job.result()

        sql = " \
            INSERT INTO rooscrape.foodhoover_store.places (place_id, place_name, place_label, place_sector, place_lat, place_lng, place_location, place_vendors) \
            (SELECT\
                hoover_place_id as place_id,\
                place_name.value as place_name,\
                CONCAT(place_name.value,': ',place_sector.value, ' (',ARRAY_TO_STRING(place_vendors,','),')') as place_label,\
                place_sector.value as place_sector,\
                place_lat,\
                place_lng,\
                SAFE.ST_GEOGPOINT(place_lat, place_lng) as place_location,\
                place_vendors,\
            FROM (\
                SELECT \
                    hoover_place_id,\
                    MIN(place_lat) as place_lat,\
                    MIN(place_lng) as place_lng,\
                    APPROX_TOP_COUNT(rx_name,1) as place_name,\
                    APPROX_TOP_COUNT(rx_sector,1) as place_sector,\
                    ARRAY_AGG(DISTINCT(vendor)) as place_vendors\
                FROM (\
                    SELECT *,\
                    PERCENTILE_CONT(rx_lat, 0.5) OVER(PARTITION BY hoover_place_id) AS place_lat,\
                    PERCENTILE_CONT(rx_lng, 0.5) OVER(PARTITION BY hoover_place_id) AS place_lng\
                    FROM rooscrape.foodhoover_store.rx_ref)\
                WHERE hoover_place_id IS NOT NULL \
                GROUP by hoover_place_id),\
            UNNEST(place_name) as place_name,\
            UNNEST(place_sector) as place_sector)"

        client = get_bq_client()
        query_job = client.query(sql)
        query_job.result()
        
        num_rows_added = query_job.num_dml_affected_rows
        bq_step_logger(run_id, 'CREATE-PLACES', 'SUCESS', num_rows_added)
        return num_rows_added
    except Exception as e:
        bq_step_logger(run_id, 'CREATE-PLACES', 'FAIL', str(e))
        return e   

def bq_agg_results_district(run_id):
    try:
        client = get_bq_client()
        table_id = 'rooscrape.foodhoover_store.agg_district_run'

        table = client.get_table(table_id)
        num_rows_begin = table.num_rows

        if run_id=='full': ## this is the tag we to remove the where clause and compute for all runids
            where_clause = ""
            sql = "DELETE FROM "+table_id+" WHERE 1=1" ##empty the current table
            query_job = client.query(sql)  # API request
            query_job.result()
        else:
            where_clause = " WHERE b.run_id = '"+run_id+"' "

        sql = " \
            MERGE "+table_id+"  q \
            USING \
            (SELECT a.postcode_district, b.run_id, max(b.scrape_time) as scrape_time, \
            count(distinct case when b.vendor='ROO' then b.rx_slug else null end) as ROO, \
            count(distinct case when b.vendor='JE' then b.rx_slug else null end) as JE, \
            count(distinct case when b.vendor='UE' then b.rx_slug else null end) as UE, \
            count(distinct case when b.vendor='FH' then b.rx_slug else null end) as FH \
            FROM rooscrape.foodhoover_store.rx_cx_results_raw b \
            LEFT join rooscrape.foodhoover_store.rx_ref c on c.rx_slug=b.rx_slug and c.vendor=b.vendor \
            LEFT JOIN rooscrape.foodhoover_store.postcode_lookup a on a.postcode=b.cx_postcode \
            "+ where_clause + "\
            GROUP BY a.postcode_district, b.run_id) p \
            ON q.run_id=p.run_id and q.postcode_district = p.postcode_district \
            WHEN MATCHED THEN \
            UPDATE SET q.scrape_time = p.scrape_time, q.roo=p.roo,q.je=p.je, q.ue=p.ue, q.fh=p.fh \
            WHEN NOT MATCHED THEN \
            INSERT (postcode_district, run_id, scrape_time, roo, je, ue, fh) VALUES (p.postcode_district, p.run_id, p.scrape_time, p.roo, p.je, p.ue, p.fh) \
            "
        query_job = client.query(sql)  # API request
        rows = query_job.result()

        table = client.get_table(table_id)
        num_rows_added = table.num_rows - num_rows_begin

        bq_step_logger(run_id, 'AGGREGATE-DISTRICT', 'SUCESS', num_rows_added)
        return num_rows_added  
    except Exception as e:
        bq_step_logger(run_id, 'AGGREGATE-DISTRICT', 'FAIL', str(e))
        return e   

def bq_agg_results_sector(run_id):

    try:
        client = get_bq_client()
        table_id = 'rooscrape.foodhoover_store.agg_sector_run'

        table = client.get_table(table_id)
        num_rows_begin = table.num_rows

        if run_id=='full': ## this is the tag we to remove the where clause and compute for all runids
            where_clause = ""
            sql = "DELETE FROM "+table_id+" WHERE 1=1" ##empty the current table
            query_job = client.query(sql)  # API request
            query_job.result()
        else:
            where_clause = " WHERE b.run_id = '"+run_id+"' "

        sql = " \
            MERGE "+table_id+"  q \
            USING \
            (SELECT a.postcode_sector, b.run_id, max(b.scrape_time) as scrape_time, \
            count(distinct case when b.vendor='ROO' then b.rx_slug else null end) as ROO, \
            count(distinct case when b.vendor='JE' then b.rx_slug else null end) as JE, \
            count(distinct case when b.vendor='UE' then b.rx_slug else null end) as UE, \
            count(distinct case when b.vendor='FH' then b.rx_slug else null end) as FH \
            FROM rooscrape.foodhoover_store.rx_cx_results_raw b \
            LEFT join rooscrape.foodhoover_store.rx_ref c on c.rx_slug=b.rx_slug and c.vendor=b.vendor \
            LEFT JOIN rooscrape.foodhoover_store.postcode_lookup a on a.postcode=b.cx_postcode \
            "+ where_clause + "\
            GROUP BY a.postcode_sector, b.run_id) p \
            ON q.run_id=p.run_id and q.postcode_sector = p.postcode_sector \
            WHEN MATCHED THEN \
            UPDATE SET q.scrape_time = p.scrape_time, q.roo=p.roo,q.je=p.je, q.ue=p.ue, q.fh=p.fh \
            WHEN NOT MATCHED THEN \
            INSERT (postcode_sector, run_id, scrape_time, roo, je, ue, fh) VALUES (p.postcode_sector, p.run_id, p.scrape_time, p.roo, p.je, p.ue, p.fh) \
            "
        query_job = client.query(sql)  # API request
        rows = query_job.result()

        table = client.get_table(table_id)
        num_rows_added = table.num_rows - num_rows_begin

        bq_step_logger(run_id, 'AGGREGATE-SECTOR', 'SUCESS', num_rows_added)
        return num_rows_added  
    except Exception as e:
        bq_step_logger(run_id, 'AGGREGATE-SECTOR', 'FAIL', str(e))
        return e 

def bq_agg_results_country_pop(run_id):
    try:
        client = get_bq_client()
        table_id = 'rooscrape.foodhoover_store.agg_country_run_pop'

        table = client.get_table(table_id)
        num_rows_begin = table.num_rows

        if run_id=='full': ## this is the tag we to remove the where clause and compute for all runids
            where_clause = ""
            sql = "DELETE FROM "+table_id+" WHERE 1=1" ##empty the current table
            query_job = client.query(sql)  # API request
            query_job.result()
        else:
            where_clause = " WHERE run_id = '"+run_id+"' "
        
        sql ="\
            MERGE "+table_id+"  q \
            USING(\
            WITH raw as (\
            SELECT run_id, max(scrape_time) as scrape_time, ref.vendor, look.postcode_sector, ARRAY_AGG(DISTINCT coverage.rx_uid) as rx_included FROM (\
                SELECT CONCAT(rx_slug,'-',vendor) as rx_uid, cx_postcode, run_id, max(scrape_time) as scrape_time FROM rooscrape.foodhoover_store.rx_cx_results_raw"\
                +where_clause+"\
                GROUP BY run_id, rx_uid, cx_postcode) coverage\
            LEFT JOIN rooscrape.foodhoover_store.postcode_lookup as look on look.postcode=coverage.cx_postcode\
            LEFT JOIN rooscrape.foodhoover_store.rx_ref ref on ref.rx_uid = coverage.rx_uid\
            GROUP BY run_id, ref.vendor, look.postcode_sector)\
            SELECT\
                raw.run_id,\
                raw.vendor,\
                MAX(raw.scrape_time) as scrape_time,\
                SUM(sectors.population) as delivery_population,\
                MAX(rx_num) as rx_num\
            FROM raw\
            LEFT JOIN rooscrape.foodhoover_store.sectors sectors on raw.postcode_sector=sectors.sector\
            LEFT JOIN (\
                SELECT raw.run_id,raw.vendor,COUNT(DISTINCT rx_uid) as rx_num\
                FROM raw, UNNEST(raw.rx_included) as rx_uid\
                GROUP BY raw.run_id, raw.vendor\
                ) rx_list ON rx_list.vendor=raw.vendor and rx_list.run_id=raw.run_id\
            GROUP BY raw.run_id, raw.vendor) p\
            ON q.run_id=p.run_id and q.vendor=p.vendor\
            WHEN MATCHED THEN\
            UPDATE SET q.scrape_time = p.scrape_time, q.delivery_population=p.delivery_population,q.rx_num=p.rx_num\
            WHEN NOT MATCHED THEN\
            INSERT (run_id, scrape_time,vendor,delivery_population,rx_num) VALUES (p.run_id, p.scrape_time, p.vendor, p.delivery_population, p.rx_num)\
        "

        query_job = client.query(sql)  # API request
        rows = query_job.result()

        table = client.get_table(table_id)
        num_rows_added = table.num_rows - num_rows_begin

        bq_step_logger(run_id, 'AGGREGATE-COUNTRY-POP', 'SUCESS', num_rows_added)
        return num_rows_added  
    except Exception as e:
        bq_step_logger(run_id, 'AGGREGATE-COUNTRY-POP', 'FAIL', str(e))
        return e       

def bq_agg_rx_results_fast(run_id):
    try:
        client = get_bq_client()
        table_id = 'rooscrape.foodhoover_store.rx_cx_fast'

        table = client.get_table(table_id)
        num_rows_begin = table.num_rows

        if run_id=='full': ## this is the tag we to remove the where clause and compute for all runids
            where_clause = ""
            sql = "DELETE FROM "+table_id+" WHERE 1=1" ##empty the current table
            query_job = client.query(sql)  # API request
            query_job.result()
        else:
            where_clause = " WHERE b.run_id = '"+run_id+"' "

        sql = " \
            MERGE "+table_id+"  rx_fast \
            USING ( \
            SELECT CONCAT(b.rx_slug,'-',b.vendor) as rx_uid, max(b.scrape_time) as scrape_time, b.cx_postcode, b.run_id \
            FROM rooscrape.foodhoover_store.rx_cx_results_raw b \
            LEFT join rooscrape.foodhoover_store.postcode_lookup c on b.cx_postcode=c.postcode \
            "+ where_clause + "\
            GROUP BY rx_uid, b.run_id, b.cx_postcode \
            ) as rx_results \
            ON rx_fast.run_id = rx_results.run_id and rx_fast.rx_uid = rx_results.rx_uid and rx_fast.cx_postcode=rx_results.cx_postcode \
            WHEN MATCHED THEN \
            UPDATE SET rx_fast.scrape_time=COALESCE(rx_results.scrape_time, rx_fast.scrape_time) \
            WHEN NOT MATCHED THEN \
            INSERT (rx_uid, scrape_time,  cx_postcode, run_id) VALUES (rx_results.rx_uid, rx_results.scrape_time, rx_results.cx_postcode, rx_results.run_id) \
            "
        query_job = client.query(sql)  # API request
        rows = query_job.result()

        table = client.get_table(table_id)
        num_rows_added = table.num_rows - num_rows_begin

        bq_step_logger(run_id, 'AGGREGATE-FAST', 'SUCESS', num_rows_added)
        return num_rows_added  
    except Exception as e:
        bq_step_logger(run_id, 'AGGREGATE-FAST', 'FAIL', str(e))
        return e

def bq_agg_rx_cx(run_id):
    try:
        client = get_bq_client()

        sql = "SELECT MAX(scrape_time) as last_date FROM rooscrape.foodhoover_store.agg_country_run"
        query_job = client.query(sql)  # API request
        rows = query_job.result()
        #row = next(rows)
        end = list(rows)[0]['last_date']
        start = end - timedelta(days=14)

        end_date = end.strftime('%Y-%m-%d')
        start_date = start.strftime('%Y-%m-%d')
        
        table_id = 'rooscrape.foodhoover_store.agg_rx_cx'
        client.delete_table(table_id, not_found_ok=True) 

        sql = "\
            CREATE TABLE rooscrape.foodhoover_store.agg_rx_cx\
            CLUSTER BY place_id\
            AS\
            SELECT\
                places.place_id,\
                bysector.vendor,\
                ST_SIMPLIFY(ST_UNION_AGG(sectors.geometry),50) as delivery_zone,\
                SUM(sectors.population) as delivery_population,\
                max(places.place_name) as place_name,\
                max(places.place_lat) as place_lat,\
                max(places.place_lng) as place_lng,\
                ARRAY_CONCAT_AGG(vendor_rx LIMIT 1) as vendor_rx,\
                ARRAY_AGG(DISTINCT(sectors.sector) IGNORE NULLS) as sectors_covered\
            FROM (\
                SELECT\
                places.place_id,\
                ref.vendor,\
                sectors.sector,\
                ARRAY_AGG(DISTINCT(ref.rx_uid) IGNORE NULLS) as vendor_rx,\
                FROM (\
                    SELECT rx_uid, cx_postcode FROM rooscrape.foodhoover_store.rx_cx_fast\
                    WHERE scrape_time>='"+start_date+"' and scrape_time<='"+end_date+"'\
                    GROUP BY rx_uid, cx_postcode) results\
                LEFT JOIN rooscrape.foodhoover_store.postcode_lookup pc on pc.postcode=results.cx_postcode\
                LEFT JOIN rooscrape.foodhoover_store.sectors sectors on sectors.sector = pc.postcode_sector\
                LEFT JOIN rooscrape.foodhoover_store.rx_ref ref on ref.rx_uid = results.rx_uid\
                LEFT JOIN rooscrape.foodhoover_store.places places on places.place_id = ref.hoover_place_id\
                GROUP BY places.place_id, ref.vendor, sectors.sector) bysector\
            LEFT JOIN rooscrape.foodhoover_store.sectors sectors on sectors.sector = bysector.sector\
            RIGHT JOIN rooscrape.foodhoover_store.places places on places.place_id = bysector.place_id\
            GROUP BY places.place_id, bysector.vendor\
        "

        query_job = client.query(sql)  # API request
        rows = query_job.result()

        table = client.get_table(table_id)
        num_rows_added = table.num_rows

        bq_step_logger(run_id, 'AGG-RX-CX', 'SUCESS', num_rows_added)
        return num_rows_added
    except Exception as e:
        bq_step_logger(run_id, 'AGG-RX-CX', 'FAIL', str(e))
        return e

def write_bq_table(table_ref, sql):
    bq_client = get_bq_client()

    # Start the query, passing in the extra configuration.
    job_config = bigquery.QueryJobConfig()
    job_config.destination = table_ref
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    query_job = bq_client.query(
        sql,
        location='US',
        job_config=job_config)

    while not query_job.done():
        time.sleep(1)

    return table_ref

def bq_to_gcs(table_ref, gcs_filename, folder):

    bucket_name = 'rooscrape-exports'

    ###empty what is in that file_run_id folder already
    storage_client = get_gcs_client()
    blobs = storage_client.list_blobs(bucket_name, prefix=folder+'/')
    for blob in blobs:
        blob.delete()

    bq_client = get_bq_client()
    job_config = bigquery.ExtractJobConfig()
    job_config.compression = bigquery.Compression.GZIP
    job_config.destination_format = (bigquery.DestinationFormat.CSV)
    job_config.print_header = False

    destination_uri = 'gs://{}/{}/{}'.format(bucket_name, folder, gcs_filename)

    extract_job = bq_client.extract_table(
        table_ref,
        destination_uri,
        job_config=job_config,
        location='US')  # API request
    extract_job.result()
    
    ##clean up temp table... only if it's a temp table
    if table_ref == 'rooscrape.foodhoover_store.temp_table':
        bq_client.delete_table(table_ref, not_found_ok=True)

    ###get the final file path
    gcs_file = compose_blobs(bucket_name, folder)

    return gcs_file

def compose_blobs(bucket_name, folder):
    storage_client = get_gcs_client()
    bucket = storage_client.bucket(bucket_name)
    n_compose = 32
    
    blobs = list(storage_client.list_blobs(bucket_name, prefix=folder+'/'))
    
    i=0
    while len(blobs)>1: ##this iterates over the files, consolidating them in bacthes of 32 into a single file
        chunks = [blobs[x:x+n_compose] for x in range(0, len(blobs), n_compose)]

        for chunk in chunks:
            destination = bucket.blob(folder+'/'+folder+'_'+str(i))
            i=i+1
            destination.compose(chunk)
            
            for file in chunk:
                file.delete()
            
        blobs = list(storage_client.list_blobs(bucket_name, prefix=folder+'/'))
        
    return 'gs://{}/{}'.format(bucket_name, blobs[0].name)

def import_sql(file_path, columns, table_name):

    project = 'rooscrape'
    instance = 'foodhoover-db'

    instances_import_request_body = {
        "importContext": {
            "uri": file_path,
            "database": "foodhoover_cache",
            "fileType": "CSV",
            "csvImportOptions": {
                "table": table_name,
                "columns" : columns
            }
        }
    }
    
    service  = get_api_client()
    request = service.instances().import_(project=project, instance=instance, body=instances_import_request_body)
    response = request.execute()
    status = response['status']
    operation_id = response['name']

    while (status not in ['DONE']):
        time.sleep(15)
        status_request = service.operations().get(project='rooscrape', operation=operation_id)
        status_response = status_request.execute()
        status = status_response['status']
        print(file_path+":  "+status)

    return status

def generic_exporter(run_id, bq_table, sql_table, sql_schema, bq_select_sql, sql_create_statement, write_mode):
    ##get a reference to the data to export
    #if write_mode =='overwrite':
    #    target_table = write_bq_table(bq_table, bq_select_sql)
    #else:

    target_table = write_bq_table('rooscrape.foodhoover_store.temp_table', bq_select_sql)

    ##write that table to GCS
    gcs_folder = sql_table+'-'+run_id
    gcs_filename = gcs_folder +'*.gzip' ###adding a wildcard to shard files on export; we consolidte in cloud storage
    file_path =  bq_to_gcs(target_table, gcs_filename,gcs_folder)
    print(file_path)

    #export data to cloud SQL
    if write_mode =='overwrite':
        ##empty the sql table ##should be drop and recreate with index
        engine = get_sql_client('foodhoover_cache')
        conn = engine.connect()
        sql = "DROP TABLE IF EXISTS {}".format(sql_table)
        result = conn.execute(sql)
        ##recreate the table
        f = open(sql_create_statement,"r")
        sql_script = f.read().split(';')
        for sql_command in sql_script:
            if len(sql_command)>0:
                result = conn.execute(sql_command)
        conn.close()
    
    #export to cloudsql
    status = import_sql(file_path, sql_schema, sql_table)
    
    return status    

def bq_export_rx_cx_results(run_id):
    try:
        bq_table = 'rooscrape.foodhoover_store.rx_cx_fast'
        sql_table = 'rx_cx_fast'
        sql_schema = ['rx_uid','scrape_time','cx_postcode','run_id']
        bq_select_sql = "SELECT rx_uid, scrape_time, cx_postcode, run_id FROM foodhoover_store.rx_cx_fast WHERE run_id='"+run_id+"'",
        sql_create_statement = "table_schemas/rx_cx_fast"

        if run_id=='full':
            write_mode = 'overwrite'
        else:
            write_mode = 'append'

        status = generic_exporter(run_id, bq_table, sql_table, sql_schema, bq_select_sql, sql_create_statement, write_mode)

        bq_step_logger(run_id, 'EXPORT-RX-CX-RESULTS', 'SUCESS', status)
        return status
    except Exception as e:
        bq_step_logger(run_id, 'EXPORT-RX-CX-RESULTS', 'FAIL', str(e))
        return e

def bq_export_rx_ref(run_id):
    try:
        bq_table = 'rooscrape.foodhoover_store.rx_ref'
        sql_table = 'rx_ref'
        sql_schema = ['rx_uid','rx_slug','vendor','rx_name','rx_postcode','rx_district','rx_sector', 'rx_lat', 'rx_lng','rx_menu','place_id','hoover_place_id']
        bq_select_sql = "SELECT rx_uid, rx_slug, vendor, rx_name, rx_postcode, rx_district, rx_sector, rx_lat, rx_lng, rx_menu, place_id, hoover_place_id FROM foodhoover_store.rx_ref"
        write_mode = 'overwrite'
        sql_create_statement = "table_schemas/rx_ref"

        status = generic_exporter(run_id, bq_table, sql_table, sql_schema, bq_select_sql, sql_create_statement, write_mode)

        bq_step_logger(run_id, 'EXPORT-RX-REF', 'SUCESS', status)
        return status
    except Exception as e:
        bq_step_logger(run_id, 'EXPORT-RX-REF', 'FAIL', str(e))
        return e

def bq_export_places(run_id):
    try:
        bq_table = 'rooscrape.foodhoover_store.places'
        sql_table = 'places'
        sql_schema = ['place_id', 'place_name', 'place_label', 'place_sector', 'place_lat', 'place_lng', 'place_location','place_vendors']
        bq_select_sql = "SELECT place_id, place_name, place_label, place_sector, place_lat, place_lng, TO_HEX(ST_ASBINARY(place_location)) as place_location, REPLACE(REPLACE(TO_JSON_STRING(place_vendors),'[','{'),']','}') as place_vendors FROM foodhoover_store.places"
        write_mode = 'overwrite'
        sql_create_statement = "table_schemas/places"

        status = generic_exporter(run_id, bq_table, sql_table, sql_schema, bq_select_sql, sql_create_statement, write_mode)

        bq_step_logger(run_id, 'EXPORT-PLACES', 'SUCESS', status)
        return status
    except Exception as e:
        bq_step_logger(run_id, 'EXPORT-PLACES', 'FAIL', str(e))
        return e
        
def bq_export_agg_sector_run(run_id):
    try:
        bq_table = 'rooscrape.foodhoover_store.agg_sector_run'
        sql_table = 'agg_sector_run'
        sql_schema = ['postcode_sector', 'run_id', 'scrape_time', 'roo', 'je', 'ue', 'fh']
        bq_select_sql = "SELECT postcode_sector, run_id, scrape_time, roo, je, ue, fh FROM rooscrape.foodhoover_store.agg_sector_run WHERE run_id='"+run_id+"' and postcode_sector IS NOT NULL"
        sql_create_statement = "table_schemas/agg_sector_run"

        if run_id=='full':
            write_mode = 'overwrite'
        else:
            write_mode = 'append'

        status = generic_exporter(run_id, bq_table, sql_table, sql_schema, bq_select_sql, sql_create_statement, write_mode)

        bq_step_logger(run_id, 'EXPORT-AGG-SECTOR-RUN', 'SUCESS', status)
        return status
    except Exception as e:
        bq_step_logger(run_id, 'EXPORT-AGG-SECTOR-RUN', 'FAIL', str(e))
        return e

def bq_export_agg_district_run(run_id):
    try:
        bq_table = 'rooscrape.foodhoover_store.agg_district_run'
        sql_table = 'agg_district_run'
        sql_schema = ['postcode_district', 'run_id', 'scrape_time', 'roo', 'je', 'ue', 'fh']
        bq_select_sql = "SELECT postcode_district, run_id, scrape_time, roo, je, ue, fh FROM rooscrape.foodhoover_store.agg_district_run WHERE run_id='"+run_id+"' and postcode_district IS NOT NULL"
        sql_create_statement = "table_schemas/agg_district_run"

        if run_id=='full':
            write_mode = 'overwrite'
        else:
            write_mode = 'append'

        status = generic_exporter(run_id, bq_table, sql_table, sql_schema, bq_select_sql, sql_create_statement, write_mode)

        bq_step_logger(run_id, 'EXPORT-AGG-DISTRICT-RUN', 'SUCESS', status)
        return status
    except Exception as e:
        bq_step_logger(run_id, 'EXPORT-AGG-DISTRICT-RUN', 'FAIL', str(e))
        return e

def bq_export_agg_country_run(run_id):
    try:
        bq_table = 'rooscrape.foodhoover_store.agg_country_run'
        sql_table = 'agg_country_run'
        sql_schema = ['country', 'run_id', 'scrape_time', 'roo', 'je', 'ue', 'fh']
        bq_select_sql = "SELECT country, run_id, scrape_time, roo, je, ue, fh FROM rooscrape.foodhoover_store.agg_country_run WHERE run_id='"+run_id+"' and country IS NOT NULL"
        sql_create_statement = "table_schemas/agg_country_run"

        if run_id=='full':
            write_mode = 'overwrite'
        else:
            write_mode = 'append'

        status = generic_exporter(run_id, bq_table, sql_table, sql_schema, bq_select_sql, sql_create_statement, write_mode)

        bq_step_logger(run_id, 'EXPORT-AGG-COUNTRY-RUN', 'SUCESS', status)
        return status
    except Exception as e:
        bq_step_logger(run_id, 'EXPORT-AGG-COUNTRY-RUN', 'FAIL', str(e))
        return e

def bq_export_agg_country_run_pop(run_id):
    try:
        bq_table = 'rooscrape.foodhoover_store.agg_country_run_pop'
        sql_table = 'agg_country_run_pop'
        sql_schema = ['run_id', 'scrape_time', 'vendor','delivery_population', 'rx_num']
        sql_create_statement = "table_schemas/agg_country_run_pop"

        if run_id=='full':
            write_mode = 'overwrite'
            bq_select_sql = "SELECT run_id, scrape_time, vendor, delivery_population, rx_num FROM rooscrape.foodhoover_store.agg_country_run_pop WHERE scrape_time>='2021-03-15'"
        else:
            write_mode = 'append'
            bq_select_sql = "SELECT run_id, scrape_time, vendor, delivery_population, rx_num FROM rooscrape.foodhoover_store.agg_country_run_pop WHERE scrape_time>='2021-03-15' and run_id='"+run_id+"'"

        status = generic_exporter(run_id, bq_table, sql_table, sql_schema, bq_select_sql, sql_create_statement, write_mode)

        bq_step_logger(run_id, 'EXPORT-AGG-COUNTRY-RUN-POP', 'SUCESS', status)
        return status
    except Exception as e:
        bq_step_logger(run_id, 'EXPORT-AGG-COUNTRY-RUN-POP', 'FAIL', str(e))
        return e

def bq_export_agg_rx_cx(run_id):
    try:
        bq_table = 'rooscrape.foodhoover_store.agg_rx_cx'
        sql_table = 'agg_rx_cx'
        sql_schema = ['place_id', 'vendor', 'delivery_zone','delivery_population','place_name','place_lat','place_lng','vendor_rx', 'sectors_covered']
        bq_select_sql = "SELECT place_id, vendor, TO_HEX(ST_ASBINARY(delivery_zone)) as delivery_zone, delivery_population,place_name,place_lat,place_lng, REPLACE(REPLACE(TO_JSON_STRING(vendor_rx),'[','{'),']','}') as vendor_rx, REPLACE(REPLACE(TO_JSON_STRING(sectors_covered),'[','{'),']','}') as sectors_covered FROM rooscrape.foodhoover_store.agg_rx_cx"
        sql_create_statement = "table_schemas/agg_rx_cx"

        status = generic_exporter(run_id, bq_table, sql_table, sql_schema, bq_select_sql, sql_create_statement,'overwrite')

        bq_step_logger(run_id, 'EXPORT-AGG-RX-CX', 'SUCESS', status)
        return status
    except Exception as e:
        bq_step_logger(run_id, 'EXPORT-AGG-RX-CX', 'FAIL', str(e))
        return e