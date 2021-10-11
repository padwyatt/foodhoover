import time
import urllib
import json
from datetime import timedelta, datetime

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

def t_post_process(steps, process_date):
    results = []
    if steps == ['ALL']:
        steps = ['INSERT','UPDATE-ROO','UPDATE-UE', 'UPDATE-JE-FH','MISSING-POSTCODES','UPDATE-GEOS','GET-PLACES','PROC-PLACES','CREATE-PLACES','AGG-RX-CX-SECTOR','AGG-RESULTS-COUNTRY-FULFILLMENT','AGG-RESULTS-DISTRICT-FULFILLMENT','AGG-RESULTS-SECTOR-FULFILLMENT','AGG-DELIVERY-ZONE','EXPORT-RX-REF','EXPORT-PLACES','EXPORT-AGG-DELIVERY-ZONE','EXPORT-AGG-COUNTRY-FULFILLMENT-DAY','EXPORT-AGG-DISTRICT-FULFILLMENT-DAY','EXPORT-AGG-SECTOR-FULFILLMENT-DAY']

    for step in steps:
        if step == 'INSERT':
            result = step+": "+str(t_update_ref(process_date))
            results.append(result)
            yield result
        elif step == 'UPDATE-ROO': 
            result = step+": "+str(t_crawl_roo(process_date))
            results.append(result)
            yield result
        elif step == 'UPDATE-UE':
            result = step+": "+str(t_crawl_ue(process_date))
            results.append(result)
            yield result
        elif step == 'UPDATE-JE-FH':
            result = step+": "+str(t_update_je_fh(process_date))
            results.append(result)
            yield result
        elif step == 'MISSING-POSTCODES':
            result = step+": "+str(t_update_missing_postcodes(process_date))
            results.append(result)
            yield result        
        elif step == 'UPDATE-GEOS':
            result = step+": "+str(t_update_geos(process_date))
            results.append(result)
            yield result
        elif step == 'GET-PLACES':
            result = step+": "+str(t_get_places(process_date))
            results.append(result)
            yield result
        elif step == 'PROC-PLACES':
            result = step+": "+str(t_places_proc(process_date))
            results.append(result)
            yield result
        elif step == 'CREATE-PLACES':
            result = step+": "+str(t_places_table(process_date))
            results.append(result)  
            yield result 
        elif step == 'AGG-RX-CX-SECTOR':
            result = step+": "+str(t_agg_rx_cx_sector(process_date))
            results.append(result)
            yield result
        elif step == 'AGG-RESULTS-DISTRICT-FULFILLMENT':
            result = step+": "+str(t_agg_results_district_fulfillment(process_date))
            results.append(result)
            yield result
        elif step == 'AGG-RESULTS-SECTOR-FULFILLMENT':
            result = step+": "+str(t_agg_results_sector_fulfillment(process_date))
            results.append(result)
            yield result
        elif step == 'AGG-RESULTS-COUNTRY-FULFILLMENT':
            result = step+": "+str(t_agg_results_country_fulfillment(process_date))
            results.append(result)
            yield result
        elif step == 'AGG-DELIVERY-ZONE':
            result = step+": "+str(t_agg_delivery_zone(process_date))
            results.append(result)
            yield result
        elif step == 'EXPORT-RX-REF':
            result = step+": "+str(t_export_rx_ref(process_date))
            results.append(result)
            yield result
        elif step == 'EXPORT-PLACES':
            result = step+": "+str(t_export_places(process_date))
            results.append(result)   
            yield result 
        elif step == 'EXPORT-AGG-DELIVERY-ZONE':
            result = step+": "+str(t_export_agg_delivery_zone(process_date))
            results.append(result)   
            yield result 
        elif step == 'EXPORT-AGG-COUNTRY-FULFILLMENT-DAY':
            result = step+": "+str(t_export_agg_country_fulfillment_day(process_date))
            results.append(result)
            yield result
        elif step == 'EXPORT-AGG-DISTRICT-FULFILLMENT-DAY':
            result = step+": "+str(t_export_agg_district_fulfillment_day(process_date))
            results.append(result)
            yield result
        elif step == 'EXPORT-AGG-SECTOR-FULFILLMENT-DAY':
            result = step+": "+str(t_export_agg_sector_fulfillment_day(process_date))
            results.append(result)
            yield result
        else:
            result = step+ ": Step not found"
            results.append(result)
            yield result

    yield "DONE PROCESS"

def t_step_logger(process_date, step,status, result):

    results = [{'process_date':process_date, 'step':step, 'status':status, 'result':result, 'process_time':'AUTO'}]

    client = get_bq_client()
    dataset = client.dataset('foodhoover_store')
    table_ref = dataset.table('process_step_log')
    table = client.get_table(table_ref)  # API call
    client.insert_rows_json(table, results)

    return "logged"

def t_update_ref(process_date):
    try:
        client = get_bq_client()

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("process_date", "DATE",process_date)   
            ]
        )
        table_id = 'rooscrape.foodhoover_store.rx_ref'

        ##delete any other partition with process date
        sql = "DELETE FROM "+table_id+" WHERE process_date=@process_date"
        query_job = client.query(sql, job_config=job_config)

        ##copy rx_ref from previous day to this new partition
        sql = "\
            DECLARE max_date DATE;\
            SET max_date = (\
                SELECT MAX(process_date) FROM rooscrape.foodhoover_store.rx_ref where process_date<=@process_date);\
            INSERT INTO "+table_id+" (process_date,rx_uid,rx_id,rx_slug,vendor,rx_name,rx_postcode,rx_district,rx_sector,rx_lat,rx_lng,rx_fulfillment_type,rx_sponsored,rx_menu,google_place_id,place_id,last_seen_live,first_seen_live,update_time,places_update)\
            SELECT @process_date,rx_uid,rx_id,rx_slug,vendor,rx_name,rx_postcode,rx_district,rx_sector,rx_lat,rx_lng,rx_fulfillment_type,rx_sponsored,rx_menu,google_place_id,place_id,last_seen_live,first_seen_live,update_time,NULL\
            FROM "+table_id+"\
            WHERE process_date = max_date\
            "
        query_job = client.query(sql, job_config=job_config)
        rows = query_job.result()

        table = client.get_table(table_id)
        num_rows_begin = table.num_rows

        sql = "\
            MERGE rooscrape.foodhoover_store.rx_ref ref\
            USING (\
                SELECT rx_uid, max(vendor) as vendor, max(scrape_time) as last_seen_live, min(scrape_time) as first_seen_live,\
                REPLACE(REPLACE(REPLACE(REPLACE(rx_uid, '-ROO',''),'-UE',''), '-JE',''),'-FH','') as rx_id\
                FROM `rooscrape.foodhoover_store.rx_cx_scrape`\
                WHERE DATE(scrape_time) = @process_date\
                GROUP BY rx_uid) results\
            ON ref.rx_uid = results.rx_uid and ref.process_date = @process_date\
            WHEN MATCHED THEN\
            UPDATE SET ref.vendor=results.vendor, ref.rx_id = results.rx_id, ref.last_seen_live=GREATEST(results.last_seen_live, ref.last_seen_live), ref.first_seen_live=LEAST(results.first_seen_live, ref.first_seen_live)\
            WHEN NOT MATCHED BY TARGET THEN\
            INSERT (process_date, rx_uid, rx_id, vendor, last_seen_live, first_seen_live) VALUES (@process_date, results.rx_uid, results.rx_id, results.vendor, results.last_seen_live, results.first_seen_live)\
        "
        
        query_job = client.query(sql, job_config=job_config)  # API request
        rows = query_job.result()
        
        table = client.get_table(table_id)
        num_rows_added = table.num_rows - num_rows_begin

        t_step_logger(process_date, 'INSERT', 'SUCESS', num_rows_added)

        return num_rows_added
    except Exception as e:
        t_step_logger(process_date, 'INSERT', 'FAIL', str(e))
        return e    
    

def t_update_missing_postcodes(process_date):
    try:
        client = get_bq_client()
        table_id = 'rooscrape.foodhoover_store.rx_ref'

        sql = " \
        MERGE "+table_id+" ref \
        USING \
        (SELECT a.rx_uid, string_agg(b.postcode ORDER BY ST_DISTANCE(a.rx_point, b.postcode_point) ASC LIMIT 1) as closest_postcode \
        FROM (SELECT rx_uid, ST_GEOGPOINT(rx_lng,rx_lat) as rx_point from rooscrape.foodhoover_store.rx_ref\
        WHERE rx_postcode IS null AND (rx_lat IS NOT null AND rx_lng is NOT null)) a, rooscrape.foodhoover_store.postcode_lookup b \
        WHERE ST_DISTANCE(a.rx_point, b.postcode_point)<5000 \
        GROUP BY a.rx_uid) locations \
        ON locations.rx_uid=ref.rx_uid AND process_date=@process_date  \
        WHEN MATCHED THEN \
        UPDATE SET ref.rx_postcode=locations.closest_postcode"

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("process_date", "DATE",process_date)   
            ]
        )

        query_job = client.query(sql, job_config=job_config)
        query_job.result() 
    
        num_rows_added = query_job.num_dml_affected_rows
        t_step_logger(process_date, 'MISSING-POSTCODES', 'SUCESS', num_rows_added)
        return "DONE"
    except Exception as e:
        t_step_logger(process_date, 'MISSING-POSTCODES', 'FAIL', str(e))
        return e  

def t_crawl_roo(process_date):
    try:
        client = get_bq_client()
        ###get the roo ones missing a postcode from rx_ref, but have an id
        missing_sql ="\
            SELECT\
                rx_id\
            FROM rooscrape.foodhoover_store.rx_ref\
            WHERE vendor='ROO' AND rx_id is not null AND (update_time is null OR update_time<TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY))\
            AND process_date=@process_date\
            LIMIT 3000\
        "
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("process_date", "DATE",process_date)   
            ]
        )
        query_job = client.query(missing_sql, job_config=job_config)

        fetch_datas = []
        for row in query_job.result():
            fetch_datas.append(row['rx_id'])

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
            if isinstance(result_batch, list):
                for roo_rx in result_batch:
                    roo_rx['roo_details']['status'] = roo_rx['scrape_status']
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
                bigquery.SchemaField("rx_blob", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("status", bigquery.enums.SqlTypeNames.STRING)
                ],
            write_disposition="WRITE_APPEND",
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        )
        job = client.load_table_from_json(roo_rxs, "rooscrape.foodhoover_store.roo_cache",job_config=job_config)
        job.result()

        num_roo_cache_added = len(roo_rxs)

        ##query to merge rx_ref with the most recent row in roo_cache (we run this twice, before and after the scrape)
        sql = " \
            MERGE rooscrape.foodhoover_store.rx_ref ref\
            USING\
            (SELECT\
                CONCAT(rx_id,'-ROO') as rx_uid,\
                ARRAY_AGG(CAST(rx_id as STRING) ORDER BY scrape_time DESC LIMIT 1)[offset(0)] as rx_id,\
                ARRAY_AGG(rx_slug ORDER BY scrape_time DESC LIMIT 1)[offset(0)] as rx_slug,\
                ARRAY_AGG(scrape_time ORDER BY scrape_time DESC LIMIT 1)[offset(0)] as scrape_time,\
                ARRAY_AGG(rx_name ORDER BY scrape_time DESC LIMIT 1)[offset(0)] as rx_name,\
                ARRAY_AGG(rx_postcode ORDER BY scrape_time DESC LIMIT 1)[offset(0)] as rx_postcode,\
                ARRAY_AGG(rx_lat ORDER BY scrape_time DESC LIMIT 1)[offset(0)] as rx_lat,\
                ARRAY_AGG(rx_lng ORDER BY scrape_time DESC LIMIT 1)[offset(0)] as rx_lng,\
                ARRAY_AGG(rx_menu_page ORDER BY scrape_time DESC LIMIT 1)[offset(0)] as rx_menu,\
                ARRAY_AGG(status ORDER BY scrape_time DESC LIMIT 1)[offset(0)] as status,\
                ARRAY_AGG(rx_fulfillment_type ORDER BY scrape_time DESC LIMIT 1)[offset(0)] as rx_fulfillment_type\
            FROM rooscrape.foodhoover_store.roo_cache\
            GROUP BY rx_uid\
            ) roo_cache\
            ON ref.rx_uid = roo_cache.rx_uid and ref.vendor='ROO' and process_date=@process_date\
            WHEN MATCHED THEN\
            UPDATE SET\
                ref.rx_id = CASE roo_cache.status WHEN 'OK' THEN COALESCE(roo_cache.rx_id, ref.rx_id) ELSE ref.rx_id END,\
                ref.rx_slug = CASE roo_cache.status WHEN 'OK' THEN COALESCE(roo_cache.rx_slug, ref.rx_slug) ELSE ref.rx_slug END,\
                ref.rx_postcode = CASE roo_cache.status WHEN 'OK' THEN COALESCE(roo_cache.rx_postcode, ref.rx_postcode) ELSE ref.rx_postcode END,\
                ref.rx_name = CASE roo_cache.status WHEN 'OK' THEN COALESCE(roo_cache.rx_name, ref.rx_name) ELSE ref.rx_name END,\
                ref.rx_lat = CASE roo_cache.status WHEN 'OK' THEN COALESCE(roo_cache.rx_lat, ref.rx_lat) ELSE ref.rx_lat END,\
                ref.rx_lng = CASE roo_cache.status WHEN 'OK' THEN COALESCE(roo_cache.rx_lng, ref.rx_lng) ELSE ref.rx_lng END,\
                ref.rx_menu = CASE roo_cache.status WHEN 'OK' THEN COALESCE(roo_cache.rx_menu, ref.rx_menu) ELSE ref.rx_menu END,\
                ref.rx_fulfillment_type = CASE roo_cache.status WHEN 'OK' THEN\
                    CASE roo_cache.rx_fulfillment_type\
                        WHEN 'restaurant' THEN 'restaurant'\
                        WHEN 'deliveroo' THEN 'vendor'\
                        ELSE NULL\
                    END\
                    ELSE ref.rx_fulfillment_type\
                END,\
                ref.update_time = roo_cache.scrape_time\
            "

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("process_date", "DATE",process_date)   
            ]
        )
        query_job = client.query(sql, job_config=job_config) 
        query_job.result()

        num_roo_rx_updated = query_job.num_dml_affected_rows

        status = {
            'Rx crawled' : num_roo_to_crawl ,
            'Rx added to cache' : num_roo_cache_added,
            'Rx updated' : num_roo_rx_updated
        }

        t_step_logger(process_date, 'UPDATE-ROO', 'SUCESS',num_roo_cache_added)
        return json.dumps(status)
    except Exception as e:
        t_step_logger(process_date, 'UPDATE-ROO', 'FAIL', str(e))
        return e     

def t_crawl_ue(process_date):
    try:
        client = get_bq_client()
        ##here we exclude the previous format of rx_slug, before we picked up the GUIDs
        missing_sql ="\
            SELECT\
                rx_id\
            FROM rooscrape.foodhoover_store.rx_ref\
            WHERE vendor='UE' AND (update_time is null OR update_time<TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY))\
            AND process_date=@process_date\
            AND LENGTH(rx_id)=36\
            LIMIT 3000\
        "

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("process_date", "DATE",process_date)   
            ]
        )
        query_job = client.query(missing_sql, job_config=job_config)

        fetch_datas = []
        for row in query_job.result():
            fetch_datas.append(row['rx_id'])

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
            if isinstance(result_batch, list):
                for ue_rx in result_batch:
                    ue_rx['ue_details']['status'] = ue_rx['scrape_status']
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
                bigquery.SchemaField("rx_blob", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("status", bigquery.enums.SqlTypeNames.STRING)
                ],
            write_disposition="WRITE_APPEND",
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        )
        job = client.load_table_from_json(ue_rxs, "rooscrape.foodhoover_store.ue_cache",job_config=job_config)
        job.result()

        num_ue_cache_added = len(ue_rxs)

        ##query to merge rx_ref with the most recent row in roo_cache (we run this twice, before and after the scrape)
        sql = " \
            MERGE rooscrape.foodhoover_store.rx_ref ref\
            USING\
            (SELECT\
                CONCAT(rx_id,'-UE') as rx_uid,\
                ARRAY_AGG(rx_id ORDER BY scrape_time DESC LIMIT 1)[offset(0)] as rx_id,\
                ARRAY_AGG(rx_slug ORDER BY scrape_time DESC LIMIT 1)[offset(0)] as rx_slug,\
                ARRAY_AGG(scrape_time ORDER BY scrape_time DESC LIMIT 1)[offset(0)] as scrape_time,\
                ARRAY_AGG(rx_name ORDER BY scrape_time DESC LIMIT 1)[offset(0)] as rx_name,\
                ARRAY_AGG(rx_postcode ORDER BY scrape_time DESC LIMIT 1)[offset(0)] as rx_postcode,\
                ARRAY_AGG(rx_lat ORDER BY scrape_time DESC LIMIT 1)[offset(0)] as rx_lat,\
                ARRAY_AGG(rx_lng ORDER BY scrape_time DESC LIMIT 1)[offset(0)] as rx_lng,\
                ARRAY_AGG(rx_menu_page ORDER BY scrape_time DESC LIMIT 1)[offset(0)] as rx_menu,\
                ARRAY_AGG(status ORDER BY scrape_time DESC LIMIT 1)[offset(0)] as status,\
                ARRAY_AGG(rx_fulfillment_type ORDER BY scrape_time DESC LIMIT 1)[offset(0)] as rx_fulfillment_type\
            FROM rooscrape.foodhoover_store.ue_cache\
            GROUP BY rx_uid\
            ) ue_cache\
            ON ref.rx_uid = ue_cache.rx_uid and ref.vendor='UE' and process_date=@process_date\
            WHEN MATCHED THEN\
            UPDATE SET\
                ref.rx_id = CASE ue_cache.status WHEN 'OK' THEN COALESCE(ue_cache.rx_id, ref.rx_id) ELSE ref.rx_id END,\
                ref.rx_slug = CASE ue_cache.status WHEN 'OK' THEN COALESCE(ue_cache.rx_slug, ref.rx_slug) ELSE ref.rx_slug END,\
                ref.rx_postcode = CASE ue_cache.status WHEN 'OK' THEN COALESCE(ue_cache.rx_postcode, ref.rx_postcode) ELSE ref.rx_postcode END,\
                ref.rx_name = CASE ue_cache.status WHEN 'OK' THEN COALESCE(ue_cache.rx_name, ref.rx_name) ELSE ref.rx_name END,\
                ref.rx_lat = CASE ue_cache.status WHEN 'OK' THEN COALESCE(ue_cache.rx_lat, ref.rx_lat) ELSE ref.rx_lat END,\
                ref.rx_lng = CASE ue_cache.status WHEN 'OK' THEN COALESCE(ue_cache.rx_lng, ref.rx_lng) ELSE ref.rx_lng END,\
                ref.rx_menu = CASE ue_cache.status WHEN 'OK' THEN COALESCE(ue_cache.rx_menu, ref.rx_menu) ELSE ref.rx_menu END,\
                ref.rx_fulfillment_type = CASE ue_cache.status WHEN 'OK' THEN COALESCE(ue_cache.rx_fulfillment_type,ref.rx_fulfillment_type) END,\
                ref.update_time = ue_cache.scrape_time\
            "

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("process_date", "DATE",process_date)   
            ]
        )
        query_job = client.query(sql, job_config=job_config) 
        query_job.result()

        num_ue_rx_updated = query_job.num_dml_affected_rows

        status = {
            'Rx crawled' : num_ue_to_crawl ,
            'Rx added to cache' : num_ue_cache_added,
            'Rx updated' : num_ue_rx_updated
        }

        t_step_logger(process_date, 'UPDATE-UE', 'SUCESS',num_ue_cache_added)
        return json.dumps(status)
    except Exception as e:
        t_step_logger(process_date, 'UPDATE-UE', 'FAIL', str(e))
        return e 

def t_update_je_fh(process_date):
    try:
        sql = "\
            MERGE rooscrape.foodhoover_store.rx_ref ref\
            USING \
            (SELECT\
                rx_uid,\
                ARRAY_AGG(rx_id ORDER BY scrape_time DESC LIMIT 1)[offset(0)] as rx_id,\
                ARRAY_AGG(rx_slug ORDER BY scrape_time DESC LIMIT 1)[offset(0)] as rx_slug,\
                ARRAY_AGG(scrape_time ORDER BY scrape_time DESC LIMIT 1)[offset(0)] as scrape_time,\
                ARRAY_AGG(rx_name ORDER BY scrape_time DESC LIMIT 1)[offset(0)] as rx_name,\
                ARRAY_AGG(vendor ORDER BY scrape_time DESC LIMIT 1)[offset(0)] as vendor,\
                ARRAY_AGG(rx_postcode ORDER BY scrape_time DESC LIMIT 1)[offset(0)] as rx_postcode,\
                ARRAY_AGG(rx_lat ORDER BY scrape_time DESC LIMIT 1)[offset(0)] as rx_lat,\
                ARRAY_AGG(rx_lng ORDER BY scrape_time DESC LIMIT 1)[offset(0)] as rx_lng,\
                ARRAY_AGG(rx_menu ORDER BY scrape_time DESC LIMIT 1)[offset(0)] as rx_menu,\
                ARRAY_AGG(rx_sponsored ORDER BY scrape_time DESC LIMIT 1)[offset(0)] as rx_sponsored,\
                ARRAY_AGG(rx_fulfillment_type ORDER BY scrape_time DESC LIMIT 1)[offset(0)] as rx_fulfillment_type\
            FROM rooscrape.foodhoover_store.je_fh_cache\
            GROUP BY rx_uid\
            ) je_fh_cache\
            ON ref.rx_uid = je_fh_cache.rx_uid and ref.vendor IN ('JE','FH') and process_date=@process_date\
            WHEN MATCHED THEN\
            UPDATE SET\
                ref.rx_id = COALESCE(je_fh_cache.rx_id, ref.rx_id),\
                ref.rx_slug = COALESCE(je_fh_cache.rx_slug, ref.rx_slug),\
                ref.rx_name = COALESCE(je_fh_cache.rx_name, ref.rx_name),\
                ref.vendor = COALESCE(je_fh_cache.vendor, ref.vendor),\
                ref.rx_postcode = COALESCE(je_fh_cache.rx_postcode, ref.rx_postcode),\
                ref.rx_lat = COALESCE(je_fh_cache.rx_lat, ref.rx_lat),\
                ref.rx_lng = COALESCE(je_fh_cache.rx_lng, ref.rx_lng),\
                ref.rx_fulfillment_type = COALESCE(je_fh_cache.rx_fulfillment_type, ref.rx_fulfillment_type),\
                ref.rx_sponsored = COALESCE(je_fh_cache.rx_sponsored, ref.rx_sponsored),\
                ref.rx_menu = COALESCE(je_fh_cache.rx_menu, ref.rx_menu),\
                ref.update_time = je_fh_cache.scrape_time\
            "

        client = bigquery.Client.from_service_account_json('rooscrape-gbq.json') 
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("process_date", "DATE",process_date)   
            ]
        )

        query_job = client.query(sql, job_config=job_config) 
        query_job.result()
        num_rx_updated = query_job.num_dml_affected_rows
        t_step_logger(process_date, 'UPDATE-JE-FH', 'SUCESS',num_rx_updated)
        return str(num_rx_updated)
    except Exception as e:
        t_step_logger(process_date, 'UPDATE-JE-FH', 'FAIL', str(e))
        return e 

def t_update_geos(process_date):
    try:
        client = get_bq_client()
        table_id = 'rooscrape.foodhoover_store.rx_ref'

        sql = " \
        MERGE "+table_id+" ref \
        USING rooscrape.foodhoover_store.postcode_lookup postcodes \
        ON postcodes.postcode=ref.rx_postcode AND process_date=@process_date \
        WHEN MATCHED THEN UPDATE \
        SET ref.rx_district=postcodes.postcode_district, ref.rx_sector=postcodes.postcode_sector, ref.rx_lat = COALESCE(ref.rx_lat,postcodes.latitude), ref.rx_lng = COALESCE(ref.rx_lng, postcodes.longitude)"
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("process_date", "DATE",process_date)   
            ]
        )

        query_job = client.query(sql, job_config=job_config) 
        query_job.result()
    
        num_rows_added = query_job.num_dml_affected_rows
        t_step_logger(process_date, 'UPDATE-GEOS', 'SUCESS', num_rows_added)
        return str(num_rows_added)
    except Exception as e:
        t_step_logger(process_date, 'UPDATE-GEOS', 'FAIL', str(e))
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


def t_get_places(process_date):

    f = open('secrets.json')
    secrets = json.load(f)
    api_key = secrets['scrape_map_key']

    try: 
        client = get_bq_client()
        ##query to merge rx_ref with the most recent row in places_cache (we run this twice, before and after the scrape)
        merge_sql = " \
        MERGE rooscrape.foodhoover_store.rx_ref ref \
        USING \
        (SELECT\
            rx_uid,\
            STRING_AGG(place_id ORDER BY scrape_time DESC LIMIT 1) as place_id,\
            STRING_AGG(status ORDER BY scrape_time DESC LIMIT 1) as status,\
            MAX(scrape_time) as scrape_time, \
        FROM rooscrape.foodhoover_store.places_cache \
        WHERE scrape_time>='2021-09-11'\
        GROUP BY rx_uid) places_cache \
        ON ref.rx_uid = places_cache.rx_uid AND ref.process_date=@process_date \
        WHEN MATCHED THEN \
        UPDATE SET\
            ref.google_place_id= (CASE WHEN places_cache.status = 'OK' THEN COALESCE(places_cache.place_id, ref.google_place_id) ELSE ref.google_place_id END),\
            ref.places_update=places_cache.scrape_time\
        "

        merge_job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("process_date", "DATE",process_date)   
            ]
        )

        query_job = client.query(merge_sql, job_config=merge_job_config) 
        query_job.result()

        ##get the places still to scrape
        client = get_bq_client()
        bqstorageclient = get_bq_storage()

        sql = "SELECT rx_uid, rx_name, rx_lat, rx_lng from rooscrape.foodhoover_store.rx_ref \
            WHERE rx_lat is not null and rx_lng is not null and rx_name is not null\
            AND (places_update is null OR places_update<TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY))\
            AND rx_ref.process_date=@process_date\
            LIMIT 30000\
            "
        place_results = (
            client.query(sql, job_config=merge_job_config)
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
                            return {'rx_uid':fetch_data['rx_uid'],'place_id':place_id,'scrape_time':time.time(), 'status':'OK'}
                        elif response['status']=='ZERO_RESULTS':
                            filtered_name = ''.join([x if (x.isalpha() or x.isspace()) else '' for x in fetch_data['rx_name']])
                            uri = make_places_uri(filtered_name, fetch_data['rx_lat'], fetch_data['rx_lng'], fetch_data['api_key'])
                            async with session.get(uri) as response:
                                response = await response.read()
                                response = json.loads(response.decode('utf8'))
                                if response['status']=='OK':
                                    place_id = response['candidates'][0]['place_id']
                                    return {'rx_uid':fetch_data['rx_uid'],'place_id':place_id,'scrape_time':time.time(), 'status':'OK'}
                                else:
                                     return {'rx_uid':fetch_data['rx_uid'],'place_id':None,'scrape_time':time.time(), 'status':response['status']}
                        else:
                            return {'rx_uid':fetch_data['rx_uid'],'place_id':None,'scrape_time':time.time(),'status':response['status']}
                except Exception as e:
                    return {'rx_uid':fetch_data['rx_uid'],'place_id':None,'scrape_time':time.time(), 'status': 'Error: Fetch Error '+ str(e)}

        try:
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy()) ##on windows, this is to stop an error that the loop isn't closed
        except:
            pass

        scrape_results = asyncio.run(url_batch(fetch_datas, places_fetch_function, 50))
        
        if len(scrape_results)>0: ###write to BQ
            print('Append: '+str(len(scrape_results)))

            client = bigquery.Client.from_service_account_json('rooscrape-gbq.json')   
            #write the ue_data
            load_job_config = bigquery.LoadJobConfig(
                schema=[
                    bigquery.SchemaField("rx_uid", bigquery.enums.SqlTypeNames.STRING),
                    bigquery.SchemaField("place_id", bigquery.enums.SqlTypeNames.STRING),
                    bigquery.SchemaField("scrape_time", bigquery.enums.SqlTypeNames.TIMESTAMP),
                    bigquery.SchemaField("status", bigquery.enums.SqlTypeNames.STRING)
                    ],
                write_disposition="WRITE_APPEND",
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            )
            job = client.load_table_from_json(scrape_results, "rooscrape.foodhoover_store.places_cache",job_config=load_job_config)
            job.result()

        ##now do the merge again
        query_job = client.query(merge_sql, job_config=merge_job_config)
        query_job.result()
        
        num_rows_added = query_job.num_dml_affected_rows
        t_step_logger(process_date, 'GET-PLACES', 'SUCESS', num_rows_added)
        return num_rows_added
    except Exception as e:
        print(str(e))
        t_step_logger(process_date, 'GET-PLACES', 'FAIL', str(e))
        return e        


def t_places_proc(process_date):
    try:
        sql = """\
        CREATE TEMPORARY FUNCTION\
            LevenshteinDistance(in_a string,in_b string)\
            RETURNS INT64\
            LANGUAGE js AS\
            '''\
            function lev(in_a,in_b){\
                var a = in_a.toLowerCase();\
                var b = in_b.toLowerCase();\
                max_length = Math.max(a.length,b.length);\
                if(a.length == 0) return b.length/max_length;\
                if(b.length == 0) return a.length/max_length;\
                var matrix = [];\
                var i;\
                for(i = 0; i <= b.length; i++){\
                    matrix[i] = [i];\
                }\
                var j;\
                for(j = 0; j <= a.length; j++){\
                    matrix[0][j] = j;\
                }\
                for(i = 1; i <= b.length; i++){\
                    for(j = 1; j <= a.length; j++){\
                        if(b.charAt(i-1) == a.charAt(j-1)){\
                            matrix[i][j] = matrix[i-1][j-1];\
                        } else {\
                            matrix[i][j] = Math.min(matrix[i-1][j-1] + 1, Math.min(matrix[i][j-1] + 1, matrix[i-1][j] + 1));\
                        }\
                    }\
                }\
                return matrix[b.length][a.length]/max_length;\
            }\
            ''';\
        MERGE `rooscrape.foodhoover_store.rx_ref` ref\
        USING (\
            SELECT\
                good_candidates.rx_uid,\
                MAX(good_candidates.rx_name) as rx_name,\
                TO_HEX(MD5(ARRAY_TO_STRING(array_agg(DISTINCT good_candidates.rx_uid_candidate ORDER BY good_candidates.rx_uid_candidate),','))) as place_id\
            FROM (\
                SELECT\
                rx_uid,\
                rx_uid_candidate,\
                rx_name,\
                rx_name_candidate,\
                google_place_id,\
                google_place_id_candidate\
                FROM (\
                    SELECT\
                        ref_a.rx_uid as rx_uid,\
                        ref_b.rx_uid as rx_uid_candidate,\
                        REGEXP_REPLACE(ref_a.rx_name, r'\((.*?)\)', '') as rx_name,\
                        REGEXP_REPLACE(ref_b.rx_name, r'\((.*?)\)', '') as rx_name_candidate,\
                        ref_a.google_place_id as google_place_id,\
                        ref_b.google_place_id as google_place_id_candidate\
                    FROM `rooscrape.foodhoover_store.rx_ref` ref_a\
                    JOIN `rooscrape.foodhoover_store.rx_ref` ref_b\
                    ON ST_DWITHIN(ST_GEOGPOINT(ref_a.rx_lng,ref_a.rx_lat),ST_GEOGPOINT(ref_b.rx_lng,ref_b.rx_lat),800)\
                    WHERE ref_a.process_date = @process_date and ref_b.process_date = @process_date\
                    AND ref_a.rx_name IS NOT NULL and ref_b.rx_name IS NOT NULL\
                ) candidates\
                WHERE\
                    (candidates.google_place_id = candidates.google_place_id_candidate)\
                    OR\
                    (candidates.google_place_id IS NULL OR candidates.google_place_id_candidate IS NULL)\
                ) good_candidates\
            WHERE\
                ((LevenshteinDistance(good_candidates.rx_name, good_candidates.rx_name_candidate) < 0.33) AND (good_candidates.google_place_id IS NULL OR good_candidates.google_place_id_candidate IS NULL))\
                OR\
                (good_candidates.google_place_id = good_candidates.google_place_id_candidate)\
            GROUP BY good_candidates.rx_uid\
            ) place_ids\
        ON place_ids.rx_uid = ref.rx_uid and ref.process_date=@process_date\
        WHEN MATCHED AND ref.process_date=@process_date THEN\
            UPDATE SET ref.place_id=place_ids.place_id\
        WHEN NOT MATCHED BY SOURCE AND ref.process_date=@process_date THEN\
            UPDATE SET ref.place_id= TO_HEX(MD5(ref.rx_uid))\
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("process_date", "DATE",process_date)   
            ]
        )

        client = get_bq_client()
        query_job = client.query(sql, job_config=job_config)
        query_job.result()
                
        num_rows_added = query_job.num_dml_affected_rows
        t_step_logger(process_date, 'PROC-PLACES', 'SUCESS', num_rows_added)
        return num_rows_added
    except Exception as e:
        t_step_logger(process_date, 'PROC-PLACES', 'FAIL', str(e))
        return e   

def t_places_table(process_date):
    try:
        client = get_bq_client()
        query_job = client.query("DELETE FROM rooscrape.foodhoover_store.places WHERE 1=1")
        query_job.result()
        
        sql ="\
                INSERT INTO rooscrape.foodhoover_store.places (place_id, place_name, place_label, place_sector, place_lat, place_lng, place_location, place_vendors)\
                SELECT\
                    place_id,\
                    place_name,\
                    CONCAT(place_name,': ',place_sector, ' (',ARRAY_TO_STRING(place_vendors,','),')') as place_label,\
                    place_sector,\
                    place_lat,\
                    place_lng,\
                    SAFE.ST_GEOGPOINT(place_lat, place_lng) as place_location,\
                    place_vendors\
                FROM (\
                    SELECT\
                        place_id,\
                        APPROX_QUANTILES(rx_lat, 10)[OFFSET(5)] AS place_lat,\
                        APPROX_QUANTILES(rx_lng, 10)[OFFSET(5)] AS place_lng,\
                        APPROX_QUANTILES(rx_name, 10)[OFFSET(5)] AS place_name,\
                        APPROX_QUANTILES(rx_sector, 10)[OFFSET(5)] AS place_sector,\
                        APPROX_QUANTILES(rx_postcode, 10)[OFFSET(5)] AS place_postcode,\
                        ARRAY_AGG(distinct(vendor) ORDER BY vendor) as place_vendors\
                    FROM `rooscrape.foodhoover_store.rx_ref`\
                    WHERE process_date=@process_date and rx_postcode is not null\
                    GROUP BY place_id\
                ) places\
            "

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("process_date", "DATE",process_date)   
            ]
        )

        query_job = client.query(sql, job_config=job_config)
        query_job.result()
        
        num_rows_added = query_job.num_dml_affected_rows
        t_step_logger(process_date, 'CREATE-PLACES', 'SUCESS', num_rows_added)
        return num_rows_added
    except Exception as e:
        t_step_logger(process_date, 'CREATE-PLACES', 'FAIL', str(e))
        return e   

def t_agg_rx_cx_sector(process_date):
    try:
        client = get_bq_client()
        table_id = 'rooscrape.foodhoover_store.rx_cx_sector'

        table = client.get_table(table_id)
        num_rows_begin = table.num_rows

        if process_date == 'full':
            sql = "DELETE FROM "+table_id+" WHERE 1=1" ##empty the current table
            query_job = client.query(sql)  # API request
            query_job.result()
            job_config = bigquery.QueryJobConfig(
                query_parameters=[]
            )
            where_clause = ''
        else:
            ##delete any data from that date
            sql = "DELETE FROM "+table_id+" WHERE scrape_date='"+process_date+"'"
            query_job = client.query(sql)  # API request
            query_job.result()

            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("process_date", "DATE",process_date)   
                ]
            )
            where_clause = 'WHERE DATE(scrape_time) = @process_date'

        sql = """
        INSERT INTO rooscrape.foodhoover_store.rx_cx_sector (scrape_date, rx_uid, vendor, place_id, sectors_seen)
        SELECT DATE(scrape_time) as scrape_date, raw.rx_uid, MAX(raw.vendor) as vendor, MAX(ref.place_id) as place_id, ARRAY_AGG(DISTINCT pc.postcode_sector IGNORE NULLS) as sectors_seen
        FROM rooscrape.foodhoover_store.rx_cx_scrape raw
        LEFT JOIN rooscrape.foodhoover_store.postcode_lookup pc on raw.cx_postcode=pc.postcode
        LEFT JOIN rooscrape.foodhoover_store.rx_ref ref on ref.rx_uid=raw.rx_uid
        """+where_clause+"""
        GROUP BY DATE(scrape_time), rx_uid
        """

        query_job = client.query(sql, job_config=job_config)
        query_job.result()

        table = client.get_table(table_id)
        num_rows_added = table.num_rows - num_rows_begin

        t_step_logger(process_date, 'AGG-RX-CX-SECTOR', 'SUCESS', num_rows_added)
        return num_rows_added  
    except Exception as e:
        t_step_logger(process_date, 'AGG-RX-CX-SECTOR', 'FAIL', str(e))
        return e   


def t_agg_results_district_fulfillment(process_date):
    try:
        client = get_bq_client()
        table_id = 'rooscrape.foodhoover_store.agg_district_fulfillment_day'

        table = client.get_table(table_id)
        num_rows_begin = table.num_rows

        if process_date == 'full':
            sql = "DELETE FROM "+table_id+" WHERE 1=1" ##empty the current table
            query_job = client.query(sql)  # API request
            query_job.result()
            job_config = bigquery.QueryJobConfig(
                query_parameters=[]
            )
            where_clause = ''
        else:
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("process_date", "DATE",process_date)   
                ]
            )
            
            where_clause = ' scrape_date = @process_date'

        sql = """
            MERGE rooscrape.foodhoover_store.agg_district_fulfillment_day agg
            USING (
                SELECT scrape_date, geo.postcode_district, scrape.vendor, ref.rx_fulfillment_type, count(distinct scrape.rx_uid) as rx_num 
                FROM rooscrape.foodhoover_store.rx_cx_sector scrape, UNNEST(sectors_seen) as sector
                LEFT JOIN rooscrape.foodhoover_store.geo_mappings geo on geo.postcode_sector=sector
                LEFT JOIN rooscrape.foodhoover_store.rx_ref ref on ref.rx_uid=scrape.rx_uid
                WHERE ref.rx_fulfillment_type is not null """+"AND"+where_clause+"""
                GROUP BY scrape_date, geo.postcode_district, scrape.vendor, ref.rx_fulfillment_type
                UNION ALL
                SELECT scrape_date, geo.postcode_district, scrape.vendor, 'all', count(distinct scrape.rx_uid) as rx_num 
                FROM rooscrape.foodhoover_store.rx_cx_sector scrape, UNNEST(sectors_seen) as sector
                LEFT JOIN rooscrape.foodhoover_store.geo_mappings geo on geo.postcode_sector=sector
                """+"WHERE"+where_clause+"""
                GROUP BY scrape_date, geo.postcode_district, scrape.vendor
                ) results
            ON results.scrape_date=agg.scrape_date and results.vendor=agg.vendor and results.postcode_district=agg.postcode_district AND results.rx_fulfillment_type=agg.fulfillment_type AND results.postcode_district IS NOT null
            WHEN MATCHED THEN
                UPDATE SET agg.rx_num=results.rx_num
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (scrape_date, postcode_district, vendor, fulfillment_type, rx_num) VALUES (results.scrape_date, results.postcode_district, results.vendor, results.rx_fulfillment_type, results.rx_num)
        """
        query_job = client.query(sql, job_config=job_config)
        query_job.result()

        table = client.get_table(table_id)
        num_rows_added = table.num_rows - num_rows_begin

        t_step_logger(process_date, 'AGGREGATE-DISTRICT-FULFILLMENT', 'SUCESS', num_rows_added)
        return num_rows_added  
    except Exception as e:
        t_step_logger(process_date, 'AGGREGATE-DISTRICT-FULFILLMENT', 'FAIL', str(e))
        return e   

def t_agg_results_sector_fulfillment(process_date):
    try:
        client = get_bq_client()
        table_id = 'rooscrape.foodhoover_store.agg_sector_fulfillment_day'

        table = client.get_table(table_id)
        num_rows_begin = table.num_rows

        if process_date == 'full':
            sql = "DELETE FROM "+table_id+" WHERE 1=1" ##empty the current table
            query_job = client.query(sql)  # API request
            query_job.result()
            job_config = bigquery.QueryJobConfig(
                query_parameters=[ 
                ]
            )
            where_clause = ''
        else:
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("process_date", "DATE",process_date)   
                ]
            )
            where_clause = ' scrape_date = @process_date'

        sql = """
            MERGE rooscrape.foodhoover_store.agg_sector_fulfillment_day agg
            USING (
                SELECT scrape_date, geo.postcode_sector, scrape.vendor, ref.rx_fulfillment_type, count(distinct scrape.rx_uid) as rx_num 
                FROM rooscrape.foodhoover_store.rx_cx_sector scrape, UNNEST(sectors_seen) as sector
                LEFT JOIN rooscrape.foodhoover_store.geo_mappings geo on geo.postcode_sector=sector
                LEFT JOIN rooscrape.foodhoover_store.rx_ref ref on ref.rx_uid=scrape.rx_uid
                WHERE ref.rx_fulfillment_type is not null """+"AND"+where_clause+"""
                GROUP BY scrape_date, geo.postcode_sector, scrape.vendor, ref.rx_fulfillment_type
                UNION ALL
                SELECT scrape_date, geo.postcode_sector, scrape.vendor, 'all', count(distinct scrape.rx_uid) as rx_num 
                FROM rooscrape.foodhoover_store.rx_cx_sector scrape, UNNEST(sectors_seen) as sector
                LEFT JOIN rooscrape.foodhoover_store.geo_mappings geo on geo.postcode_sector=sector
                """+"WHERE"+where_clause+"""
                GROUP BY scrape_date, geo.postcode_sector, scrape.vendor
                ) results
            ON results.scrape_date=agg.scrape_date and results.vendor=agg.vendor and results.postcode_sector=agg.postcode_sector and results.rx_fulfillment_type=agg.fulfillment_type AND results.postcode_sector IS NOT null
            WHEN MATCHED THEN
                UPDATE SET agg.rx_num=results.rx_num
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (scrape_date, postcode_sector, vendor,  fulfillment_type, rx_num) VALUES (results.scrape_date, results.postcode_sector, results.vendor, results.rx_fulfillment_type, results.rx_num)
        """

        query_job = client.query(sql, job_config=job_config)
        query_job.result()

        table = client.get_table(table_id)
        num_rows_added = table.num_rows - num_rows_begin

        t_step_logger(process_date, 'AGGREGATE-SECTOR-FULFILLMENT', 'SUCESS', num_rows_added)
        return num_rows_added  
    except Exception as e:
        t_step_logger(process_date, 'AGGREGATE-SECTOR-FULFILLMENT', 'FAIL', str(e))
        return e  

def t_agg_results_country_fulfillment(process_date):
    try:
        client = get_bq_client()
        table_id = 'rooscrape.foodhoover_store.agg_country_fulfillment_day'

        table = client.get_table(table_id)
        num_rows_begin = table.num_rows

        if process_date == 'full':
            sql = "DELETE FROM "+table_id+" WHERE 1=1" ##empty the current table
            query_job = client.query(sql)  # API request
            query_job.result()

            job_config = bigquery.QueryJobConfig(
                query_parameters=[]
            )
            where_clause = ''
        else:
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("process_date", "DATE",process_date)   
                ]
            )
            where_clause = 'WHERE scrape_date = @process_date'

        sql = """
            MERGE rooscrape.foodhoover_store.agg_country_fulfillment_day agg
            USING (
                WITH results AS
                (SELECT 
                    scrape_date, 
                    scrape.vendor, 
                    count(distinct scrape.rx_uid) as rx_num_total,
                    array_agg(distinct sector) as sectors_seen_total, 
                    count(distinct CASE WHEN ref.rx_fulfillment_type='vendor' THEN scrape.rx_uid ELSE null END) as rx_num_vendor,
                    array_agg(distinct CASE WHEN ref.rx_fulfillment_type='vendor' THEN sector ELSE null END) as sectors_seen_vendor,
                    count(distinct CASE WHEN ref.rx_fulfillment_type='restaurant' THEN scrape.rx_uid ELSE null END) as rx_num_restaurant,
                    array_agg(distinct CASE WHEN ref.rx_fulfillment_type='restaurant' THEN sector ELSE null END) as sectors_seen_restaurant
                FROM rooscrape.foodhoover_store.rx_cx_sector scrape, UNNEST(sectors_seen) as sector
                LEFT JOIN rooscrape.foodhoover_store.geo_mappings geo on geo.postcode_sector=sector
                LEFT JOIN rooscrape.foodhoover_store.rx_ref ref on ref.rx_uid=scrape.rx_uid
                """+where_clause+"""
                GROUP BY scrape_date, vendor
                ) 
                SELECT 'uk' as country, scrape_date, 'all' as fulfillment_type, vendor, max(rx_num_total) as rx_num, SUM(sectors.population) as delivery_population
                FROM results, UNNEST(results.sectors_seen_total) as sector_seen_total
                LEFT JOIN rooscrape.foodhoover_store.sectors sectors ON sectors.sector=sector_seen_total
                GROUP BY country, results.scrape_date, results.vendor
                UNION ALL
                SELECT 'uk' as country, scrape_date, 'vendor' as fulfillment_type, vendor, max(rx_num_vendor) as rx_num, SUM(sectors.population) as delivery_population
                FROM results, UNNEST(results.sectors_seen_vendor) as sector_seen_vendor
                LEFT JOIN rooscrape.foodhoover_store.sectors sectors ON sectors.sector=sector_seen_vendor
                GROUP BY country, results.scrape_date, results.vendor
                UNION ALL
                SELECT 'uk' as country, scrape_date, 'restaurant' as fulfillment_type, vendor, max(rx_num_restaurant) as rx_num, SUM(sectors.population) as delivery_population
                FROM results, UNNEST(results.sectors_seen_restaurant) as sector_seen_restaurant
                LEFT JOIN rooscrape.foodhoover_store.sectors sectors ON sectors.sector=sector_seen_restaurant
                GROUP BY country, results.scrape_date, results.vendor
            ) final
            ON final.scrape_date=agg.scrape_date and final.vendor=agg.vendor and final.fulfillment_type=agg.fulfillment_type
            WHEN MATCHED THEN
                UPDATE SET agg.rx_num=final.rx_num, agg.delivery_population=final.delivery_population
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (scrape_date, country, vendor, fulfillment_type, rx_num, delivery_population) VALUES (final.scrape_date, final.country, final.vendor, final.fulfillment_type,final.rx_num, final.delivery_population)
            """

        query_job = client.query(sql, job_config=job_config)
        query_job.result()

        table = client.get_table(table_id)
        num_rows_added = table.num_rows - num_rows_begin

        t_step_logger(process_date, 'AGGREGATE-COUNTRY-FULFILLMENT', 'SUCESS', num_rows_added)
        return num_rows_added  
    except Exception as e:
        t_step_logger(process_date, 'AGGREGATE-COUNTRY-FULFILLMENT', 'FAIL', str(e))
        return e  

def t_agg_delivery_zone(process_date):
    try:
        process_date = datetime.strptime(process_date, '%Y-%m-%d').date()
        process_date_start = process_date - timedelta(days=14)
        process_date = process_date.strftime('%Y-%m-%d')
        process_date_start = process_date_start.strftime('%Y-%m-%d')

        client = get_bq_client()
        table_id = 'rooscrape.foodhoover_store.agg_delivery_zone'
        table = client.get_table(table_id)
        num_rows_start = table.num_rows

        sql ="""\
            MERGE rooscrape.foodhoover_store.agg_delivery_zone agg
            USING(
                SELECT
                    places.place_id,
                    bysector.vendor,
                    ST_SIMPLIFY(ST_UNION_AGG(sectors.geometry),50) as delivery_zone,
                    SUM(sectors.population) as delivery_population,
                    max(places.place_name) as place_name,
                    max(places.place_lat) as place_lat,
                    max(places.place_lng) as place_lng,
                    ANY_VALUE(vendor_rx) as vendor_rx,
                    ARRAY_AGG(DISTINCT(sectors.sector) IGNORE NULLS) as sectors_covered
                FROM (
                    SELECT
                    place_id,
                    vendor,
                    sector,
                    ARRAY_AGG(DISTINCT(rx_uid) IGNORE NULLS) as vendor_rx,
                    FROM rooscrape.foodhoover_store.rx_cx_sector, UNNEST(sectors_seen) as sector
                    WHERE scrape_date>=@process_date_start AND scrape_date<=@process_date
                    GROUP BY place_id,vendor, sector
                    ORDER BY place_id,vendor, sector) bysector
                LEFT JOIN rooscrape.foodhoover_store.sectors sectors on sectors.sector = bysector.sector
                RIGHT JOIN rooscrape.foodhoover_store.places places on places.place_id = bysector.place_id
                GROUP BY places.place_id, bysector.vendor) as final
            ON agg.process_date=@process_date and agg.process_date_start=@process_date_start and agg.place_id=final.place_id and agg.vendor=final.vendor
            WHEN MATCHED THEN
                UPDATE SET agg.delivery_zone=final.delivery_zone, agg.delivery_population=final.delivery_population, agg.place_name=final.place_name, agg.place_lng=final.place_lng, agg.place_lat=final.place_lat, agg.vendor_rx=final.vendor_rx, agg.sectors_covered=final.sectors_covered
            WHEN NOT MATCHED THEN
                INSERT (process_date, process_date_start, place_id, vendor, delivery_zone, delivery_population, place_name, place_lng, place_lat, vendor_rx, sectors_covered) VALUES (@process_date,@process_date_start, final.place_id, final.vendor, final.delivery_zone, final.delivery_population, final.place_name, final.place_lng, final.place_lat, final.vendor_rx, final.sectors_covered)
            """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("process_date", "DATE",process_date),
                bigquery.ScalarQueryParameter("process_date_start", "DATE",process_date_start)   
            ]
        )

        query_job = client.query(sql, job_config=job_config)  # API request
        query_job.result()

        table = client.get_table(table_id)
        num_rows_added = table.num_rows - num_rows_start

        t_step_logger(process_date, 'AGG-DELIVERY-ZONE', 'SUCESS', num_rows_added)
        return num_rows_added
    except Exception as e:
        t_step_logger(process_date, 'AGG-DELIVERY-ZONE', 'FAIL', str(e))
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
    instance = 'foodhoover-web'

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

def generic_exporter(process_date, bq_table, sql_table, sql_schema, bq_select_sql, sql_create_statement, write_mode):
    ##get a reference to the data to export
    #if write_mode =='overwrite':
    #    target_table = write_bq_table(bq_table, bq_select_sql)
    #else:

    target_table = write_bq_table('rooscrape.foodhoover_store.temp_table', bq_select_sql)

    ##write that table to GCS
    gcs_folder = sql_table+'-'+process_date
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

def t_export_rx_ref(process_date):
    try:
        bq_table = 'rooscrape.foodhoover_store.rx_ref'
        sql_table = 'rx_ref'
        sql_schema = ['rx_uid','rx_slug','vendor','rx_name','rx_postcode','rx_district','rx_sector', 'rx_lat', 'rx_lng','rx_menu','place_id']
        bq_select_sql = "SELECT rx_uid, rx_slug, vendor, rx_name, rx_postcode, rx_district, rx_sector, rx_lat, rx_lng, rx_menu, place_id FROM foodhoover_store.rx_ref WHERE process_date='"+process_date+"'"
        write_mode = 'overwrite'
        sql_create_statement = "table_schemas/rx_ref"

        status = generic_exporter(process_date, bq_table, sql_table, sql_schema, bq_select_sql, sql_create_statement, write_mode)

        t_step_logger(process_date, 'EXPORT-RX-REF', 'SUCESS', status)
        return status
    except Exception as e:
        t_step_logger(process_date, 'EXPORT-RX-REF', 'FAIL', str(e))
        return e

def t_export_places(process_date):
    try:
        bq_table = 'rooscrape.foodhoover_store.places'
        sql_table = 'places'
        sql_schema = ['place_id', 'place_name', 'place_label', 'place_sector', 'place_lat', 'place_lng', 'place_location','place_vendors']
        bq_select_sql = "SELECT place_id, place_name, place_label, place_sector, place_lat, place_lng, TO_HEX(ST_ASBINARY(place_location)) as place_location, REPLACE(REPLACE(TO_JSON_STRING(place_vendors),'[','{'),']','}') as place_vendors FROM foodhoover_store.places"
        write_mode = 'overwrite'
        sql_create_statement = "table_schemas/places"

        status = generic_exporter(process_date, bq_table, sql_table, sql_schema, bq_select_sql, sql_create_statement, write_mode)

        t_step_logger(process_date, 'EXPORT-PLACES', 'SUCESS', status)
        return status
    except Exception as e:
        t_step_logger(process_date, 'EXPORT-PLACES', 'FAIL', str(e))
        return e

def t_export_agg_district_fulfillment_day(process_date):

    if process_date=='full':
        write_mode = 'overwrite'
        where_clause = ''
    else:
        write_mode = 'append'
        where_clause = "WHERE scrape_date='"+process_date+"'"

    try:
        bq_table = 'rooscrape.foodhoover_store.agg_district_fulfillment_day'
        sql_table = 'agg_district_fulfillment_day'
        sql_schema = ['scrape_date','postcode_district', 'vendor', 'fulfillment_type','rx_num',]
        bq_select_sql = "SELECT scrape_date, postcode_district, vendor, fulfillment_type, rx_num FROM rooscrape.foodhoover_store.agg_district_fulfillment_day "+where_clause
        sql_create_statement = "table_schemas/agg_district_fulfillment_day"

        status = generic_exporter(process_date, bq_table, sql_table, sql_schema, bq_select_sql, sql_create_statement, write_mode)

        t_step_logger(process_date, 'EXPORT-AGG-DISTRICT-FULFILLMENT-DAY', 'SUCESS', status)
        return status
    except Exception as e:
        t_step_logger(process_date, 'EXPORT-AGG-DISTRICT-FULFILLMENT-DAY', 'FAIL', str(e))
        return e

def t_export_agg_sector_fulfillment_day(process_date):

    if process_date=='full':
        write_mode = 'overwrite'
        where_clause = ''
    else:
        write_mode = 'append'
        where_clause = "WHERE scrape_date='"+process_date+"'"

    try:
        bq_table = 'rooscrape.foodhoover_store.agg_sector_fulfillment_day'
        sql_table = 'agg_sector_fulfillment_day'
        sql_schema = ['scrape_date','postcode_sector', 'vendor', 'fulfillment_type','rx_num',]
        bq_select_sql = "SELECT scrape_date, postcode_sector, vendor, fulfillment_type,rx_num FROM rooscrape.foodhoover_store.agg_sector_fulfillment_day "+where_clause
        sql_create_statement = "table_schemas/agg_sector_fulfillment_day"

        status = generic_exporter(process_date, bq_table, sql_table, sql_schema, bq_select_sql, sql_create_statement, write_mode)

        t_step_logger(process_date, 'EXPORT-AGG-SECTOR-FULFILLMENT-DAY', 'SUCESS', status)
        return status
    except Exception as e:
        t_step_logger(process_date, 'EXPORT-AGG-SECTOR-FULFILLMENT-DAY', 'FAIL', str(e))
        return e

def t_export_agg_country_fulfillment_day(process_date):

    if process_date=='full':
        write_mode = 'overwrite'
        where_clause = ''
    else:
        write_mode = 'append'
        where_clause = "WHERE scrape_date='"+process_date+"'"

    try:
        bq_table = 'rooscrape.foodhoover_store.agg_country_fulfillment_day'
        sql_table = 'agg_country_fulfillment_day'
        sql_schema = ['scrape_date','country', 'vendor', 'fulfillment_type','rx_num', 'delivery_population']
        bq_select_sql = "SELECT scrape_date, country, vendor, fulfillment_type, rx_num, delivery_population FROM rooscrape.foodhoover_store.agg_country_fulfillment_day "+where_clause
        sql_create_statement = "table_schemas/agg_country_fulfillment_day"

        status = generic_exporter(process_date, bq_table, sql_table, sql_schema, bq_select_sql, sql_create_statement, write_mode)

        t_step_logger(process_date, 'EXPORT-AGG-COUNTRY-FULFILLMENT-DAY', 'SUCESS', status)
        return status
    except Exception as e:
        t_step_logger(process_date, 'EXPORT-AGG-COUNTRY-FULFILLMENT-DAY', 'FAIL', str(e))
        return e

def t_export_agg_delivery_zone(process_date):
    try:
        bq_table = 'rooscrape.foodhoover_store.agg_delivery_zone'
        sql_table = 'agg_delivery_zone'
        sql_schema = ['place_id', 'vendor', 'delivery_zone','delivery_population','place_name','place_lat','place_lng','vendor_rx', 'sectors_covered']
        bq_select_sql = "SELECT place_id, vendor, TO_HEX(ST_ASBINARY(delivery_zone)) as delivery_zone, delivery_population,place_name,place_lat,place_lng, REPLACE(REPLACE(TO_JSON_STRING(vendor_rx),'[','{'),']','}') as vendor_rx, REPLACE(REPLACE(TO_JSON_STRING(sectors_covered),'[','{'),']','}') as sectors_covered FROM rooscrape.foodhoover_store.agg_delivery_zone where process_date='"+process_date+"'"
        sql_create_statement = "table_schemas/agg_delivery_zone"

        status = generic_exporter(process_date, bq_table, sql_table, sql_schema, bq_select_sql, sql_create_statement,'overwrite')

        t_step_logger(process_date, 'EXPORT-AGG-DELIVERY-ZONE', 'SUCESS', status)
        return status
    except Exception as e:
        t_step_logger(process_date, 'EXPORT-AGG-DELIVERY-ZONE', 'FAIL', str(e))
        return e