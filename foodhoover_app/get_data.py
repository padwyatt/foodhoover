from sqlalchemy.sql import text
from connections import get_sql_client

import asyncio
from aiohttp import ClientSession
import uuid
import json

def get_country_data(start, end, lngw, lats, lnge, lngn, zoom):
    if zoom<11:
        sql = text("  \
            SELECT jsonb_build_object( \
            'type',     'FeatureCollection', \
            'features', jsonb_agg(districts.feature) \
            ) as geometry \
            FROM ( \
                SELECT \
                    postcode_district, \
                    jsonb_build_object( \
                        'type',       'Feature', \
                        'id',         postcode_district, \
                        'geometry',   ST_AsGeoJSON(ST_ForcePolygonCW(agg.geometry))::jsonb, \
                        'properties', to_jsonb( \
                            jsonb_build_object( \
                                'postcode_name', agg.postcode_district, \
                                'ROO', agg.roo, 'JE',agg.je, 'UE',agg.ue, 'FH',agg.fh \
                            ) \
                        ) \
                    ) as feature \
                FROM ( \
                    SELECT b.district as postcode_district, \
                    max(ST_CollectionExtract(ST_SnapToGrid(b.geometry,0.001),3)) as geometry, \
                    MAX(a.roo) as roo, \
                    MAX(a.je) as je, \
                    MAX(a.ue) as ue, \
                    MAX(a.fh) as fh \
                    FROM agg_district_run a \
                    INNER JOIN districts b ON a.postcode_district=b.district \
                    WHERE a.scrape_time>=:start and a.scrape_time<=:end \
                    AND geometry && ST_MakeEnvelope(:lngw,:lats,:lnge,:lngn) \
                    GROUP by b.district \
                ) as agg \
            ) as districts \
        ")

    else:
        sql = text("  \
            SELECT jsonb_build_object( \
            'type',     'FeatureCollection', \
            'features', jsonb_agg(sectors.feature) \
            ) as geometry \
            FROM ( \
                SELECT \
                    postcode_sector, \
                    jsonb_build_object( \
                        'type',       'Feature', \
                        'id',         postcode_sector, \
                        'geometry',   ST_AsGeoJSON(ST_ForcePolygonCW(agg.geometry))::jsonb, \
                        'properties', to_jsonb( \
                            jsonb_build_object( \
                                'postcode_name', agg.postcode_sector, \
                                'ROO', agg.roo, 'JE',agg.je, 'UE',agg.ue, 'FH',agg.fh \
                            ) \
                        ) \
                    ) as feature \
                FROM ( \
                    SELECT b.sector as postcode_sector, \
                    max(ST_CollectionExtract(b.geometry,3)) as geometry, \
                    MAX(a.roo) as roo, \
                    MAX(a.je) as je, \
                    MAX(a.ue) as ue, \
                    MAX(a.fh) as fh \
                    FROM agg_sector_run a \
                    INNER JOIN sectors b ON a.postcode_sector=b.sector \
                    WHERE a.scrape_time>=:start and a.scrape_time<=:end \
                    AND geometry && ST_MakeEnvelope(:lngw,:lats,:lnge,:lngn) \
                    GROUP by b.sector \
                ) as agg \
            ) as sectors \
        ")

    engine = get_sql_client('foodhoover_cache')
    conn = engine.connect()
    result = conn.execute(sql, start=start, end=end, lngw=lngw, lats=lats, lnge=lnge, lngn=lngn)
    coverage_map = result.fetchone()['geometry']
    return coverage_map

def get_geo_objects(lngw, lats, lnge, lngn):
    sql = text("  \
            SELECT jsonb_build_object( \
                'type',     'FeatureCollection', \
                'features', jsonb_agg(sectors.feature) \
                ) as geometry \
                FROM ( \
                    SELECT \
                        sector as postcode_sector, \
                        jsonb_build_object( \
                            'type',       'Feature', \
                            'id',         sector, \
                            'geometry',   ST_AsGeoJSON(ST_ForcePolygonCW(ST_CollectionExtract(geometry,3)))::jsonb, \
                            'properties', to_jsonb( \
                                jsonb_build_object( \
                                    'postcode_sector', sector \
                                ) \
                            ) \
                        ) as feature \
                    FROM sectors \
                    WHERE geometry && ST_MakeEnvelope(:lngw,:lats,:lnge,:lngn) LIMIT 1000) as sectors \
        ")
    
    engine = get_sql_client('foodhoover_cache')
    conn = engine.connect()
    result = conn.execute(sql, lngw=lngw, lats=lats, lnge=lnge, lngn=lngn)
    geo_map = result.fetchone()['geometry']
    return geo_map

def get_delivery_boundary(start, end, place_id, run_id = None):

    if run_id != None:
        run_condition = "run_id = '"+run_id+"'"
        time_condition = ""
        target_table = 'rx_cx_fast_flash'
    else:
        run_condition = ""
        time_condition = "scrape_time>=:start AND scrape_time<=:end"
        target_table = 'rx_cx_fast'

    sql = text("  \
            SELECT rx_uid, jsonb_build_object( \
                'type',     'FeatureCollection', \
                'features', jsonb_agg(features.feature) \
            ) as geometry \
            FROM ( \
                SELECT \
                    z.rx_uid, \
                    jsonb_build_object( \
                        'type', 'Feature', \
                        'id', z.rx_uid, \
                        'geometry', ST_AsGeoJSON(ST_ForcePolygonCW(ST_BuildArea(ST_UNION(z.geometry))))::jsonb, \
                        'properties', to_jsonb( \
                            jsonb_build_object( \
                                'rx_uid', z.rx_uid, \
                                'vendor', MAX(rx_ref.vendor), \
                                'delivery_area', ST_AREA(ST_UNION(z.geometry)),\
                                'place_id', MAX(COALESCE(rx_ref.place_id, z.rx_uid)) \
                            ) \
                        )\
                    ) as feature \
                FROM ( \
                    SELECT \
                    postcode_sector, \
                    rx_uid, \
                    ST_MakeValid(MAX(geometry)) AS geometry \
                    FROM \
                    "+target_table+" rx_cx_fast \
                    LEFT JOIN \
                    postcode_lookup \
                    ON \
                    postcode_lookup.postcode = rx_cx_fast.cx_postcode \
                    LEFT JOIN \
                    sectors \
                    ON \
                    sectors.sector=postcode_lookup.postcode_sector \
                    WHERE \
                    "+ time_condition + "\
                    "+ run_condition + " \
                    AND rx_uid IN( \
                    SELECT \
                        rx_uid \
                    FROM \
                        rx_ref \
                    WHERE \
                        place_id=:s \
                        OR rx_uid=:s) \
                    AND sectors.geometry IS NOT NULL \
                    GROUP BY \
                    postcode_sector, \
                    rx_uid) z \
                LEFT JOIN rx_ref on rx_ref.rx_uid=z.rx_uid \
                GROUP BY z.rx_uid \
            ) features \
            GROUP BY rx_uid \
        ")

    engine = get_sql_client('foodhoover_cache')
    conn = engine.connect()
    result = conn.execute(sql, s=place_id, start=start, end=end)
    resto_map = [row['geometry'] for row in result]
    place_details = get_restaurant_details([place_id])

    return {
        'place_details': place_details,
        'place_map' : resto_map } 

def get_rx_names(search, lat, lng):

    #sql = text("SELECT q.value, q.label FROM \
    #    (SELECT COALESCE(place_id, rx_uid) as value, CONCAT(MAX(rx_name),': ', MAX(rx_sector),' ', REPLACE(REPLACE(CAST(array_agg(distinct(vendor)) as TEXT),'{','('),'}',')')) as label, AVG(rx_lat) as rx_lat, AVG(rx_lng) as rx_lng FROM rx_ref \
    #    WHERE UPPER(rx_name) LIKE UPPER(:s) AND COALESCE(place_id, rx_uid) IS NOT NULL \
    #    GROUP BY COALESCE(place_id, rx_uid) \
    #    LIMIT 200) q \
    #    ORDER BY ST_Distance(ST_MakePoint("+str(lat)+","+str(lng)+"),ST_MakePoint(q.rx_lat,q.rx_lng)) ASC LIMIT 20")

    sql = text("\
        SELECT z.place_id as value, z.place_label as label FROM (\
	        SELECT place_id, place_label, place_location, place_lat, place_lng from places \
	        WHERE UPPER(place_name) LIKE UPPER(:s) \
	        LIMIT 200) z\
        ORDER BY ST_Distance(ST_MakePoint(:lat,:lng), z.place_location, false) ASC \
        LIMIT 20 \
        ")

    engine = get_sql_client('foodhoover_cache')
    conn = engine.connect()
    result = conn.execute(sql, s="%"+search.replace(" ","%")+"%", lat=lat, lng=lng)

    return  [{'value': r['value'],'label': r['label']} for r in result]

def get_restaurant_details(place_ids):
    #sql = text("\
    #    SELECT \
    #        place_id, \
    #        MODE() WITHIN GROUP (ORDER BY rx_name) as place_name, \
    #        CONCAT(MODE() WITHIN GROUP (ORDER BY rx_name),': ', MODE() WITHIN GROUP (ORDER BY rx_sector),' ', REPLACE(REPLACE(CAST(array_agg(distinct(vendor)) as TEXT),'{','('),'}',')')) as place_label, \
    #        MODE() WITHIN GROUP (ORDER BY rx_lat) as place_lat, \
    #        MODE() WITHIN GROUP (ORDER BY rx_lng) as place_lng, \
    #        array_to_json(array_agg(restaurant)) as entities\
    #    FROM ( \
    #       SELECT \
    #            COALESCE(place_id, rx_uid) as place_id, \
    #            rx_name, \
    #            vendor, \
    #            rx_sector, \
    #            ROUND(rx_lat::numeric,3) as rx_lat, \
    #            ROUND(rx_lng::numeric,3) as rx_lng, \
    #            jsonb_build_object('rx_uid',rx_uid,'rx_name',rx_name,'vendor',vendor) as restaurant \
    #        FROM \
    #        rx_ref \
    #        WHERE (place_id IN :rx_uids) OR (rx_uid IN :rx_uids) \
    #        )rxs \
    #    GROUP BY place_id \
    #    ")

    sql = text("\
        SELECT\
        places.place_id,\
        max(place_name) as place_name,\
        max(place_label) as place_label,\
        max(place_lat) as place_lat,\
        max(place_lng) as place_lng,\
        array_to_json(\
            array_agg(\
                jsonb_build_object('rx_uid',rx_uid,'rx_name',rx_name,'vendor',vendor)\
            )\
        ) as entities\
        FROM places\
        LEFT JOIN rx_ref on places.place_id=rx_ref.place_id\
        WHERE places.place_id IN :place_ids\
        GROUP by places.place_id\
    ")

    if len(place_ids)>0:
        sql = sql.bindparams(place_ids=tuple(place_ids))

        engine = get_sql_client('foodhoover_cache')
        conn = engine.connect()
        result = conn.execute(sql)

        data = {}
        for r in result:
            place = {'place_id': r['place_id'],
                    'place_name': r['place_name'],
                    'place_label': r['place_label'],
                    'place_lat': float(r['place_lat']),
                    'place_lng': float(r['place_lng']),
                    'entities': r['entities']}
            data[place['place_id']] = place
    else:
        data = {}

    return data

def count_flash(lngw, lats, lnge, lngn):
    sql = text(
        "SELECT count(1) as sector_count FROM sectors \
        WHERE geometry && ST_MakeEnvelope(:lngw,:lats,:lnge,:lngn)")

    engine = get_sql_client('foodhoover_cache')
    conn = engine.connect()
    result = conn.execute(sql, lngw=lngw, lats=lats, lnge=lnge, lngn=lngn)

    sector_count = str(result.fetchone()['sector_count'])

    return sector_count

def get_flash(lngw, lats, lnge, lngn, place_ids, vendors):
    sql = text(
        "SELECT sector, postcode_area, latitude, longitude, postcode FROM sectors \
        LEFT JOIN postcode_lookup on sectors.closest_postcode = postcode_lookup.postcode \
        WHERE geometry && ST_MakeEnvelope(:lngw,:lats,:lnge,:lngn) AND postcode IS NOT NULL")
    
    engine = get_sql_client('foodhoover_cache')
    conn = engine.connect()
    result = conn.execute(sql, lngw=lngw, lats=lats, lnge=lnge, lngn=lngn)
    urls_to_crawl = []

    run_id = str(uuid.uuid4()) + '-flash'
    vendor_string = "&vendors="+"&vendors=".join(vendors)
    for row in result:
        urls_to_crawl.append('https://europe-west2-rooscrape.cloudfunctions.net/foodhoover?mode=flash&postcode='+row['postcode']+"&postcode_area="+row['postcode_area']+"&lat="+str(row['latitude'])+"&lng="+str(row['longitude'])+vendor_string+"&run_id="+run_id)

    asyncio.run(url_batch(urls_to_crawl, flash_fetch_function, 25))
    ##get refreshed data
    scrape_results = get_delivery_boundary(None, None, place_ids[0], run_id = run_id)

    return scrape_results

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

async def flash_fetch_function(fetch_data):
    print(fetch_data)
    async with ClientSession() as session:
        try:
            async with session.get(fetch_data, timeout=30) as response:
                response = await response.read()
                #response = json.loads(response.decode('utf8'))
                return response.decode('utf8')
        except Exception as e:
            return str(e)
            