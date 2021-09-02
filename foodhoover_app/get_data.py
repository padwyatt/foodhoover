from sqlalchemy.sql import text
from sqlalchemy.sql.expression import true
from connections import get_sql_client, get_bq_client
from google.cloud import bigquery
import json
from datetime import timedelta

def get_country_data(start, end, lngw, lats, lnge, lngn, granularity):
    if granularity=='districts':
        sql = text("\
            SELECT jsonb_build_object(\
                'type',     'FeatureCollection',\
                'features', jsonb_agg(feature)\
                ) as geometry\
            FROM (\
                SELECT\
                    districts.district,\
                    jsonb_build_object(\
                        'type',       'Feature',\
                        'id',         districts.district,\
                        'geometry',   ST_AsGeoJSON(ST_CollectionExtract(ST_ForcePolygonCW(ST_Simplify(districts.geometry,0.001)),3))::jsonb,\
                        'properties', to_jsonb(\
                            jsonb_build_object(\
                                'postcode_name', districts.district,\
                                'ROO', counts.roo, 'JE',counts.je, 'UE',counts.ue, 'FH',counts.fh\
                            )\
                        )\
                    ) as feature\
                    FROM districts\
                    LEFT JOIN\
                        (SELECT\
                            postcode_district,\
                            MAX(roo) as roo,\
                            MAX(je) as je,\
                            MAX(ue) as ue,\
                            MAX(fh) as fh\
                        FROM agg_district_run z\
                        WHERE DATE_TRUNC('day',scrape_time)>=:start and DATE_TRUNC('day',scrape_time)<=:end\
                        GROUP by postcode_district) counts\
                    ON counts.postcode_district=districts.district\
            ) agg\
        ")
    elif granularity=='sectors':
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
                    WHERE DATE_TRUNC('day',a.scrape_time)>=:start and DATE_TRUNC('day',a.scrape_time)<=:end \
                    AND geometry && ST_MakeEnvelope(:lngw,:lats,:lnge,:lngn) \
                    GROUP by b.sector \
                ) as agg \
            ) as sectors \
        ")
    else:
        return 'granularity not specified'

    engine = get_sql_client('foodhoover_cache')
    conn = engine.connect()
    result = conn.execute(sql, start=start, end=end, lngw=lngw, lats=lats, lnge=lnge, lngn=lngn)
    coverage_map = result.fetchone()['geometry']

    sql = text("\
        SELECT vendor,jsonb_build_object(\
            'delivery_population',MAX(delivery_population),\
            'rx_num', MAX(rx_num)\
            ) as country_stats\
        FROM agg_country_run_pop\
        WHERE DATE_TRUNC('day',scrape_time)>=:start AND DATE_TRUNC('day',scrape_time)<=:end\
        GROUP BY vendor\
    ")

    engine = get_sql_client('foodhoover_cache')
    conn = engine.connect()
    result = conn.execute(sql, start=start, end=end)

    country_stats = {}
    for r in result:
        country_stats[r['vendor']]=r['country_stats']

    return {'coverage':coverage_map,'stats':country_stats}

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
                                    'postcode_sector', sector, \
                                    'population', population \
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

def get_delivery_boundary(start, end, place_ids, last_update): 

    ###this is no longer used, but would fetch from the sql rx_cx_fast data
    old_sql = text("  \
            SELECT rx_uid, jsonb_build_object( \
                'type',     'FeatureCollection', \
                'features', jsonb_agg(features.feature) \
            ) as geometry \
            FROM ( \
                SELECT \
                    z.rx_uid, \
                    jsonb_build_object( \
                        't_ype', 'Feature', \
                        'id', z.rxuid, \
                        'geometry', ST_AsGeoJSON(ST_ForcePolygonCW(ST_BuildArea(ST_UNION(z.geometry))))::jsonb, \
                        'properties', to_jsonb( \
                            jsonb_build_object( \
                                'rx_uid', z.rx_uid, \
                                'vendor', MAX(rx_ref.vendor), \
                                'delivery_area',  ST_AREA(ST_TRANSFORM(ST_UNION(z.geometry), 31467))/1000000,\
                                'delivery_population', SUM(population), \
                                'place_id', MAX(COALESCE(rx_ref.place_id, z.rx_uid)) \
                            ) \
                        )\
                    ) as feature \
                FROM ( \
                    SELECT \
                    postcode_sector, \
                    rx_uid, \
                    MAX(sectors.population) as population, \
                    ST_MakeValid(MAX(geometry)) AS geometry \
                    FROM \
                    rx_cx_fast \
                    LEFT JOIN \
                    postcode_lookup \
                    ON \
                    postcode_lookup.postcode = rx_cx_fast.cx_postcode \
                    LEFT JOIN \
                    sectors \
                    ON \
                    sectors.sector=postcode_lookup.postcode_sector \
                    WHERE \
                    DATE_TRUNC('day',scrape_time)>=:start AND DATE_TRUNC('day',scrape_time)<=:end\
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

    sql = text("\
        SELECT\
            place_id,\
            jsonb_build_object(\
                'place_id', max(places.place_id),\
                'place_name', max(places.place_name),\
                'place_lat', max(places.place_lat),\
                'place_lng', max(places.place_lng),\
                'entities', array_to_json(\
                    array_agg(\
                        jsonb_build_object('place_vendor_id',CONCAT(places.place_id,'-',places.vendor),'rx_name',places.place_name,'vendor',places.vendor)\
                    )\
                )\
            ) as place_details,\
            jsonb_build_object(\
                'type',     'FeatureCollection',\
                'features', jsonb_agg(places.feature)\
            ) as geometry\
        FROM (\
            SELECT\
            place_id,\
            place_name,\
            place_lat,\
            place_lng,\
            vendor,\
            jsonb_build_object(\
                'type', 'Feature',\
                'id', CONCAT(place_id,'-',vendor),\
                'geometry', ST_AsGeoJSON(ST_CollectionExtract(delivery_zone,3))::jsonb,\
                'properties', to_jsonb(\
                    jsonb_build_object(\
                        'place_vendor_id', CONCAT(place_id,'-',vendor),\
                        'vendor', vendor,\
                        'delivery_area',  COALESCE(ST_AREA(ST_TRANSFORM(ST_SetSRID(delivery_zone,4326), 31467))/1000000,0),\
                        'delivery_population', COALESCE(delivery_population,0),\
                        'place_id', place_id\
                    )\
                )\
            ) as feature\
        FROM agg_rx_cx\
        where place_id IN :place_ids\
        ) as places\
    GROUP by places.place_id\
    ")

    bq_sql = "\
        SELECT\
            place_id,\
            to_json_string(final.place_details) as place_details,\
            to_json_string(final.geometry) as geometry\
        FROM(\
            SELECT\
                places.place_id,\
                STRUCT(\
                    MAX(places.place_id) as place_id,\
                    MAX(places.place_name) as place_name,\
                    MAX(places.place_lat) as place_lat,\
                    MAX(places.place_lng) as place_lng,\
                    array_agg(\
                        STRUCT(\
                            CONCAT(places.place_id,'-',places.vendor) as place_vendor_id,\
                            places.place_name as rx_name,\
                            places.vendor as vendor\
                            )\
                    ) as entities\
                ) as place_details,\
                STRUCT(\
                    'FeatureCollection' as type,\
                    array_agg(places.feature) as features\
                ) as geometry\
            FROM (\
                SELECT\
                    place_id,\
                    place_name,\
                    place_lat,\
                    place_lng,\
                    vendor,\
                    STRUCT(\
                        'Feature' AS type,\
                        CONCAT(place_id,'-',vendor) AS id,\
                        ST_ASGEOJSON(delivery_zone) as geometry,\
                        STRUCT(\
                            CONCAT(place_id,'-',vendor) as place_vendor_id,\
                            vendor,\
                            place_id,\
                            ST_AREA(delivery_zone)/1000000 as delivery_area,\
                            delivery_population,\
                            place_id\
                            ) as properties\
                    ) as feature\
                FROM (\
                    SELECT\
                        places.place_id,\
                        bysector.vendor,\
                        ST_UNION(ST_DUMP(ST_SIMPLIFY(ST_UNION_AGG(sectors.geometry),50),2)) as delivery_zone,\
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
                            WHERE scrape_time>='"+start+"' and scrape_time<='"+end+"'\
                            AND rx_uid IN(\
                                SELECT distinct(rx_uid) from rooscrape.foodhoover_store.rx_ref\
                                WHERE hoover_place_id IN UNNEST(@place_ids)\
                            )\
                            GROUP BY rx_uid, cx_postcode) results\
                        LEFT JOIN rooscrape.foodhoover_store.postcode_lookup pc on pc.postcode=results.cx_postcode\
                        LEFT JOIN rooscrape.foodhoover_store.sectors sectors on sectors.sector = pc.postcode_sector\
                        LEFT JOIN rooscrape.foodhoover_store.rx_ref ref on ref.rx_uid = results.rx_uid\
                        LEFT JOIN rooscrape.foodhoover_store.places places on places.place_id = ref.hoover_place_id\
                        GROUP BY places.place_id, ref.vendor, sectors.sector) bysector\
                    LEFT JOIN rooscrape.foodhoover_store.sectors sectors on sectors.sector = bysector.sector\
                    RIGHT JOIN rooscrape.foodhoover_store.places places on places.place_id = bysector.place_id\
                    WHERE places.place_id IN UNNEST(@place_ids)\
                    GROUP BY places.place_id, bysector.vendor\
                ) agg_rx_cx\
            ) as places\
            GROUP BY places.place_id\
        ) final\
    "

    max_cache = last_update
    min_cache = max_cache - timedelta(days=14)
    max_cache = max_cache.strftime('%Y-%m-%d')
    min_cache = min_cache.strftime('%Y-%m-%d')

    if ((min_cache == start) & (max_cache== end)): ##get from precomputed table
        engine = get_sql_client('foodhoover_cache')
        conn = engine.connect()

        sql = sql.bindparams(place_ids=tuple(place_ids))
        result = conn.execute(sql)
        
        place_boundary = {}
        for row in result: 
            print(row['place_id'])
            place_boundary[row['place_id']] = {
                'place_details':  row['place_details'],
                'place_map' : row['geometry']
                }
    else:  #get from raw table      
        client = get_bq_client()
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("place_ids", "STRING",place_ids),
                bigquery.ScalarQueryParameter("start", "STRING", start),
                bigquery.ScalarQueryParameter("end", "STRING", end),       
            ]
        )
        query_job = client.query(bq_sql, job_config=job_config) 
        result = query_job.result()

        place_boundary = {}
        for row in result: 
            place_boundary[row['place_id']] = {
                'place_details':  json.loads(row['place_details']),
                'place_map' : json.loads(row['geometry'])
                }
            for index, feature in enumerate(place_boundary[row['place_id']]['place_map']['features']): ##We need to do this because geoJSON is double encoded
                geometry = place_boundary[row['place_id']]['place_map']['features'][index]['geometry']
                if geometry != None:
                    place_boundary[row['place_id']]['place_map']['features'][index]['geometry'] = json.loads(geometry)
                
    return place_boundary


def get_rx_names(search, lat, lng):

    sql = text("\
	        SELECT\
                place_id as value,\
                place_label as label\
            FROM places\
	        WHERE UPPER(place_name) LIKE UPPER(:s)\
            ORDER BY ST_Distance(ST_MakePoint(:lat,:lng), place_location, false) ASC \
            LIMIT 20 \
        ")

    engine = get_sql_client('foodhoover_cache')
    conn = engine.connect()
    result = conn.execute(sql, s="%"+search.replace(" ","%")+"%", lat=lat, lng=lng)

    return  [{'value': r['value'],'label': r['label']} for r in result]

def get_restaurant_details(place_ids):

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
        LEFT JOIN rx_ref on places.place_id=rx_ref.hoover_place_id\
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


def get_chains_boundary(chain, start, end, last_update):

    bq_sql = "\
            WITH raw as (\
            SELECT ref.vendor, look.postcode_sector, ARRAY_AGG(DISTINCT coverage.rx_uid) as rx_included FROM (\
                SELECT rx_uid, cx_postcode FROM rooscrape.foodhoover_store.rx_cx_fast\
                WHERE DATE_TRUNC(scrape_time,DAY)>=@start AND DATE_TRUNC(scrape_time,DAY)<=@end\
                AND rx_uid IN(\
                    SELECT distinct(ref.rx_uid) from rooscrape.foodhoover_store.places\
                    LEFT join rooscrape.foodhoover_store.rx_ref ref on ref.hoover_place_id = places.place_id\
                    WHERE UPPER(place_name) LIKE UPPER(@chain)\
                    )\
                GROUP BY rx_uid, cx_postcode) coverage\
            LEFT JOIN rooscrape.foodhoover_store.postcode_lookup as look on look.postcode=coverage.cx_postcode\
            LEFT JOIN rooscrape.foodhoover_store.rx_ref ref on ref.rx_uid = coverage.rx_uid\
            GROUP BY ref.vendor, look.postcode_sector)\
            SELECT\
                raw.vendor,\
                ST_AsGeoJSON(ST_UNION(ST_DUMP(ST_SIMPLIFY(ST_UNION_AGG(sectors.geometry),50),2))) as delivery_area,\
                SUM(sectors.population) as delivery_population,\
                MAX(rx_num) as rx_num\
            FROM raw\
            LEFT JOIN rooscrape.foodhoover_store.sectors sectors on raw.postcode_sector=sectors.sector\
            LEFT JOIN (\
                SELECT raw.vendor, COUNT(DISTINCT rx_uid) as rx_num\
                FROM raw, UNNEST(raw.rx_included) as rx_uid\
                GROUP BY raw.vendor\
                ) rx_list ON rx_list.vendor=raw.vendor\
            GROUP BY raw.vendor\
        "

    sql = text("\
        SELECT\
            pop_stats.vendor,\
            pop_stats.delivery_population,\
            results.rx_num as rx_num,\
            ST_ASGEOJSON(results.delivery_area) as delivery_area\
            FROM (\
                SELECT\
                    vendor,\
                    sum(sectors.population) as delivery_population\
                FROM (\
                    SELECT\
                        vendor,\
                        UNNEST(sectors_covered) as inc_sector\
                    FROM agg_rx_cx\
                    where UPPER(place_name) LIKE UPPER(:chain)\
                    GROUP BY vendor, UNNEST(sectors_covered)\
                    ) sectors_covered\
                LEFT JOIN sectors on sectors.sector=sectors_covered.inc_sector\
                GROUP by sectors_covered.vendor\
                ) pop_stats\
        LEFT JOIN\
            (SELECT\
                vendor,\
                ST_SIMPLIFY(ST_UNION(ST_CollectionExtract(ST_MAKEVALID(delivery_zone),3)),0.001) as delivery_area,\
                COUNT(DISTINCT place_id) as rx_num\
            FROM agg_rx_cx\
            WHERE UPPER(place_name) LIKE UPPER(:chain)\
            GROUP BY vendor) results ON results.vendor=pop_stats.vendor\
        ")

    max_cache = last_update
    min_cache = max_cache - timedelta(days=14)
    max_cache = max_cache.strftime('%Y-%m-%d')
    min_cache = min_cache.strftime('%Y-%m-%d')

    if ((min_cache == start) & (max_cache== end)):
        engine = get_sql_client('foodhoover_cache')
        conn = engine.connect()
        result = conn.execute(sql, chain="%"+chain.replace(" ","%")+"%")

    else:
        client = get_bq_client()

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("chain", "STRING","%"+chain.replace(" ","%")+"%"),
                bigquery.ScalarQueryParameter("start", "STRING", start),
                bigquery.ScalarQueryParameter("end", "STRING", end),       
            ]
        )
        query_job = client.query(bq_sql, job_config=job_config)  # API request
        result = query_job.result()

    chain_areas = {
            "type": "FeatureCollection",
            "features": [{
                "type":"Feature", 
                "geometry":json.loads(row['delivery_area']),
                "properties": {
                    "vendor": row['vendor'],
                    "delivery_population" : row['delivery_population'],
                    "rx_num": row['rx_num']
                }
            } for row in result]
    }

    return chain_areas

def get_places_in_area(chain, lngw, lats, lnge, latn):
    sql = text(
        "SELECT\
            place_id,\
            place_name,\
            place_lat,\
            place_lng,\
            array_to_json(place_vendors) as place_vendors\
        FROM places\
        WHERE UPPER(place_name) LIKE UPPER(:chain)\
        AND place_lat>=:lats\
        AND place_lat<=:latn\
        AND place_lng>=:lngw\
        AND place_lng<=:lnge\
    ")

    engine = get_sql_client('foodhoover_cache')
    conn = engine.connect()
    result = conn.execute(sql, chain="%"+chain.replace(" ","%")+"%", lngw=lngw, lats=lats, lnge=lnge, latn=latn)

    return  [{'place_id': r['place_id'],'place_name': r['place_name'],'place_lat': r['place_lat'],'place_lng': r['place_lng'],'place_vendors': r['place_vendors']} for r in result]

def get_last_update():
    sql = "select min(scrape_time) as first_update, max(scrape_time) as last_update from agg_country_run_pop"
    engine = get_sql_client('foodhoover_cache')
    conn = engine.connect()
    result = conn.execute(sql)

    return result.fetchone()
