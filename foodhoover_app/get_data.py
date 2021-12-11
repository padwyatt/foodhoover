from sqlalchemy.sql import text
from sqlalchemy.sql.expression import true
from connections import get_sql_client, get_bq_client
from google.cloud import bigquery
import json
from datetime import date, timedelta
import io
import csv


def get_country_fulfillment_data(start, end, lngw, lats, lnge, lngn, granularity):
    if granularity=='districts':
        sql = text("""
            SELECT jsonb_build_object(
                'type',     'FeatureCollection',
                'features', jsonb_agg(feature)
                ) as geometry
            FROM (
                SELECT
                    districts.district,
                    jsonb_build_object(
                        'type',       'Feature',
                        'id',         districts.district,
                        'geometry',   ST_AsGeoJSON(ST_CollectionExtract(ST_ForcePolygonCW(ST_Simplify(MAX(districts.geometry),0.001)),3))::jsonb,
                        'properties', to_jsonb(
                            jsonb_build_object(
                                'postcode_name', districts.district,
                                'rx_counts', jsonb_object_agg(COALESCE(counts.fulfillment_type,'None'), counts.counts)
                            )
                        )
                    ) as feature
                    FROM districts
                    LEFT JOIN
                        (SELECT
                            postcode_district,
                            fulfillment_type,
                            jsonb_build_object(
                                'ROO', MAX(CASE WHEN vendor='ROO' THEN rx_num ELSE 0 END),
                                'UE', MAX(CASE WHEN vendor='UE' THEN rx_num ELSE 0 END),
                                'JE', MAX(CASE WHEN vendor='JE' THEN rx_num ELSE 0 END),
                                'FH', MAX(CASE WHEN vendor='FH' THEN rx_num ELSE 0 END)
                            ) as counts
                        FROM agg_district_fulfillment_day
                        WHERE scrape_date>=:start and scrape_date<=:end and fulfillment_type is not null
                        GROUP BY postcode_district, fulfillment_type) counts
                    ON counts.postcode_district=districts.district
                    GROUP BY districts.district
            ) agg
        """)
    elif granularity=='sectors': ###this one could perhaps be faster by putting the geofilter inside the subquery
        sql = text("""
            SELECT jsonb_build_object(
                'type',     'FeatureCollection',
                'features', jsonb_agg(feature)
                ) as geometry
            FROM (
                SELECT
                    sectors.sector,
                    jsonb_build_object(
                        'type',       'Feature',
                        'id',         sectors.sector,
                        'geometry',   ST_AsGeoJSON(ST_CollectionExtract(ST_ForcePolygonCW(ST_Simplify(MAX(sectors.geometry),0.0001)),3))::jsonb,
                        'properties', to_jsonb(
                            jsonb_build_object(
                                'postcode_name', sectors.sector,
                                'rx_counts', jsonb_object_agg(COALESCE(counts.fulfillment_type,'None'), counts.counts)
                            )
                        )
                    ) as feature
                    FROM sectors
                    LEFT JOIN
                        (SELECT
                            postcode_sector,
                            fulfillment_type,
                            jsonb_build_object(
                                'ROO', MAX(CASE WHEN vendor='ROO' THEN rx_num ELSE 0 END),
                                'UE', MAX(CASE WHEN vendor='UE' THEN rx_num ELSE 0 END),
                                'JE', MAX(CASE WHEN vendor='JE' THEN rx_num ELSE 0 END),
                                'FH', MAX(CASE WHEN vendor='FH' THEN rx_num ELSE 0 END)
                            ) as counts
                        FROM agg_sector_fulfillment_day
                        WHERE scrape_date>=:start and scrape_date<=:end and fulfillment_type is not null
                        GROUP BY postcode_sector, fulfillment_type) counts
                    ON counts.postcode_sector=sectors.sector
                    WHERE sectors.geometry && ST_MakeEnvelope(:lngw,:lats,:lnge,:lngn)
                    GROUP BY sectors.sector
            ) agg
        """)
    else:
        return 'granularity not specified'

    engine = get_sql_client('foodhoover_cache')
    conn = engine.connect()
    result = conn.execute(sql, start=start, end=end, lngw=lngw, lats=lats, lnge=lnge, lngn=lngn)
    coverage_map = result.fetchone()['geometry']

    sql = text("""
        SELECT vendor, jsonb_object_agg(COALESCE(fulfillment_type, 'None'), fulfillment_stats) as country_stats
        FROM (
            SELECT vendor,
            fulfillment_type,
            jsonb_build_object(
                'delivery_population',COALESCE(MAX(delivery_population),0),
                'rx_num', COALESCE(MAX(rx_num),0)
                ) as fulfillment_stats
            FROM agg_country_fulfillment_day
            WHERE scrape_date>=:start AND scrape_date<=:end and fulfillment_type is not null
            GROUP BY vendor, fulfillment_type
            ) q
        GROUP BY vendor
    """)

    engine = get_sql_client('foodhoover_cache')
    conn = engine.connect()
    result = conn.execute(sql, start=start, end=end)

    country_stats = {}
    for r in result:
        country_stats[r['vendor']]=r['country_stats']

    return {'coverage':coverage_map,'stats':country_stats}


def get_country_data(start, end, lngw, lats, lnge, lngn, granularity):
    if granularity=='districts':
        sql = text("""
            SELECT jsonb_build_object(
                'type',     'FeatureCollection',
                'features', jsonb_agg(feature)
                ) as geometry
            FROM (
                SELECT
                    districts.district,
                    jsonb_build_object(
                        'type',       'Feature',
                        'id',         districts.district,
                        'geometry',   ST_AsGeoJSON(ST_CollectionExtract(ST_ForcePolygonCW(ST_Simplify(districts.geometry,0.001)),3))::jsonb,
                        'properties', to_jsonb(
                            jsonb_build_object(
                                'postcode_name', districts.district,
                                'ROO', counts.roo, 'JE',counts.je, 'UE',counts.ue, 'FH',counts.fh
                            )
                        )
                    ) as feature
                    FROM districts
                    LEFT JOIN
                        (SELECT
                            postcode_district,
                            MAX(CASE WHEN vendor='ROO' THEN rx_num ELSE 0 END) as roo,
                            MAX(CASE WHEN vendor='UE' THEN rx_num ELSE 0 END) as ue,
                            MAX(CASE WHEN vendor='JE' THEN rx_num ELSE 0 END) as je,
                            MAX(CASE WHEN vendor='FH' THEN rx_num ELSE 0 END) as fh
                        FROM agg_district_fulfillment_day
                        WHERE scrape_date>=:start and scrape_date<=:end
                        AND fulfillment_type='vendor'
                        GROUP BY postcode_district) counts
                    ON counts.postcode_district=districts.district
            ) agg
        """)
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
                        MAX(ST_CollectionExtract(b.geometry,3)) as geometry, \
                        MAX(CASE WHEN vendor='ROO' THEN rx_num ELSE 0 END) as roo,\
                        MAX(CASE WHEN vendor='UE' THEN rx_num ELSE 0 END) as ue,\
                        MAX(CASE WHEN vendor='JE' THEN rx_num ELSE 0 END) as je,\
                        MAX(CASE WHEN vendor='FH' THEN rx_num ELSE 0 END) as fh\
                    FROM agg_sector_fulfillment_day a \
                    INNER JOIN sectors b ON a.postcode_sector=b.sector \
                    WHERE scrape_date>=:start and scrape_date<=:end\
                    AND fulfillment_type='vendor'\
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
            'delivery_population',COALESCE(MAX(delivery_population),0),\
            'rx_num', COALESCE(MAX(rx_num),0)\
            ) as country_stats\
        FROM agg_country_fulfillment_day\
        WHERE scrape_date>=:start AND scrape_date<=:end\
        AND fulfillment_type='vendor'\
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
        FROM agg_delivery_zone\
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
                            SELECT rx_uid, cx_postcode FROM rooscrape.foodhoover_store.rx_cx_scrape\
                            WHERE DATE(scrape_time)>='"+start+"' and DATE(scrape_time)<='"+end+"'\
                            AND rx_uid IN(\
                                SELECT distinct(rx_uid) from rooscrape.foodhoover_store.rx_ref\
                                WHERE place_id IN UNNEST(@place_ids)\
                            )\
                            GROUP BY rx_uid, cx_postcode) results\
                        LEFT JOIN rooscrape.foodhoover_store.postcode_lookup pc on pc.postcode=results.cx_postcode\
                        LEFT JOIN rooscrape.foodhoover_store.sectors sectors on sectors.sector = pc.postcode_sector\
                        LEFT JOIN rooscrape.foodhoover_store.rx_ref ref on ref.rx_uid = results.rx_uid\
                        LEFT JOIN rooscrape.foodhoover_store.places places on places.place_id = ref.place_id\
                        GROUP BY places.place_id, ref.vendor, sectors.sector) bysector\
                    LEFT JOIN rooscrape.foodhoover_store.sectors sectors on sectors.sector = bysector.sector\
                    RIGHT JOIN rooscrape.foodhoover_store.places places on places.place_id = bysector.place_id\
                    WHERE places.place_id IN UNNEST(@place_ids)\
                    GROUP BY places.place_id, bysector.vendor\
                ) agg_delivery_zone\
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


def get_chains_boundary(chain, start, end, last_update):

    old_bq_sql = "\
            DECLARE rx_uids ARRAY <STRING>;\
            SET rx_uids = (SELECT ARRAY_AGG(distinct(ref.rx_uid) LIMIT 10000) FROM rooscrape.foodhoover_store.places\
                    LEFT join rooscrape.foodhoover_store.rx_ref ref on ref.place_id = places.place_id\
                    WHERE UPPER(place_name) LIKE UPPER(@chain)\
                    );\
            WITH raw as (\
            SELECT ref.vendor, look.postcode_sector, ARRAY_AGG(DISTINCT coverage.rx_uid) as rx_included FROM (\
                SELECT rx_uid, cx_postcode FROM rooscrape.foodhoover_store.rx_cx_scrape\
                WHERE DATE(scrape_time)>=@start AND DATE(scrape_time)<=@end\
                AND rx_uid IN UNNEST(rx_uids)\
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

    bq_sql = """
            DECLARE place_ids ARRAY<STRING>;
            SET place_ids = (SELECT ARRAY_AGG(distinct(place_id) LIMIT 10000) FROM rooscrape.foodhoover_store.places
                    WHERE UPPER(place_name) LIKE UPPER(@chain)
                    );
            WITH 
                raw as (
                    SELECT vendor, sector_seen, ARRAY_AGG(DISTINCT rx_uid) as rx_included
                    FROM rooscrape.foodhoover_store.rx_cx_sector, UNNEST(sectors_seen) as sector_seen
                    WHERE scrape_date>=@start AND scrape_date<=@end  
                    AND place_id IN UNNEST(place_ids)
                    GROUP BY vendor, sector_seen
                    ),
                distinct_zones as (
                    SELECT 
                        vendor,
                        ARRAY_AGG(DISTINCT area_covered IGNORE NULLS) as areas,
                        ARRAY_AGG(DISTINCT (CASE WHEN area_covered is NULL THEN district_covered ELSE NULL END) IGNORE NULLS) as districts,
                        ARRAY_AGG(DISTINCT (CASE WHEN district_covered is NULL THEN sector_seen ELSE NULL END) IGNORE NULLS) as sectors,
                        SUM(delivery_population) as delivery_population,
                        ARRAY_CONCAT_AGG(rx_included) as rx_included
                    FROM (
                        SELECT 
                            vendor,
                            postcode_sector,
                            CASE WHEN
                                SAFE_DIVIDE(
                                    SUM(CASE WHEN sector_seen is not null then sector_area else 0 END) OVER(pc_area) ,sum(sector_area) OVER(pc_area) )>=0.9
                            THEN postcode_area
                            ELSE null
                            END as area_covered,
                            CASE WHEN
                                SAFE_DIVIDE(
                                    SUM(CASE WHEN sector_seen is not null then sector_area else 0 END) OVER(pc_district) ,sum(sector_area) OVER(pc_district) )>=0.9
                            THEN postcode_district
                            ELSE null
                            END as district_covered,
                            sector_seen, 
                            CASE WHEN sector_seen is not null then sector_population else 0 END as delivery_population,
                            rx_included as rx_included
                        FROM (
                            SELECT * FROM rooscrape.foodhoover_store.geo_mappings 
                            CROSS JOIN UNNEST(['JE','FH','UE','ROO']) as vendor_list
                            LEFT JOIN raw ON raw.sector_seen=geo_mappings.postcode_sector and raw.vendor=vendor_list)
                        WINDOW 
                            pc_area AS (PARTITION BY vendor_list, postcode_area),
                            pc_district AS (PARTITION BY vendor_list, postcode_district)
                    )
                    GROUP BY vendor)
            SELECT 
                w.vendor, 
                ST_ASGEOJSON(ST_UNION(ST_DUMP(ST_SIMPLIFY(ST_UNION_AGG(geometry), 200),2))) as delivery_area,
                MAX(delivery_population) as delivery_population,
                MAX(rx_included) as rx_num
            FROM(
                SELECT vendor,geometry FROM distinct_zones, UNNEST(areas) as areas_seen
                INNER JOIN rooscrape.foodhoover_store.areas on areas.area=areas_seen
                UNION ALL
                SELECT vendor,geometry FROM distinct_zones, UNNEST(districts) as districts_seen
                INNER JOIN rooscrape.foodhoover_store.districts on districts.district=districts_seen
                UNION ALL
                SELECT vendor, geometry FROM distinct_zones, UNNEST(sectors) as sectors_seen
                INNER JOIN rooscrape.foodhoover_store.sectors on sectors.sector=sectors_seen
                ) w
            LEFT JOIN (
                SELECT vendor, MAX(delivery_population) as delivery_population, COUNT(DISTINCT rx_list) as rx_included
                FROM distinct_zones, UNNEST(rx_included) as rx_list
                GROUP BY vendor)
                u ON w.vendor=u.vendor 
            WHERE w.vendor is NOT NULL
            GROUP BY vendor
    """

    sql = text("""
        SELECT
            pop_stats.vendor,
            pop_stats.delivery_population,
            results.rx_num as rx_num,
            ST_ASGEOJSON(results.delivery_area) as delivery_area
            FROM (
                SELECT
                    vendor,
                    sum(sectors.population) as delivery_population
                FROM (
                    SELECT
                        vendor,
                        UNNEST(sectors_covered) as inc_sector
                    FROM agg_delivery_zone
                    where UPPER(place_name) LIKE UPPER(:chain)
                    GROUP BY vendor, UNNEST(sectors_covered)
                    ) sectors_covered
                LEFT JOIN sectors on sectors.sector=sectors_covered.inc_sector
                GROUP by sectors_covered.vendor
                ) pop_stats
        LEFT JOIN
            (SELECT
                vendor,
                ST_SIMPLIFY(ST_UNION(ST_CollectionExtract(ST_MAKEVALID(delivery_zone),3)),0.001) as delivery_area,
                COUNT(DISTINCT place_id) as rx_num
            FROM agg_delivery_zone
            WHERE UPPER(place_name) LIKE UPPER(:chain)
            GROUP BY vendor) results ON results.vendor=pop_stats.vendor
        WHERE delivery_area IS NOT NULL
        """)

    max_cache = last_update
    min_cache = max_cache - timedelta(days=14)
    max_cache = max_cache.strftime('%Y-%m-%d')
    min_cache = min_cache.strftime('%Y-%m-%d')

    if ((min_cache == start) & (max_cache== end)):
        engine = get_sql_client('foodhoover_cache')
        conn = engine.connect()
        print(chain)
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
    sql = "select min(scrape_date) as first_update, max(scrape_date) as last_update from agg_country_fulfillment_day"
    engine = get_sql_client('foodhoover_cache')
    conn = engine.connect()
    result = conn.execute(sql)

    return result.fetchone()

def get_download(start, end):

    sql = """
    SELECT 
        places.place_id, 
        MAX(place_name) as place_name, 
        MAX(place_sector) as postcode_sector,
        MIN(first_seen_live) as first_seen,
        CASE WHEN 'ROO' IN UNNEST(ARRAY_AGG(vendor)) THEN true ELSE false END as Deliveroo,
        CASE WHEN 'UE' IN UNNEST(ARRAY_AGG(vendor)) THEN true ELSE false END as UberEats,
        CASE WHEN 'JE' IN UNNEST(ARRAY_AGG(vendor)) THEN true ELSE false END as JustEat,
        CASE WHEN 'FH' IN UNNEST(ARRAY_AGG(vendor)) THEN true ELSE false END as FoodHub,
    FROM rooscrape.foodhoover_store.rx_ref 
    LEFT JOIN rooscrape.foodhoover_store.places ON places.place_id=rx_ref.place_id
    WHERE process_date=@end AND EXTRACT(DATE FROM last_seen_live)>=@start
    group by places.place_id
    """
    client = get_bq_client()

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("end", "DATE",end),
            bigquery.ScalarQueryParameter("start", "DATE",start),   
        ]
    )

    query_job = client.query(sql, job_config=job_config)

    add_header = True
    output = io.StringIO()
    writer = csv.writer(output, quoting=csv.QUOTE_NONNUMERIC)
    for row in query_job.result():
        if add_header:
            header = list(dict(row).keys())
            writer.writerow(header)
            add_header = False
        row = [str(value) for value in row]
        writer.writerow(row)

    return output.getvalue()


def get_status(last_update):

    today = date.today()
    yesterday = today - timedelta(days=1)
    day_to_check = max(last_update, yesterday).strftime('%Y-%m-%d')

    sql = text("""
        SELECT 
            status.*, 
            CASE WHEN live_rx>10000 AND new_rx_with_metadata>0 AND new_rx_with_google_place_id>0 THEN 2 ELSE CASE WHEN live_rx>10000 THEN 1 ELSE 0 END END as status
        FROM (
            SELECT 
                vendor, 
                count(1) as all_rx,
                SUM(CASE WHEN last_seen_live>=:day_to_check THEN 1 ELSE 0 END) as live_rx, 
                SUM(CASE WHEN first_seen_live>=:day_to_check THEN 1 ELSE 0 END) as new_rx,
                SUM(CASE WHEN first_seen_live>=:day_to_check and rx_sector is not null THEN 1 ELSE 0 END) as new_rx_with_metadata,
                SUM(CASE WHEN first_seen_live>=:day_to_check and google_place_id is not null THEN 1 ELSE 0 END) as new_rx_with_google_place_id
            FROM rx_ref
            group by vendor
        ) status
    """)

    engine = get_sql_client('foodhoover_cache')
    conn = engine.connect()
    result = conn.execute(sql, day_to_check=day_to_check)

    return  [{'vendor':r['vendor'], 'all_rx': r['all_rx'],'live_rx': r['live_rx'],'new_rx': r['new_rx'],'new_rx_with_metadata': r['new_rx_with_metadata'],'new_rx_with_google_place_id': r['new_rx_with_google_place_id'], 'status':r['status']} for r in result]

