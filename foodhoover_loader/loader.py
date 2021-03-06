import pandas as pd
from google.cloud import bigquery
import geojson
import geopandas as gpd
import numpy as np
import time

from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

from connections import get_sql_client

def load_postcodes():

    pc_headers = ['postcode','status','usertype','easting','northing','positional_quality_indicator','country','latitude','longitude','postcode_no_space','postcode_fixed_width_seven','postcode_fixed_width_eight','postcode_area','postcode_district','postcode_sector','outcode','incode']
    postcode_lookup = pd.read_csv('postcodes/open_postcode_geo.csv',header=0,names=pc_headers)

    postcode_lookup['latitude'] = pd.to_numeric(postcode_lookup['latitude'],errors='coerce')
    postcode_lookup['longitude'] = pd.to_numeric(postcode_lookup['longitude'],errors='coerce')

    postcode_lookup['postcode_district'] = postcode_lookup['postcode_district'].str.replace(' ','')
    postcode_lookup['postcode_sector'] = postcode_lookup['postcode_sector'].str.replace(' ','')

    postcode_lookup = postcode_lookup[['postcode_no_space','status','country','latitude','longitude','postcode_area','postcode_district','postcode_sector']]
    postcode_lookup.rename(columns={'postcode_no_space': 'postcode'}, inplace=True)

    ##add a geometry which describes the center of each postcode
    ##postcode_lookup['postcode_point'] = np.where((~np.isnan(postcode_lookup['longitude']) & ~np.isnan(postcode_lookup['latitude'])), gpd.points_from_xy(postcode_lookup['longitude'],postcode_lookup['latitude']),None)
    postcode_lookup['postcode_point'] = None
    postcode_lookup['postcode_geohash'] = None
    
    ##load the postcodes metadata
    metadata = pd.read_csv('metadata/postcodes_data.csv',on_bad_lines='warn', usecols=[0,8])
    metadata['postcode'] = metadata['PCD'].str.replace(' ','')

    postcode_lookup = postcode_lookup.merge(metadata[['postcode','Total_Persons']], on='postcode', how='left')

    ##send to bigquery
    client = bigquery.Client.from_service_account_json('rooscrape-gbq.json')
    table_id = "rooscrape.foodhoover_store.postcode_lookup"

    ##delete current table
    client.delete_table(table_id, not_found_ok=True)

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("postcode_point", bigquery.enums.SqlTypeNames.GEOGRAPHY),
            bigquery.SchemaField("postcode_geohash", bigquery.enums.SqlTypeNames.STRING)],
        write_disposition="WRITE_TRUNCATE"
    )

    job = client.load_table_from_dataframe(
        postcode_lookup, table_id, job_config=job_config
    )
    job.result()

    ###update the add a centre point to each sector
    sql = "UPDATE "+table_id+ " SET postcode_point = ST_GEOGPOINT(longitude, latitude), postcode_geohash= ST_GEOHASH(ST_GEOGPOINT(longitude, latitude),12) where longitude is not null and latitude is not null"
    job = client.query(sql)
    job.result()

    table = client.get_table(table_id)  # Make an API request.

    return (
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )

def load_sectors():

    fp = "boundaries/Sectors.shp"
    sectors_df = gpd.read_file(fp)

    ###adapt to lat/lng and convert to geojson
    sectors_df['json_geometry'] = sectors_df['geometry'].to_crs(epsg=4326)
    sectors_df['json_geometry'] = sectors_df['json_geometry'].apply(lambda x: geojson.dumps(x))

    ###add dummy columns we will use later
    sectors_df = sectors_df[['StrSect','json_geometry']]
    sectors_df.columns = ['sector','json_geometry']
    sectors_df['geometry'] = np.nan
    sectors_df['sector_centre'] = np.nan
    sectors_df['closest_postcode'] = np.nan
    sectors_df['population'] = np.nan
    sectors_df['sector_area'] = np.nan
    
    ###write table to big query
    client = bigquery.Client.from_service_account_json('rooscrape-gbq.json')
    table_id = "rooscrape.foodhoover_store.sectors"

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("sector", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("geometry", bigquery.enums.SqlTypeNames.GEOGRAPHY),
            bigquery.SchemaField("sector_centre", bigquery.enums.SqlTypeNames.GEOGRAPHY),
            bigquery.SchemaField("closest_postcode", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("json_geometry", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("population", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("sector_area", bigquery.enums.SqlTypeNames.FLOAT)
            ],
        write_disposition="WRITE_TRUNCATE",
    )

    job = client.load_table_from_dataframe(
        sectors_df, table_id, job_config=job_config
    )
    job.result()

    table = client.get_table(table_id)  # Make an API request.

    return (
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )

def sector_manipulations():
    
    client = bigquery.Client.from_service_account_json('rooscrape-gbq.json')
    table_id = "rooscrape.foodhoover_store.sectors"
    
    ###update the geometry column
    sql = "UPDATE "+table_id+ " SET geometry=ST_SIMPLIFY(IFNULL(SAFE.ST_GEOGFROMGEOJSON(json_geometry),ST_GEOGFROMGEOJSON(json_geometry, make_valid => TRUE)),5) where json_geometry is not null "
    job = client.query(sql)
    job.result()

    ###update the add a centre point and area to each sector
    sql = "UPDATE "+table_id+ " SET sector_centre = ST_CENTROID(geometry), sector_area=ST_AREA(geometry) where geometry is not null"
    job = client.query(sql)
    job.result()

    ###add the closest point
    sql = "MERGE "+table_id+" sectors \
        USING \
        (SELECT \
        a.sector, \
        ARRAY_AGG(b.postcode ORDER BY ST_Distance(a.sector_centre, b.postcode_point) LIMIT 1) \
            [ORDINAL(1)] as closest_postcode \
        FROM rooscrape.foodhoover_store.sectors a JOIN rooscrape.foodhoover_store.postcode_lookup b \
        ON ST_DWithin(a.sector_centre, b.postcode_point, 10000) \
        WHERE b.status='live' and a.sector=b.postcode_sector\
        GROUP BY a.sector) as close \
        ON sectors.sector=close.sector \
        WHEN MATCHED THEN \
        UPDATE SET sectors.closest_postcode = close.closest_postcode"
        
    job = client.query(sql)
    job.result()

    ###add the population to each sector
    sql = "UPDATE `rooscrape.foodhoover_store.sectors` a \
        SET a.population = CAST(b.population as INT64) \
        FROM \
            (SELECT postcode_sector, sum(Total_Persons) as population from  \
            `rooscrape.foodhoover_store.postcode_lookup` \
            GROUP BY postcode_sector) b\
        WHERE a.sector = b.postcode_sector\
    "
    job = client.query(sql)
    job.result()

    return "Done sector manipulations"

def load_districts():

    fp = "boundaries/Districts.shp"
    districts_df = gpd.read_file(fp)

    ###adapt to lat/lng and convert to geojson
    districts_df['json_geometry'] = districts_df['geometry'].to_crs(epsg=4326)
    districts_df['json_geometry'] = districts_df['json_geometry'].apply(lambda x: geojson.dumps(x))

    ###add dummy columns we will use later
    districts_df = districts_df[['name','json_geometry']]
    districts_df.columns = ['district','json_geometry']
    districts_df['geometry'] = np.nan
    districts_df['district_centre'] = np.nan 
    districts_df['population'] = np.nan 
    districts_df['district_area'] = np.nan 
    
    ###write table to big query
    client = bigquery.Client.from_service_account_json('rooscrape-gbq.json')
    table_id = "rooscrape.foodhoover_store.districts"

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("district", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("geometry", bigquery.enums.SqlTypeNames.GEOGRAPHY),
            bigquery.SchemaField("json_geometry", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("district_centre", bigquery.enums.SqlTypeNames.GEOGRAPHY),
            bigquery.SchemaField("population", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("district_area", bigquery.enums.SqlTypeNames.FLOAT)
            ],
        write_disposition="WRITE_TRUNCATE",
    )

    job = client.load_table_from_dataframe(
        districts_df, table_id, job_config=job_config
    )
    job.result()

    table = client.get_table(table_id)  # Make an API request.

    return (
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )

def district_manipulations():

    client = bigquery.Client.from_service_account_json('rooscrape-gbq.json')
    table_id = "rooscrape.foodhoover_store.districts"
    
    ###update the geometry column
    sql = "UPDATE "+table_id+ " SET geometry= ST_SIMPLIFY(IFNULL(SAFE.ST_GEOGFROMGEOJSON(json_geometry),ST_GEOGFROMGEOJSON(json_geometry, make_valid => TRUE)),100) where json_geometry is not null"
    job = client.query(sql)
    job.result()

    ###update the add a centre point and area to each sector
    sql = "UPDATE "+table_id+ " SET district_centre = ST_CENTROID(geometry), district_area=ST_AREA(geometry) where geometry is not null "
    job = client.query(sql)
    job.result()

    ###add the population to each district
    sql = "UPDATE `rooscrape.foodhoover_store.districts` a \
        SET a.population = CAST(b.population as INT64) \
        FROM \
            (SELECT postcode_district, sum(Total_Persons) as population from  \
            `rooscrape.foodhoover_store.postcode_lookup` \
            GROUP BY postcode_district) b \
        WHERE a.district = b.postcode_district\
    "
    job = client.query(sql)
    job.result()

    return "Done district manipulations"

def load_areas():

    fp = "boundaries/Areas.shp"
    areas_df = gpd.read_file(fp)

    ###adapt to lat/lng and convert to geojson
    areas_df['json_geometry'] = areas_df['geometry'].to_crs(epsg=4326)
    areas_df['json_geometry'] = areas_df['json_geometry'].apply(lambda x: geojson.dumps(x))

    ###add dummy columns we will use later
    areas_df = areas_df[['name','json_geometry']]
    areas_df.columns = ['area','json_geometry']
    areas_df['geometry'] = np.nan
    areas_df['area_centre'] = np.nan 
    areas_df['population'] = np.nan 
    areas_df['area_area'] = np.nan

    ###write table to big query
    client = bigquery.Client.from_service_account_json('rooscrape-gbq.json')
    table_id = "rooscrape.foodhoover_store.areas"

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("area", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("geometry", bigquery.enums.SqlTypeNames.GEOGRAPHY),
            bigquery.SchemaField("json_geometry", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("area_centre", bigquery.enums.SqlTypeNames.GEOGRAPHY),
            bigquery.SchemaField("population", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("area_area", bigquery.enums.SqlTypeNames.FLOAT)
            ],
        write_disposition="WRITE_TRUNCATE",
    )

    job = client.load_table_from_dataframe(
        areas_df, table_id, job_config=job_config
    )
    job.result()

    table = client.get_table(table_id)  # Make an API request.

    return (
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )

def area_manipulations():

    client = bigquery.Client.from_service_account_json('rooscrape-gbq.json')
    table_id = "rooscrape.foodhoover_store.areas"
    
    ###update the geometry column
    sql = "UPDATE "+table_id+ " SET geometry= ST_SIMPLIFY(IFNULL(SAFE.ST_GEOGFROMGEOJSON(json_geometry),ST_GEOGFROMGEOJSON(json_geometry, make_valid => TRUE)),100) where json_geometry is not null"
    job = client.query(sql)
    job.result()

    ###update the add a centre point and area to each sector
    sql = "UPDATE "+table_id+ " SET area_centre = ST_CENTROID(geometry), area_area=ST_AREA(geometry) where geometry is not null "
    job = client.query(sql)
    job.result()

    ###add the population to each area
    sql = "UPDATE `rooscrape.foodhoover_store.areas` a \
        SET a.population = CAST(b.population as INT64) \
        FROM \
            (SELECT postcode_area, sum(Total_Persons) as population from  \
            `rooscrape.foodhoover_store.postcode_lookup` \
            GROUP BY postcode_area) b \
        WHERE a.area = b.postcode_area\
    "
    job = client.query(sql)
    job.result()

    return "Done area manipulations"

def bq_to_gcs(sql, gcs_filename):
    client = bigquery.Client.from_service_account_json('rooscrape-gbq.json')

    job_config = bigquery.QueryJobConfig()
    bucket_name = 'rooscrape-exports'

    table_ref = client.dataset('foodhoover_store').table('my_temp_table')
    job_config.destination = table_ref

    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    # Start the query, passing in the extra configuration.
    query_job = client.query(
        sql,
        location='US',
        job_config=job_config)

    while not query_job.done():
        time.sleep(1)

    #check if table successfully written
    print("query completed")
    job_config = bigquery.ExtractJobConfig()
    job_config.compression = bigquery.Compression.GZIP
    job_config.destination_format = (bigquery.DestinationFormat.CSV)
    job_config.print_header = False

    destination_uri = 'gs://{}/{}'.format(bucket_name, gcs_filename)

    extract_job = client.extract_table(
        table_ref,
        destination_uri,
        job_config=job_config,
        location='US')  # API request
    extract_job.result()
    
    ##clean up temp table
    client.delete_table(table_ref, not_found_ok=True)
    
    return destination_uri

def import_sql(file_path, columns, table_name, write_mode):
    credentials = GoogleCredentials.from_stream('rooscrape-gbq.json')
    service = discovery.build('sqladmin', 'v1beta4', credentials=credentials)

    if write_mode=='overwrite':
        engine = get_sql_client('foodhoover_cache')
        conn = engine.connect()
        sql = "DELETE FROM "+table_name+" where 1=1"
        result = conn.execute(sql)
        conn.close()
    
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

    request = service.instances().import_(project=project, instance=instance, body=instances_import_request_body)
    response = request.execute()

    operation_id = response['name']
    status = 'PENDING'
    loop = 0
    while (status in ['PENDING','RUNNING']) & (loop<100):
        loop = loop+1
        time.sleep(5)
        request = service.operations().get(project='rooscrape', operation=operation_id)
        response = request.execute()
        status = response['status']
    
    return status

def export_sectors():
    try:
        sql = "SELECT sector,  TO_HEX(ST_ASBINARY(geometry)) as geometry, TO_HEX(ST_ASBINARY(sector_centre)) as sector_centre, closest_postcode, population, sector_area FROM foodhoover_store.sectors"
        gcs_filename = 'sectors.gzip'
        file_path =  bq_to_gcs(sql, gcs_filename)
        status = import_sql(file_path, ['sector','geometry','sector_centre','closest_postcode','population','sector_area'], "sectors",'overwrite')
        return "Sectors exported"
    except Exception as e:
        return e

def export_districts():
    try:
        sql = "SELECT district,  TO_HEX(ST_ASBINARY(geometry)) as geometry, TO_HEX(ST_ASBINARY(district_centre)) as district_centre, population, district_area FROM foodhoover_store.districts"
        gcs_filename = 'districts.gzip'
        file_path =  bq_to_gcs(sql, gcs_filename)
        status = import_sql(file_path, ['district','geometry','district_centre','population', 'district_area'], "districts",'overwrite')
        return "Districts exported"
    except Exception as e:
        return e

def export_areas():
    try:
        sql = "SELECT area,  TO_HEX(ST_ASBINARY(geometry)) as geometry, TO_HEX(ST_ASBINARY(area_centre)) as area_centre, population, area_area FROM foodhoover_store.areas"
        gcs_filename = 'areas.gzip'
        file_path =  bq_to_gcs(sql, gcs_filename)
        status = import_sql(file_path, ['area','geometry','area_centre','population','area_area'], "areas",'overwrite')
        return "Areas exported"
    except Exception as e:
        return e

def export_postcodes():
    try:
        sql = "SELECT postcode, status, country, latitude, longitude, postcode_area, postcode_district, postcode_sector, TO_HEX(ST_ASBINARY(postcode_point)) as postcode_point,postcode_geohash, Total_Persons FROM foodhoover_store.postcode_lookup"
        gcs_filename = 'postcode_lookup.gzip'
        file_path =  bq_to_gcs(sql, gcs_filename)
        status = import_sql(file_path, ['postcode', 'status', 'country', 'latitude', 'longitude', 'postcode_area', 'postcode_district', 'postcode_sector','postcode_point','postcode_geohash','Total_Persons'], "postcode_lookup",'overwrite')
        return "Postcodes exported"
    except Exception as e:
        return e

