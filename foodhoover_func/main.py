import json
import uuid
import requests
import re
from bs4 import BeautifulSoup

from functools import partial
import asyncio
import sqlalchemy

from connectionslight import get_bq_client, get_sql_client


def get_je_blob(data):

    postcode = data['postcode']
    postcode_area = data['postcode_area']
    run_id = data['run_id']

    api = 'https://uk.api.just-eat.io/restaurants/bypostcode/'
    url = api+postcode
    
    blob = {}
    rx_open = 0
    rx_total = 0
    open_set = []
    
    try: 
        r = requests.get(url)
        try:
            blob = json.loads(r.content.decode('utf-8'))
            restaurants_list = blob['Restaurants']
            rx_total = len(restaurants_list)
            open_set = [{'vendor':'JE','cx_postcode':postcode,'rx_name':restaurant['Name'],'rx_postcode':restaurant['Address']['Postcode'].replace(" ", ""),'rx_menu':restaurant['Url'], 'rx_slug':restaurant['UniqueName'], 'rx_lat':restaurant['Address']['Latitude'],'rx_lng':restaurant['Address']['Longitude'],'run_id':run_id} for restaurant in restaurants_list if restaurant['IsOpenNow']]
            rx_open = len(open_set)
            status = 'Sucess'
        except Exception as e:
            status = 'Parse Error: ' +str(e)  
    except Exception as e:
        status='Fetch Error:' + str(e)    
    
    blob = None

    result = {
        'run_id' : run_id,
        'cx_postcode' : postcode,
        'vendor':'JE',
        'rx_open':rx_open,
        'status':status
    }

    return {'result':result, 'dataset':open_set}

def get_ue_blob(data):

    postcode = data['postcode']
    postcode_area = data['postcode_area']
    lat = data['lat']
    lng = data['lng']
    run_id = data['run_id']
    
    url = 'https://www.ubereats.com/api/getFeedV1?localeCode=gb'
    
    new_uuid = str(uuid.uuid4())
    header = {
    'authority': 'www.ubereats.com',
    'method': 'POST',
    'path': '/api/getFeedV1?localeCode=gb',
    'scheme': 'https',
    'accept': '*/*',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
    'content-length': '919',
    'content-type': 'application/json',
    'cookie': 'uev2.id.xp='+new_uuid+';uev2.loc=%7B%22address%22%3A%7B%22address1%22%3A%22SW8%204JU%22%2C%22address2%22%3A%22Canonbury%20Park%20N%2C%20London%22%2C%22aptOrSuite%22%3A%22%22%2C%22eaterFormattedAddress%22%3A%22Canonbury%20Park%20N%2C%20London%20N1%202JT%2C%20UK%22%2C%22subtitle%22%3A%22Canonbury%20Park%20N%2C%20London%22%2C%22title%22%3A%22N1%202JT%22%2C%22uuid%22%3A%22%22%7D%2C%22latitude%22%3A'+str(lat)+'%2C%22longitude%22%3A'+str(lng)+'%2C%22reference%22%3A%22ChIJFetJrmIbdkgRgaOYxYyy4B4%22%2C%22referenceType%22%3A%22google_places%22%2C%22type%22%3A%22google_places%22%2C%22source%22%3A%22manual_auto_complete%22%2C%22addressComponents%22%3A%7B%22countryCode%22%3A%22%22%2C%22firstLevelSubdivisionCode%22%3A%22%22%2C%22city%22%3A%22%22%2C%22postalCode%22%3A%22%22%7D%2C%22originType%22%3A%22user_autocomplete%22%7D;',
    'origin': 'https://www.ubereats.com',
    'referer': 'https://www.ubereats.com/gb/feed',
    'sec-ch-ua': '"Google Chrome";v="87", " Not;A Brand";v="99", "Chromium";v="87"',
    'sec-ch-ua-mobile': '?0',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (X11; CrOS x86_64 13505.111.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.152 Safari/537.36',
    'x-csrf-token': 'x',
    }
    
    post = {
    'billboardUuid': '',
    'carouselId': '',
    'date': '',
    'endTime': 0,
    'feedProvider': '',
    'feedSessionCount': {'announcementCount': 9, 'announcementLabel': 'subscription.analytics_label'},
    'marketingFeedType': '',
    'pageInfo': {'offset': 1, 'pageSize': 2000},
    'showSearchNoAddress': 'false',
    'startTime': 0,
    'userQuery': ''
    }
    
    blob = {}
    rx_open = 0
    rx_total = 0
    open_set = []
    
    try: 
        r = requests.post(url, data=json.dumps(post), headers=header)
        try:
            blob = json.loads(r.content.decode('utf-8'))
            restaurants = blob['data']['storesMap'].items()
            restaurants_list = [restaurant[1] for restaurant in restaurants]
            rx_total = len(restaurants_list)
            open_set = [{'vendor':'UE','cx_postcode':postcode,'rx_name':restaurant['title'],'rx_menu':restaurant['slug'], 'rx_slug':restaurant['uuid'],'rx_postcode':None,'rx_lat':restaurant['location']['latitude'],'rx_lng':restaurant['location']['longitude'],'run_id':run_id} for restaurant in restaurants_list if restaurant['isOpen']]
            rx_open = len(open_set)
            status = 'Sucess'

        except Exception as e:
            status = 'Parse Error: ' +str(e)  
    except Exception as e:
        status='Fetch Error:' + str(e)   
    
    blob = None

    result = {
        'run_id' : run_id,
        'cx_postcode' : postcode,
        'vendor':'UE',
        'rx_open':rx_open,
        'status':status
    }

    return {'result':result, 'dataset':open_set}


def get_fh_auth():
    auth_url = "https://api.t2sonline.com/oauth/client?api_token=99b8ad5d2f9e80889efcd73bc31f7e7b&app_name=FOODHUB"
    
    auth_headers={'authority': 'api.t2sonline.com',
              'method': 'POST',
              'path': '/oauth/client?api_token=99b8ad5d2f9e80889efcd73bc31f7e7b&app_name=FOODHUB',
              'scheme': 'https',
              'accept': 'application/json, text/plain, */*',
              'accept-encoding': 'gzip, deflate, br',
              'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
              'content-length': '112',
              'content-type': 'application/x-www-form-urlencoded',
              'origin': 'https://foodhub.co.uk',
              'referer': 'https://foodhub.co.uk/',
              'sec-fetch-dest': 'empty',
              'sec-fetch-mode': 'cors',
              'sec-fetch-site': 'cross-site',
              'user-agent': 'Mozilla/5.0 (X11; CrOS x86_64 13421.102.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.199 Safari/537.36'
             }
    
    new_uuid = str(uuid.uuid4())
    
    auth_post = {'name':'Chrome',
             'auth_type':'CONSUMER',
             'platform_id':'1',
             'product_id':'4',
             'uuid':'1591263365962'+new_uuid
            }
    
    r = requests.post(auth_url, data=auth_post, headers=auth_headers)
    result = json.loads(r.content.decode('utf-8'))
    
    auth_token = result['data']['access_token']  
    
    return auth_token

def get_fh_blob(data):

    postcode = data['postcode']
    postcode_area = data['postcode_area']
    run_id = data['run_id']

    auth_token = get_fh_auth()
    url = 'https://api.t2sonline.com/foodhub/takeaway/list?api_token=99b8ad5d2f9e80889efcd73bc31f7e7b&app_name=FOODHUB'
    post = {'postcode': postcode}
    header = {'authority': 'api.t2sonline.com',
        'method': 'POST',
        'path': '/foodhub/takeaway/list?api_token=99b8ad5d2f9e80889efcd73bc31f7e7b&app_name=FOODHUB',
        'scheme': 'https',
        'accept': 'application/json, text/plain, */*',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'authorization': 'Bearer '+auth_token,
        'content-length': '17',
        'content-type': 'application/x-www-form-urlencoded',
        'origin': 'https://foodhub.co.uk',
        'passport': '1',
        'referer': 'https://foodhub.co.uk/',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'user-agent': 'Mozilla/5.0 (X11; CrOS x86_64 13421.102.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.199 Safari/537.36}'
    }    

    blob = {}
    rx_open = 0
    rx_total = 0
    open_set= []
    
    try: 
        r = requests.post(url, data=post, headers=header)
        try:
            blob = json.loads(r.content.decode('utf-8'))
            restaurants_list = blob['data']
            rx_total = len(restaurants_list)
            open_set = [{'vendor':'FH','cx_postcode':postcode,'rx_name':restaurant['name'],'rx_menu':None, 'rx_slug':str(restaurant['id']),'rx_postcode':restaurant['postcode'].replace(" ", ""),'rx_lat':restaurant['lat'],'rx_lng':restaurant['lng'],'run_id':run_id} for restaurant in restaurants_list if restaurant['takeaway_open_status']=='open']
            rx_open = len(open_set)
            status = 'Sucess'
        except Exception as e:
            status = 'Parse Error: ' +str(e)  
    except Exception as e:
        print(e)
        status='Fetch Error:' + str(e)  

    blob = None

    result = {
        'run_id' : run_id,
        'cx_postcode' : postcode,
        'vendor':'FH',
        'rx_open':rx_open,
        'status':status
    }

    return {'result':result, 'dataset':open_set}


def get_roo_blob(data):

    postcode = data['postcode']
    postcode_area = data['postcode_area']
    run_id = data['run_id']

    url = "https://deliveroo.co.uk/restaurants/london/new-cross?postcode="+postcode+"&collection=all-restaurants"

    blob = []
    rx_open = 0
    rx_total = 0
    open_set = []
    
    try: 
        r = requests.get(url)
        try:
            feed = BeautifulSoup(r.content.decode('utf-8'), features="lxml").find_all("div", {"class": "HomeFeedUICard-9e4c25acad3130ed"})
            for rx in feed:
                try:
                    rx_link = rx.findAll("a")[0]['href']
                    rx_name = re.search(r'.*\/(.*)\?', rx_link).group(1)
                    if 'time=ASAP' in rx_link:##detect preorder
                        rx_status = 'Open'
                    else: 
                        rx_status= 'Pre-order'
                    rx_record = {'rx_uuid':rx_name,'rx_menu':rx_link,'rx_status':rx_status}
                    blob.append(rx_record)
                    if rx_status == 'Open':
                        open_set_record = {'vendor':'ROO','cx_postcode':postcode,'rx_menu':rx_link,'rx_slug':rx_name,'rx_name':None,'rx_postcode':None,'rx_lat':None,'rx_lng':None,'run_id':run_id}
                        open_set.append(open_set_record)
                except:
                    pass
            rx_total = len(feed)
            rx_open = len(open_set)
            status = 'Sucess'
        except Exception as e:
            status = 'Parse Error: ' +str(e)  
    except Exception as e:
        print(e)
        status='Fetch Error:' + str(e)  

    blob = None

    result = {
        'run_id' : run_id,
        'cx_postcode' : postcode,
        'vendor':'ROO',
        'rx_open':rx_open,
        'status':status
    }

    return {'result':result, 'dataset':open_set}


def foodhoover(request):
    mode = request.args.get('mode')
    
    if mode=='availability':
        loop = asyncio.new_event_loop()
        scrape_response = loop.run_until_complete(open_status(request))

    elif mode=='flash':
        loop = asyncio.new_event_loop()
        scrape_response = loop.run_until_complete(open_status(request))
    
    elif mode == 'roo_rx':
        scrape_response = get_roo_metadata(request)
    else:
        scrape_response = 'error'
    
    return scrape_response

async def open_status(request):
    postcode = request.args.get('postcode')
    postcode_area = request.args.get('postcode_area')
    lat = request.args.get('lat')
    lng = request.args.get('lng')
    vendors = request.args.getlist('vendors')
    run_id = request.args.get('run_id')
    mode = request.args.get('mode')

    results = []

    loop = asyncio.get_event_loop()
        
    futures = []

    if 'UE' in vendors:
        futures.append(loop.run_in_executor(None, partial(get_ue_blob, {'postcode':postcode, 'postcode_area':postcode_area, 'lat':lat,'lng':lng, 'run_id':run_id})))

    if 'JE' in vendors:
        futures.append(loop.run_in_executor(None, partial(get_je_blob, {'postcode':postcode, 'postcode_area':postcode_area, 'run_id':run_id})))

    if 'FH' in vendors:
         futures.append(loop.run_in_executor(None, partial(get_fh_blob, {'postcode':postcode, 'postcode_area':postcode_area, 'run_id':run_id})))
    
    if 'ROO' in vendors:
        futures.append(loop.run_in_executor(None, partial(get_roo_blob, {'postcode':postcode, 'postcode_area':postcode_area, 'run_id':run_id})))

    results = await asyncio.gather(*futures)
    
    final_result = []
    dataset = []
    for result in results:
        final_result.append(result['result'])
        dataset = dataset + result['dataset']
    
    for result in dataset: 
        result['scrape_time']='AUTO'

    if mode == 'flash': ##write to SQL if flash
        flash_dataset = [{'rx_uid':r['rx_slug']+'-'+r['vendor'],'cx_postcode':r['cx_postcode'], 'run_id':r['run_id']} for r in dataset]
        write_sql(flash_dataset, 'rx_cx_fast_flash')
    else:
        if len(dataset)>0:
            write_bq(dataset, 'rx_cx_results_raw') ##write to BQ
        for result in final_result: 
            result['scrape_time']='AUTO'
        if len(final_result)>0:
            write_bq(final_result, 'scrape_event') ##write to BQ

    return json.dumps(final_result)

def write_bq(results, table_name):
    client = get_bq_client()

    dataset = client.dataset('foodhoover_store')

    table_ref = dataset.table(table_name)
    table = client.get_table(table_ref)  # API call
    bq_blob = client.insert_rows_json(table, results)

    client.close()
    return (bq_blob)

def write_sql(results, table_name):
    engine = get_sql_client('foodhoover_cache')
    conn = engine.connect()

    metadata = sqlalchemy.MetaData(bind=engine)
    rx_cx = sqlalchemy.Table(table_name, metadata, autoload=True)

    psql_rx_cx = conn.execute(
        rx_cx.insert().values(results)
    )

    conn.close()

    return 'sql done'

def get_roo_metadata(request):
    rx_url = request.args.get('rx_url')
    rx_ref = {}
    try:
        r = requests.get(rx_url)
    
        try: 
            soup = BeautifulSoup(r.content.decode('utf-8'), features="lxml")
            feed = soup.find_all("script", {"class": "js-react-on-rails-component"})
            metadata = json.loads(feed[0].decode_contents())['restaurant']

            rx_name = metadata['name']
            rx_slug = metadata['uname']
            rx_address = metadata['street_address']
            rx_postcode = metadata['post_code']
            rx_neighbourhood = metadata['neighborhood']
            rx_ref = {'rx_name':rx_name, 'rx_slug':rx_slug,'rx_postcode':rx_postcode}
            rx_cache = {'scrape_time':'AUTO','rx_slug':rx_slug,'rx_name':rx_name,'rx_address':rx_address,'rx_postcode':rx_postcode,'rx_neighbourhood':rx_neighbourhood}
            try:
                write_bq([rx_cache], 'roo_cache')
                status = 'success'
            except Exception as e:
                status = 'DB Error: ' +str(e) 
        except Exception as e:
            status = 'Parse Error: ' +str(e)  
    except Exception as e:
        status = 'Fetch Error: ' +str(e)  
    
    return (status, rx_ref) 