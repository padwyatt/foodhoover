import json
import uuid
import time
from flask import jsonify
import re
import gzip

import asyncio
from aiohttp import ClientSession

async def get_je(data):

    postcode = data['postcode']
    api = 'https://uk.api.just-eat.io/restaurants/bypostcode/'
    url = api+postcode 

    header = {
        'Application-Version': '33.32.0',
        'User-Agent': 'Just Eat/33.32.0 (1935) iOS 14.7.1/iPhone',
        'Accept-Language': 'en-GB',
        'Accept-Charset': 'utf-8',
        'Content-Type': 'application/json',
        'Accept-Encoding': 'gzip',
        'Accept': '*/*',
        'Connection': 'keep-alive'
    }

    def parse_je(response):
        open_set = []
        try:
            restaurants_list = response['Restaurants']
            for restaurant in restaurants_list:
                if restaurant['IsOpenNow']:
                    try:
                        eta_min = restaurant['DeliveryEtaMinutes']['RangeLower']
                        eta_max = restaurant['DeliveryEtaMinutes']['RangeUpper']
                        eta = (int(eta_min)+int(eta_max))/2
                    except Exception as e:
                        print("Error - could not parse ETA" + str(e))
                        eta = None
                    try:
                        fee = restaurant['DeliveryCost']
                    except Exception as e:
                        print("Error - could not parse fee" + str(e))
                        fee = None
                    try :
                        is_sponsored = restaurant['IsSponsored']
                    except Exception as e:
                        print("Error - could not parse sponsored" + str(e))
                        is_sponsored = None

                    try: 
                        bands = response['deliveryFees']['restaurants'][str(restaurant['Id'])]['bands']
                        if bands in [
                                [{"minimumAmount": 0,"fee": 249}, {"minimumAmount": 700,"fee": 99}],
                                [{"minimumAmount": 0,"fee": 249}, {"minimumAmount": 700,"fee": 199}],
                                [{"minimumAmount": 0,"fee": 350}, {"minimumAmount": 700,"fee": 99}]
                            ]:
                            fulfillment_type = 'vendor'
                        else:
                            fulfillment_type = 'restaurant'
                    except Exception as e:
                        print("Error - could not parse fulfillment type" + str(e))
                        fulfillment_type = 'restaurant'

                    open_set.append({
                        'rx_uid' : restaurant['UniqueName']+'-JE',
                        'rx_slug':restaurant['UniqueName'], 
                        'rx_id':str(restaurant['Id']),
                        'vendor':'JE',
                        'cx_postcode':postcode,
                        'rx_postcode':restaurant['Address']['Postcode'].replace(" ", ""),
                        'rx_lat':restaurant['Address']['Latitude'],
                        'rx_lng':restaurant['Address']['Longitude'],
                        'rx_name':restaurant['Name'],
                        'eta': eta,
                        'fee':fee,
                        'fulfillment_type': fulfillment_type,
                        'is_sponsored':is_sponsored,
                        'rx_menu':restaurant['Url'], 
                    })
            return open_set, 'OK'
        except Exception as e:
            print(e)
            return open_set, 'Parse error: '+str(e)

    async with ClientSession() as session:
        try:
            async with session.get(url, headers=header) as response:
                response = await response.read()
                response = json.loads(response.decode('utf8'))
                payload_size = len(json.dumps(response))
                open_set, status = parse_je(response)
        except Exception as e:
            status =  'Fetch error: '+str(e)
            payload_size = 0
            open_set = []

    return {'vendor':'JE', 'status':status, 'open_set':open_set, 'payload_size':payload_size}

async def get_ue(data):
    postcode = data['postcode']
    lat = data['lat']
    lng = data['lng']

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
        'x-csrf-token': 'x'
    }
    post_page = {
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

    post_init = {
        'billboardUuid': '',
        'carouselId': '',
        'date': '',
        'endTime': 0,
        'feedProvider': '',
        'feedSessionCount': {'announcementCount': 0, 'announcementLabel': ''},
        'marketingFeedType': '',
        'showSearchNoAddress': 'false',
        'startTime': 0,
        'userQuery': ''
    }

    def parse_ue(response):
        open_set = []
        try:
            restaurants = response['data']['feedItems']
            
            for restaurant in restaurants:
                try: ##attempt to get fee and eta data
                    eta_min =  re.search('(\d*)\–?(\d*)', restaurant['store']['meta'][1]['text']).group(1) 
                    eta_max = re.search('(\d*)\–?(\d*)', restaurant['store']['meta'][1]['text']).group(2) 
                    eta = (int(eta_min)+int(eta_max))/2
                except Exception as e:   
                    try:
                        eta_min =  re.search('(\d*)\–?(\d*)', restaurant['store']['meta2'][0]['text']).group(1) 
                        eta_max = re.search('(\d*)\–?(\d*)', restaurant['store']['meta2'][0]['text']).group(2) 
                        eta = (int(eta_min)+int(eta_max))/2
                    except Exception as e: 
                        print("Error - could not parse ETA" + str(e))  
                        eta = None        
                try:     
                    fee = re.search('\d+\.?\d+', restaurant['store']['meta'][0]['text']).group(0) 
                except Exception as e:  
                    fee = None  
                    print("Error - could not parse fee" + str(e))

                if eta != None:
                    open_set.append({
                        'rx_uid' : restaurant['uuid']+'-UE',
                        'rx_slug':None, 
                        'rx_id': restaurant['uuid'],
                        'vendor':'UE',
                        'cx_postcode':postcode,
                        'rx_postcode':None,
                        'rx_lat':None,
                        'rx_lng':None,
                        'rx_name':restaurant['store']['title']['text'],
                        'eta': eta,
                        'fee':fee,
                        'fulfillment_type': None,
                        'is_sponsored':None, ##to add
                        'rx_menu':restaurant['store']['actionUrl'], 
                    })
            return open_set, 'OK'
        except Exception as e:
            print(e)
            return open_set, 'Parse error: '+str(e)

    async with ClientSession() as session:
        try:
            async with session.post(url,json=post_init, headers=header) as response:
                response = await response.json()
                payload_size = len(json.dumps(response))
                open_set, status = parse_ue(response)

                try:
                    print(response['data']['meta']['hasMore'])
                    if response['data']['meta']['hasMore']==True:

                        offset = response['data']['meta']['offset']
                        print(offset)

                        post_page = {
                            'billboardUuid': '',
                            'carouselId': '',
                            'date': '',
                            'endTime': 0,
                            'feedProvider': '',
                            'feedSessionCount': {'announcementCount': 9, 'announcementLabel': 'subscription.analytics_label'},
                            'marketingFeedType': '',
                            'pageInfo': {'offset': offset, 'pageSize': 2000},
                            'showSearchNoAddress': 'false',
                            'startTime': 0,
                            'userQuery': ''
                        }
                        async with session.post(url,json=post_page, headers=header) as response:
                            response = await response.json()
                            payload_size = len(json.dumps(response))
                            open_set_next, status = parse_ue(response)
                            open_set = open_set + open_set_next

                except Exception as e:
                    status = "Next page error: " + str(e)

        except Exception as e:
            status = 'Fetch error: '+str(e)
            payload_size = 0
            open_set = []
    
    return {'vendor':'UE', 'status':status, 'open_set':open_set, 'payload_size':payload_size}

async def get_fh(data):

    postcode = data['postcode']

    ##fetch a token for the authorisation
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

    def parse_fh(response):
        open_set = []

        try: 
            restaurants_list = response['data']       

            for restaurant in restaurants_list:
                if restaurant['takeaway_open_status']=='open':
                    open_set.append({
                        'rx_uid' : str(restaurant['id'])+'-FH',
                        'rx_slug':None, 
                        'rx_id': str(restaurant['id']),
                        'vendor':'FH',
                        'cx_postcode':postcode,
                        'rx_postcode':restaurant['postcode'].replace(" ", ""),
                        'rx_lat':restaurant['lat'],
                        'rx_lng':restaurant['lng'],
                        'rx_name':restaurant['name'],
                        'eta': restaurant['delivery_time'],
                        'fee':restaurant['delivery_charge'],
                        'fulfillment_type': 'restaurant', ##hard coded assumption
                        'is_sponsored':None, 
                        'rx_menu':restaurant['url'], 
                    })
            return open_set, 'OK'
        except Exception as e:
            print(e)
            return open_set, 'Parse error: '+str(e)
    
    async with ClientSession() as session:
        try:
            async with session.post(auth_url,data=auth_post, headers=auth_headers) as response: ##fetch a token for the authorisation
                result = await response.json()
                auth_token = result['data']['access_token']

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
                    'content-type': 'application/x-www-form-urlencoded',
                    'origin': 'https://foodhub.co.uk',
                    'passport': '1',
                    'referer': 'https://foodhub.co.uk/',
                    'sec-fetch-dest': 'empty',
                    'sec-fetch-mode': 'cors',
                    'sec-fetch-site': 'cross-site',
                    'user-agent': 'Mozilla/5.0 (X11; CrOS x86_64 13421.102.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.199 Safari/537.36}'
                }

            async with session.post(url,data=post, headers=header) as response: ##make the main request
                result = await response.json()
                payload_size = len(json.dumps(result))
                open_set, status = parse_fh(result)
        except Exception as e:
            print(e)
            status='Fetch error: '+ str(e)
            payload_size = 0
            open_set = []

    return {'vendor':'FH', 'status':status, 'open_set':open_set, 'payload_size':payload_size}

async def get_roo(data):

    geohash = data['geohash']
    postcode = data['postcode']

    def parse_roo(response):
        try:
            parsed = response["data"]["results"]["layoutGroups"][0]["data"][0]["blocks"]
            open_set = []
            for restaurant in parsed:
                rx_link = restaurant["target"]["restaurant"]["links"]["self"]["href"]
                rx_name = restaurant["target"]["restaurant"]["name"]
                rx_id = restaurant["target"]["restaurant"]["id"]
                rx_slug = re.search(r'.*\/(.*)\?', rx_link).group(1)
                try:
                    description = restaurant["contentDescription"]
                    eta_min =  re.search('Delivers in (\d*)\ to ?(\d*) minutes',description).group(1) 
                    eta_max = re.search('Delivers in (\d*)\ to ?(\d*) minutes',description).group(2) 
                    eta = (int(eta_min)+int(eta_max))/2
                except Exception as e: 
                    print("Error - could not parse fee" + str(e))
                    eta = None
                    
                if 'time=ASAP' in rx_link:##detect preorder
                    open_set.append({
                        'rx_uid' : str(rx_id)+'-ROO',
                        'rx_slug':rx_slug, 
                        'rx_id': str(rx_id),
                        'vendor':'ROO',
                        'cx_postcode':postcode,
                        'rx_postcode':None,
                        'rx_lat':None,
                        'rx_lng':None,
                        'rx_name':rx_name,
                        'eta': eta,
                        'fee':None,
                        'fulfillment_type': None,
                        'is_sponsored':None, 
                        'rx_menu':rx_link, 
                    })

            return open_set, 'OK'
        except Exception as e:
            print(e)
            return [], 'Parse error: '+str(e)

    url = "https://api.uk.deliveroo.com/consumer/graphql/"

    headers = {}
    headers["authority"] = "api.uk.deliveroo.com"
    headers["method"] = "POST"
    headers["path"] = "/consumer/graphql/"
    headers["scheme"] = "http"
    headers["accept"] = "application/json, application/vnd.api+json"
    headers["accept-encoding"] = "gzip, deflate, br"
    headers["accept-language"] = "en"
    headers["Content-Type"] = "application/json"
    headers["origin"] = "https://deliveroo.co.uk"
    headers["referer"] = "https://deliveroo.co.uk/"
    headers["sec-ch-ua-mobile"] = "?0"
    headers["sec-fetch-dest"] = "empty"
    headers["sec-fetch-mode"] = "cors"
    headers["sec-fetch-site"] = "cross-site"
    headers["user-agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36"
    headers["x-requested-with"] = "XMLHttpRequest"
    headers["x-roo-client"] = "consumer-web-app"
    headers["x-roo-country"] = "uk"

    query = "\
          query getHomeFeed(\
            $ui_blocks: [UIBlockType!]\
            $ui_layouts: [UILayoutType!]\
            $location: LocationInput!\
            $url: String\
            $uuid: String!\
            ) {\
                results: search(\
                location: $location\
                url: $url\
                capabilities: {\
                    ui_blocks: $ui_blocks,\
                    ui_layouts: $ui_layouts\
                }\
                uuid: $uuid\
                ){\
                layoutGroups: ui_layout_groups {\
                    subheader\
                    data: ui_layouts { ...uiLayoutFields }\
                    }\
                }\
            }\
        fragment uiTargetFields on UITarget {\
            ...on UITargetRestaurant {\
            restaurant {\
                id\
                name\
                links {\
                self {\
                    href\
                }\
                }\
            }\
            }\
        }\
        fragment uiBlockFields on UIBlock {\
            ...on UICard {\
            key\
            contentDescription: content_description,\
            target {\
                ...uiTargetFields\
            }\
            }\
        }\
        fragment uiLayoutFields on UILayout {\
            ...on UILayoutList {\
            blocks: ui_blocks {\
                ...uiBlockFields\
            }\
            }\
        }"

    roo_uuid = str(uuid.uuid4())
    payload ={
        "query": query, 
        "variables": {
            "ui_blocks": ["CARD"],
            "fulfillment_methods": ["DELIVERY"],
            "location": {},
            "url": "https://deliveroo.co.uk/restaurants/london/canonbury?geohash="+geohash+"&collection=all-restaurants",
            "uuid": roo_uuid,
            "ui_layouts": ["LIST"]
        }
    }

    async with ClientSession() as session:
        try:
            async with session.post(url,data=json.dumps(payload), headers=headers) as response: ##make the main request
                result = await response.json()
                payload_size = len(json.dumps(result))
                open_set, status = parse_roo(result)
        except Exception as e:
            print(e)
            status='Fetch error: '+ str(e)
            payload_size = 0
            open_set = []

    return {'vendor':'ROO', 'status':status, 'open_set':open_set, 'payload_size':payload_size}

async def crawl_roo(rx_id):

    def parse_roo_details(response):
        try:
            final = {
                'scrape_status' : 'OK',
                'roo_details': {
                    'scrape_time' : time.time(),
                    'rx_id' : response['id'],
                    'rx_name' : response['name'],
                    'rx_slug' : response['uname'],
                    'rx_neighbourhood' : response['neighborhood']['name'],
                    'rx_city' : response['city'],
                    'rx_lng' : response['coordinates'][0],
                    'rx_lat' : response['coordinates'][1],
                    'rx_prep_time' : response['curr_prep_time'],
                    'rx_address' : response['address']['address1'],
                    'rx_postcode' : response['address']['post_code'],
                    'rx_fulfillment_type' : response['fulfillment_type'],
                    'rx_menu_page' : response['share_url'],
                    'rx_blob' : json.dumps(response)
                }
            }
        except Exception as e:
            final = {
                'scrape_status' : 'Parse Error: ' + str(e),
                'roo_details': {
                    'scrape_time' : time.time(),
                    'rx_id' :rx_id,
                    'rx_name' : None,
                    'rx_slug' : None,
                    'rx_neighbourhood' : None,
                    'rx_city' : None,
                    'rx_lng' : None,
                    'rx_lat' : None,
                    'rx_prep_time' : None,
                    'rx_address' : None,
                    'rx_postcode' : None,
                    'rx_fulfillment_type' : None,
                    'rx_menu_page' : None,
                    'rx_blob' : json.dumps(response)
                }
            }
        return final

    url = 'https://api.uk.deliveroo.com/orderapp/v1/restaurants/'+rx_id+'?include_unavailable=true&fulfillment_method=DELIVERY&restaurant_fulfillments_supported=true'
    async with ClientSession() as session:
        try:
            async with session.get(url) as response:
                response = await response.json()
                return parse_roo_details(response)
        except Exception as e:
            final = {
                'scrape_status' : 'Fetch Error: ' + str(e),
                'roo_details': {
                    'scrape_time' : time.time(),
                    'rx_id' :rx_id,
                    'rx_name' : None,
                    'rx_slug' : None,
                    'rx_neighbourhood' : None,
                    'rx_city' : None,
                    'rx_lng' : None,
                    'rx_lat' : None,
                    'rx_prep_time' : None,
                    'rx_address' : None,
                    'rx_postcode' : None,
                    'rx_fulfillment_type' : None,
                    'rx_menu_page' : None,
                    'rx_blob' : None
                }
            }
            return final


async def crawl_ue(rx_id):

    def parse_ue_details(response):
        try:
            ue_data = response['data']

            try:
                rx_neighbourhood = ue_data['location']['geo']['neighborhood']
            except:
                rx_neighbourhood = None

            try:
                rx_city = ue_data['location']['geo']['city']
            except:
                rx_city = None

            final = {
                'scrape_status' : 'OK',
                'ue_details': {
                    'scrape_time' : time.time(),
                    'rx_id' : ue_data['uuid'],
                    'rx_name' : ue_data['title'],
                    'rx_slug' : ue_data['slug'],
                    'rx_neighbourhood' : rx_neighbourhood,
                    'rx_city' : rx_city,
                    'rx_lng' : ue_data['location']['longitude'],
                    'rx_lat' : ue_data['location']['latitude'],
                    'rx_prep_time' : None,
                    'rx_address' : ue_data['location']['streetAddress'],
                    'rx_postcode' : None,
                    'rx_fulfillment_type' : 'restaurant' if ue_data['isDeliveryThirdParty'] else 'vendor',
                    'rx_menu_page' : None,
                    'rx_blob' : json.dumps(ue_data)
                    }
            } 
        except Exception as e:
            final = {
                'scrape_status' : 'Parse Error: ' + str(e),
                'ue_details': {
                    'scrape_time' : time.time(),
                    'rx_id' : rx_id,
                    'rx_name' : None,
                    'rx_slug' : None,
                    'rx_neighbourhood' : None,
                    'rx_city' : None,
                    'rx_lng' : None,
                    'rx_lat' : None,
                    'rx_prep_time' : None,
                    'rx_address' : None,
                    'rx_postcode' : None,
                    'rx_fulfillment_type' : None,
                    'rx_menu_page' : None,
                    'rx_blob' : json.dumps(response)
                }
            }
        return final
    
    lat = 51
    lng = 0
    new_uuid = str(uuid.uuid4())
    header = {
        'authority': 'www.ubereats.com',
        'method': 'POST',
        'path': '/api/getFeedV1?localeCode=gb',
        'scheme': 'https',
        'accept': '*/*',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
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
        'x-csrf-token': 'x'
    }
    url = "https://www.ubereats.com/api/getStoreV1?localeCode=gb"
    data = '{"storeUuid":"'+rx_id+'","sfNuggetCount":13}'
    
    async with ClientSession() as session:
        try:
            async with session.post(url=url,data=data, headers=header) as response:
                response = await response.json()
                return parse_ue_details(response)
        except Exception as e:
                final = {
                    'scrape_status' : 'Fetch Error: ' + str(e),
                    'ue_details': {
                        'scrape_time' : time.time(),
                        'rx_id' : rx_id,
                        'rx_name' : None,
                        'rx_slug' : None,
                        'rx_neighbourhood' : None,
                        'rx_city' : None,
                        'rx_lng' : None,
                        'rx_lat' : None,
                        'rx_prep_time' : None,
                        'rx_address' : None,
                        'rx_postcode' : None,
                        'rx_fulfillment_type' : None,
                        'rx_menu_page' : None,
                        'rx_blob' : None
                    }
                }
                return final

def foodhoover_get(request):

    if request.args.get('mode')=='scrape':
        data = {
            'postcode': request.args.get('postcode'),
            'lat': request.args.get('lat'),
            'lng': request.args.get('lng'),
            'geohash':request.args.get('geohash')
        }

        vendors = request.args.getlist('vendors')
        run_id = request.args.get('run_id')

        try:
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy()) ##on windows, this is to stop an error that the loop isn't closed
        except:
            pass

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        futures = []

        if 'UE' in vendors:
            futures.append(get_ue(data))
        if 'JE' in vendors:
            futures.append(get_je(data))
        if 'FH' in vendors:
            futures.append(get_fh(data))
        if 'ROO' in vendors:
            futures.append(get_roo(data))

        scrapes_to_run = asyncio.gather(*futures)
        scrape_responses = loop.run_until_complete(scrapes_to_run)
        loop.close()

        scrape_status = []
        open_set = []
        for vendor_response in scrape_responses:
            scrape_status.append({
                'run_id':run_id,
                'scrape_time':time.time(),
                'cx_postcode':data['postcode'],
                'vendor': vendor_response['vendor'],
                'rx_open': len(vendor_response['open_set']),
                'status': vendor_response['status'],
                'payload_size': vendor_response['payload_size']
            })
            for rx in vendor_response['open_set']:
                rx['scrape_time'] = time.time()
                rx['run_id'] = run_id
                open_set.append(rx)

        final = {
            'scrape_status' : scrape_status,
            'open_set': open_set
        }

        final = json.dumps(final).encode('utf8')
        
        return gzip.compress(final)

    elif request.args.get('mode')=='roo':

        try:
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy()) ##on windows, this is to stop an error that the loop isn't closed
        except:
            pass

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        rx_ids = request.args.getlist('rx_id')

        futures = []
        for rx_id in rx_ids:
            futures.append(crawl_roo(rx_id))

        scrapes_to_run = asyncio.gather(*futures)
        scrape_responses = loop.run_until_complete(scrapes_to_run)
        loop.close()

        return jsonify(scrape_responses)

    elif request.args.get('mode')=='ue':

        try:
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy()) ##on windows, this is to stop an error that the loop isn't closed
        except:
            pass

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        rx_ids = request.args.getlist('rx_id')

        futures = []
        for rx_id in rx_ids:
            futures.append(crawl_ue(rx_id))

        scrapes_to_run = asyncio.gather(*futures)
        scrape_responses = loop.run_until_complete(scrapes_to_run)
        loop.close()

        return jsonify(scrape_responses)


    else:
        return "Error: mode not found"
 
