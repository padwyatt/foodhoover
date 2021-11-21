from flask import Flask, render_template, request, jsonify, Response
from datetime import timedelta
from get_data import get_country_fulfillment_data, get_restaurant_details, get_rx_names, get_geo_objects, get_delivery_boundary, get_last_update, get_chains_boundary, get_places_in_area, get_download, get_status
import json
import uuid

app = Flask(__name__)

from flask_cachebuster import CacheBuster
config = { 'extensions': ['.js', '.css', '.csv'], 'hash_size': 5 }
cache_buster = CacheBuster(config=config)
cache_buster.init_app(app)

def init():
    f = open('secrets.json')
    secrets = json.load(f)
    map_secret = secrets['map_key']

    first_update, last_update = get_last_update()
    start = (last_update - timedelta(14)).strftime('%Y-%m-%d')
    end = last_update.strftime('%Y-%m-%d')

    status_data = get_status(last_update)
    status = min([r['status'] for r in status_data])

    return start, end, first_update, last_update, map_secret, status

@app.route('/aggregator')
@app.route('/')
#@app.errorhandler(404)
def country_view():
    start, end, first_update, last_update, map_secret, status = init()
    if 'start' in request.args:
        start = request.args.get('start')
    if 'end' in request.args:
        end = request.args.get('end')
    if 'delivery' in request.args:
        delivery = request.args.get('delivery')
    else:
        delivery='all'
    if 'vendor' in request.args:
        vendor = request.args.get('vendor')
    else:
        vendor ='ROO'

    tab_name = 'country'
    return render_template('index.html', place_details=None, chain=None, tab_name = tab_name, start=start, end=end, delivery=delivery, vendor=vendor, map_secret=map_secret, first_update=first_update, last_update=last_update, status=status)

@app.route('/restaurant')
def restaurant_view():
    start, end, first_update, last_update, map_secret, status = init()
    if 'start' in request.args:
        start = request.args.get('start')
    if 'end' in request.args:
        end = request.args.get('end')
    place_ids = request.args.getlist('place_id')
    place_details = get_restaurant_details(place_ids)
    tab_name = 'resto'
    return render_template('index.html', place_details=place_details, chain=None, tab_name = tab_name, start=start, end=end, map_secret=map_secret, first_update=first_update, last_update=last_update)

@app.route('/chain')
def chain_view():
    start, end, first_update, last_update, map_secret, status = init()
    if 'start' in request.args:
        start = request.args.get('start')
    if 'end' in request.args:
        end = request.args.get('end')
    chain = request.args.get('chain')
    tab_name = 'chains'
    print(chain)
    return render_template('index.html', place_details=None, chain=chain, tab_name = tab_name, start=start, end=end, map_secret=map_secret, first_update=first_update, last_update=last_update)

@app.route('/country.json')
def country_data():
    start = request.args.get('start')
    end = request.args.get('end')
    lngw = request.args.get('lngw')
    lats = request.args.get('lats')
    lnge = request.args.get('lnge')
    latn = request.args.get('latn')
    granularity = request.args.get('granularity')
    return jsonify(get_country_fulfillment_data(start, end, lngw, lats, lnge, latn, granularity))

@app.route('/deliveryboundary.json')
def delivery_boundary():
    first_update, last_update = get_last_update()
    start = request.args.get('start')
    end = request.args.get('end')
    place_ids = request.args.getlist('place_id')
    return get_delivery_boundary(start, end, place_ids, last_update)

@app.route('/places.json')
def get_places():
    lngw = request.args.get('lngw')
    lats = request.args.get('lats')
    lnge = request.args.get('lnge')
    latn = request.args.get('latn')
    chain= request.args.get('chain')
    return jsonify(get_places_in_area(chain, lngw, lats, lnge, latn))

@app.route('/restaurant.json')
def restaurant():
    rx_uids = request.args.getlist('rx_uid')
    return get_restaurant_details(rx_uids)

@app.route('/geo_objects.json')
def geo_objects():
    lngw = request.args.get('lngw')
    lats = request.args.get('lats')
    lnge = request.args.get('lnge')
    latn = request.args.get('latn')
    return get_geo_objects(lngw, lats, lnge, latn)

@app.route('/chainsboundary.json')
def chains_boundary():
    first_update, last_update = get_last_update()
    chain= request.args.get('chain')
    start = request.args.get('start')
    end = request.args.get('end')
    return get_chains_boundary(chain, start, end, last_update)

@app.route('/autocomplete', methods=['GET'])
def autocomplete():
    search = request.args.get('q')
    lat = request.args.get('lat')
    lng = request.args.get('lng')
    results = get_rx_names(search, lat, lng)
    return jsonify(matching_results=results)

@app.route('/flash', methods=['POST'])
def flash_get():
    from flash_scrape import to_sync_generator, flash_url_batch, scrape_fetch_function, prepare_flash_scrape

    params = request.get_json()
    run_id = str(uuid.uuid4())
    bounds = params['bounds']  
    place_ids = params['place_ids']
    flash_prep = prepare_flash_scrape(bounds, place_ids)

    if flash_prep['status'] == 'OK':
        return app.response_class(to_sync_generator(flash_url_batch(flash_prep['fetch_datas'], flash_prep['rx_uids'], scrape_fetch_function, 30, flash_prep['postcodes_to_scrape'], run_id)), mimetype='text/event-stream')
    else:
        return flash_prep

@app.route('/download', methods=['GET'])
def download():
    start = request.args.get('start')
    end = request.args.get('end')

    result = get_download(start, end)

    return Response(
        result,
        mimetype="text/csv",
        headers={"Content-disposition":"attachment; filename=restaurants.csv"})

@app.route('/status')
def status():
    first_update, last_update = get_last_update()
    status_data = get_status(last_update)

    return render_template('status.html',status_data=status_data)

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)

