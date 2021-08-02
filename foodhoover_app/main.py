from flask import Flask, render_template, request, jsonify
from datetime import timedelta
from get_data import get_country_data, get_restaurant_details, get_rx_names, get_geo_objects, get_delivery_boundary, get_flash, count_flash, get_last_update, get_chains_boundary, get_places_in_area
import json

app = Flask(__name__)

from flask_cachebuster import CacheBuster
config = { 'extensions': ['.js', '.css', '.csv'], 'hash_size': 5 }
cache_buster = CacheBuster(config=config)
cache_buster.init_app(app)

f = open('secrets.json')
secrets = json.load(f)
map_secret = secrets['map_key']

first_update, last_update = get_last_update()
start = (last_update - timedelta(14)).strftime('%Y-%m-%d')
end = last_update.strftime('%Y-%m-%d')
print(end)

@app.route('/aggregator')
@app.route('/')
@app.errorhandler(404)
def country_view(start=start,end=end):
    if 'start' in request.args:
        start = request.args.get('start')
    if 'end' in request.args:
        end = request.args.get('end')
    tab_name = 'country'
    return render_template('index.html', place_details=None, chain=None, tab_name = tab_name, start=start, end=end, map_secret=map_secret, first_update=first_update, last_update=last_update)

@app.route('/restaurant')
def restaurant_view(start=start, end=end):
    if 'start' in request.args:
        start = request.args.get('start')
    if 'end' in request.args:
        end = request.args.get('end')
    place_ids = request.args.getlist('place_id')
    place_details = get_restaurant_details(place_ids)
    tab_name = 'resto'
    return render_template('index.html', place_details=place_details, chain=None, tab_name = tab_name, start=start, end=end, map_secret=map_secret, first_update=first_update, last_update=last_update)

@app.route('/chain')
def chain_view(start=start, end=end):
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
    return jsonify(get_country_data(start, end, lngw, lats, lnge, latn, granularity))

@app.route('/deliveryboundary.json')
def delivery_boundary():
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

@app.route('/flash')
def flash():
    lngw = request.args.get('lngw')
    lats = request.args.get('lats')
    lnge = request.args.get('lnge')
    latn = request.args.get('latn')
    place_ids = request.args.getlist('place_id')
    vendors = request.args.getlist('vendors')
    
    return get_flash(lngw, lats, lnge, latn, place_ids, vendors)

@app.route('/count_flash')
def count_flashers():
    lngw = request.args.get('lngw')
    lats = request.args.get('lats')
    lnge = request.args.get('lnge')
    latn = request.args.get('latn')

    return count_flash(lngw, lats, lnge, latn)

if __name__ == '__main__':
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    app.run(host='127.0.0.1', port=8080, debug=True)
# [END gae_python3_app]
# [END gae_python38_app]

