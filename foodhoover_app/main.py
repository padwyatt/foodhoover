from flask import Flask, render_template, request, jsonify
from datetime import datetime, timedelta
from get_data import get_country_data, get_restaurant_details, get_rx_names, get_geo_objects, get_delivery_boundary, get_flash, count_flash

app = Flask(__name__)

from flask_cachebuster import CacheBuster
config = { 'extensions': ['.js', '.css', '.csv'], 'hash_size': 5 }
cache_buster = CacheBuster(config=config)
cache_buster.init_app(app)

@app.route('/')
def index(): 

    if 'tab' in request.args:
        tab_name = request.args.get('tab')
    else:
        tab_name = 'country'
    
    if 'start' in request.args:
        start = request.args.get('start')
    else:
        start = (datetime.now() - timedelta(14)).strftime('%Y-%m-%d')

    if 'end' in request.args:
        end = request.args.get('end')
    else:
        end = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')

    place_ids = request.args.getlist('place_id')
    place_details = get_restaurant_details(place_ids)
    return render_template('index.html', place_details=place_details, tab_name = tab_name, start=start, end=end)

@app.route('/country.json')
def country_data():
    start = request.args.get('start')
    end = request.args.get('end')
    lngw = request.args.get('lngw')
    lats = request.args.get('lats')
    lnge = request.args.get('lnge')
    latn = request.args.get('latn')
    zoom = int(request.args.get('zoom'))
    return get_country_data(start, end, lngw, lats, lnge, latn, zoom)

@app.route('/deliveryboundary.json')
def delivery_boundary():
    start = request.args.get('start')
    end = request.args.get('end')
    place_id = request.args.get('place_id')
    return get_delivery_boundary(start, end, place_id)

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

