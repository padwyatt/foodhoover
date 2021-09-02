from flask import Flask, request, jsonify, render_template, Response
from process import bq_post_process, bq_step_logger
from scrape import get_scrape_candidates, bq_run_scrape_new
from status import get_run_status, get_ref_stats, get_country_stats
import uuid
import time
import requests

# If `entrypoint` is not defined in app.yaml, App Engine will look for an app
# called `app` in `main.py`.
app = Flask(__name__)

@app.route('/process')
def process():
    run_id = request.args.get('run_id')
    steps = ['ALL']
    return app.response_class(bq_post_process(steps,run_id), mimetype='text/event-stream')

@app.route('/do')
def do():
    run_id = request.args.get('run_id')
    steps = request.args.getlist('steps')
    return app.response_class(bq_post_process(steps,run_id), mimetype='text/event-stream') 

@app.route('/trigger') 
def trigger():

    def scrape_runner(vendors_to_scrape, run_id):
        fetch_datas = get_scrape_candidates(vendors_to_scrape)
        scraper = bq_run_scrape_new(fetch_datas, run_id, mode='market')
        for batch_count in scraper:
            yield batch_count + '\n'
        bq_step_logger(run_id, 'SCRAPE', 'SUCESS', batch_count)

        ##do other post processing
        steps = ['ALL']
        processor = bq_post_process(steps,run_id) 
        for step in processor:
            yield step + '\n'

    try:
        run_id = str(uuid.uuid4())
        bq_step_logger(run_id, 'INIT', 'SUCESS', 'DONE')
        vendors_to_scrape = ['JE','ROO','FH', 'UE']
        return app.response_class(scrape_runner(vendors_to_scrape, run_id), mimetype='text/event-stream')
    except Exception as e:
        bq_step_logger(run_id, 'SCRAPE', 'FAIL', str(e))
        return e

@app.route('/test') 
def scrape_test():
    postcode = request.args.get('postcode')
    vendors_to_scrape = ['JE','ROO','FH', 'UE']
    fetch_datas = get_scrape_candidates(vendors_to_scrape, postcode)

    def make_scrape_uri(fetch_data):
        vendor_string = '&vendors='.join(fetch_data['vendors'])
        #old_uri = "https://europe-west2-rooscrape.cloudfunctions.net/foodhoover?mode=availability&postcode={postcode}&postcode_area={postcode}&lat={lat}}&lng={lng}&geohash={geohash}&vendors={vendor_string}&run_id={run_id}".format(
        uri = "https://europe-west2-rooscrape.cloudfunctions.net/foodhoover_get?mode=scrape&postcode={postcode}&lat={lat}&lng={lng}&geohash={geohash}&vendors={vendor_string}&run_id={run_id}".format(
            postcode=fetch_data['postcode'],
            lat=fetch_data['lat'], 
            lng=fetch_data['lng'], 
            geohash=fetch_data['geohash'], 
            vendor_string=vendor_string,
            run_id = 'test'
            )
        
        return uri
    
    uri = make_scrape_uri(fetch_datas[0])

    response = requests.get(uri)

    final = {"uri": uri, "response": response.json()}
    
    return jsonify(final)

@app.route('/full_export')
def full_export():
    steps = ['EXPORT-RX-CX-RESULTS','EXPORT-RX-REF','EXPORT-AGG-SECTOR-RUN','EXPORT-AGG-DISTRICT-RUN','EXPORT-AGG-COUNTRY-RUN']
    run_id = 'full'
    return bq_post_process(steps,run_id)

@app.route('/')
def render_status():
    return render_template('status.html', run_status=get_run_status(), ref_stats = get_ref_stats(), country_stats=get_country_stats())

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)