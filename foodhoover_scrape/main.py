from flask import Flask, request, jsonify, render_template
from scrape import get_scrape_candidates, bq_run_scrape_new
import uuid
import requests
from process import t_post_process, t_step_logger
from datetime import datetime
import gzip
import json

# If `entrypoint` is not defined in app.yaml, App Engine will look for an app
# called `app` in `main.py`.
app = Flask(__name__)

@app.route('/process')
def process():
    process_date = request.args.get('process_date')
    steps = ['ALL']
    return app.response_class(t_post_process(steps,process_date), mimetype='text/event-stream')

@app.route('/do')
def do():
    process_date = request.args.get('process_date')
    steps = request.args.getlist('steps')
    return app.response_class(t_post_process(steps, process_date), mimetype='text/event-stream') 

@app.route('/trigger') 
def trigger(): 

    def scrape_runner(vendors_to_scrape, process_date, run_id):
        fetch_datas = get_scrape_candidates(vendors_to_scrape)
        scraper = bq_run_scrape_new(fetch_datas, run_id, mode='market')
        for batch_count in scraper:
            yield batch_count + '\n'
        t_step_logger(process_date, 'SCRAPE', 'SUCESS', batch_count)

        ##do other post processing
        steps = ["ALL"]
        processor = t_post_process(steps,process_date) 
        for step in processor:
            yield step + '\n'

    try:
        run_id = str(uuid.uuid4())
        process_date = datetime.today().strftime('%Y-%m-%d')
        t_step_logger(process_date, 'INIT', 'SUCESS', 'DONE')
        vendors_to_scrape = ['JE','ROO','FH', 'UE']
        return app.response_class(scrape_runner(vendors_to_scrape, process_date, run_id), mimetype='text/event-stream')
    except Exception as e:
        t_step_logger(process_date, 'SCRAPE', 'FAIL', str(e))
        return e

@app.route('/test') 
def scrape_test():
    postcode = request.args.get('postcode')
    vendors_to_scrape = ['JE','ROO','FH', 'UE']
    fetch_datas = get_scrape_candidates(vendors_to_scrape, postcode)

    def make_scrape_uri(fetch_data):
        vendor_string = '&vendors='.join(fetch_data['vendors'])
        uri = "https://europe-west2-rooscrape.cloudfunctions.net/foodhoover_get?mode=scrape&postcode={postcode}&lat={lat}&lng={lng}&geohash={geohash}&vendors={vendor_string}&run_id={run_id}".format(
        #uri = "http://192.168.1.207:8080/foodhoover_get?mode=scrape&postcode={postcode}&lat={lat}&lng={lng}&geohash={geohash}&vendors={vendor_string}&run_id={run_id}".format(
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

    response = gzip.decompress(response.content).decode('utf8')

    final = {"uri": uri, "response": json.loads(response)}
    
    return jsonify(final)

@app.route('/full_export')
def full_export():
    steps = ['EXPORT-RX-REF','EXPORT-PLACES','EXPORT-AGG-DELIVERY-ZONE','EXPORT-AGG-COUNTRY-FULFILLMENT-DAY','EXPORT-AGG-DISTRICT-FULFILLMENT-DAY','EXPORT-AGG-SECTOR-FULFILLMENT-DAY']
    process_date = 'full'
    return t_post_process(steps,process_date)

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)