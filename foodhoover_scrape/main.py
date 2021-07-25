from flask import Flask, request, jsonify, render_template
from scrape_bq import bq_get_places, bq_places_table, bq_places_proc, bq_run_scrape, bq_insert_new_rx, bq_update_ue_sectors, bq_crawl_roo, bq_agg_results_district, bq_agg_results_sector, bq_agg_rx_cx, bq_export_places, bq_update_geos, bq_agg_rx_results_fast, bq_step_logger, bq_agg_results_country, bq_export_rx_cx_results,bq_export_rx_ref,bq_export_agg_sector_run, bq_export_agg_district_run, bq_export_agg_country_run,bq_export_agg_rx_cx
from status import get_run_status, get_ref_stats, get_country_stats
import uuid

# If `entrypoint` is not defined in app.yaml, App Engine will look for an app
# called `app` in `main.py`.
app = Flask(__name__)

def bq_post_process(steps, run_id):
    results = []
    if steps == ['ALL']:
        steps = ['INSERT','UPDATE-ROO','UPDATE-UE','UPDATE-GEOS','GET-PLACES','PROC-PLACES','CREATE-PLACES','AGG-RESULTS-DISTRICT','AGG-RESULTS-SECTOR', 'RX-RESULTS-FAST','AGG-RESULTS-COUNTRY','AGG-RX-CX','EXPORT-PLACES','EXPORT-RX-CX-RESULTS','EXPORT-RX-REF','EXPORT-AGG-SECTOR-RUN','EXPORT-AGG-DISTRICT-RUN','EXPORT-AGG-COUNTRY-RUN','EXPORT-AGG-RX-CX']
    for step in steps:
        if step == 'INSERT':
            result = bq_insert_new_rx(run_id)
            print(result)
            results.append(step+": "+str(result))
        elif step == 'UPDATE-ROO': 
            result = bq_crawl_roo(run_id)
            print(result)
            results.append(step+": "+str(result))
        elif step == 'UPDATE-UE':
            result = bq_update_ue_sectors(run_id)
            print(result)
            results.append(step+": "+str(result))
        elif step == 'UPDATE-GEOS':
            result = bq_update_geos(run_id)
            print(result)
            results.append(step+": "+str(result))  
        elif step == 'GET-PLACES':
            result = bq_get_places(run_id)
            print(result)
            results.append(step+": "+str(result))
        elif step == 'PROC-PLACES':
            result = bq_places_proc(run_id)
            print(result)
            results.append(step+": "+str(result)) 
        elif step == 'CREATE-PLACES':
            print(step)
            result = bq_places_table(run_id)
            print(result)
            results.append(step+": "+str(result))  
        elif step == 'AGG-RESULTS-DISTRICT':
            result = bq_agg_results_district(run_id)
            print(result)
            results.append(step+": "+str(result))
        elif step == 'AGG-RESULTS-SECTOR':
            result = bq_agg_results_sector(run_id)
            print(result)
            results.append(step+": "+str(result))
        elif step == 'RX-RESULTS-FAST':
            result = bq_agg_rx_results_fast(run_id)
            print(result)
            results.append(step+": "+str(result))
        elif step == 'AGG-RESULTS-COUNTRY':
            result = bq_agg_results_country(run_id)
            print(result)
            results.append(step+": "+str(result))
        elif step == 'AGG-RX-CX':
            result = bq_agg_rx_cx(run_id)
            print(result)
            results.append(step+": "+str(result))
        elif step == 'EXPORT-PLACES':
            result = bq_export_places(run_id)
            print(result)
            results.append(step+": "+str(result))    
        elif step == 'EXPORT-RX-CX-RESULTS':
            result = bq_export_rx_cx_results(run_id)
            print(result)
            results.append(step+": "+str(result))
        elif step == 'EXPORT-RX-REF':
            result = bq_export_rx_ref(run_id)
            print(result)
            results.append(step+": "+str(result))
        elif step == 'EXPORT-AGG-SECTOR-RUN':
            result = bq_export_agg_sector_run(run_id)
            print(result)
            results.append(step+": "+str(result))
        elif step == 'EXPORT-AGG-DISTRICT-RUN':
            result = bq_export_agg_district_run(run_id)
            print(result)
            results.append(step+": "+str(result))
        elif step == 'EXPORT-AGG-COUNTRY-RUN':
            result = bq_export_agg_country_run(run_id)
            print(result)
            results.append(step+": "+str(result))
        elif step == 'EXPORT-AGG-RX-CX':
            result = bq_export_agg_rx_cx(run_id)
            print(result)
            results.append(step+": "+str(result))
        else:
            results.append(step+": Step not found")
    return jsonify(results)

@app.route('/process')
def process():
    run_id = request.args.get('run_id')
    steps = ['ALL']
    return bq_post_process(steps,run_id)

@app.route('/do')
def do():
    run_id = request.args.get('run_id')
    steps = request.args.getlist('steps')
    return bq_post_process(steps,run_id)

@app.route('/trigger')
def trigger():
    run_id = str(uuid.uuid4())
    ###run the main scrape
    bq_step_logger(run_id, 'INIT', 'SUCESS', 'DONE')
    scrape = bq_run_scrape(run_id)

    steps = ['ALL']
    result = bq_post_process(steps,run_id) 

    return "DONE"

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