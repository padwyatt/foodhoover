from flask import Flask, request, jsonify, render_template
from loader import load_postcodes, load_sectors, sector_manipulations, load_districts, district_manipulations, export_sectors,export_districts, export_postcodes
# If `entrypoint` is not defined in app.yaml, App Engine will look for an app
# called `app` in `main.py`.
app = Flask(__name__)

def loader(steps):
    results = []
    if steps == ['ALL']:
        steps = ['LOAD-POSTCODES','LOAD-SECTORS','SECTOR-MANIPULATIONS','LOAD-DISTRICTS','DISTRICT-MANIPULATIONS','EXPORT-SECTORS','EXPORT-DISTRICTS','EXPORT-POSTCODES']
    for step in steps:
        if step == 'LOAD-POSTCODES':
            result = load_postcodes()
            print(result)
            results.append(step+": "+str(result))
        elif step == 'LOAD-DISTRICTS':
            result = load_districts()
            print(result)
            results.append(step+": "+str(result))
        elif step == 'DISTRICT-MANIPULATIONS': 
            result = district_manipulations()
            print(result)
            results.append(step+": "+str(result))
        elif step == 'LOAD-SECTORS':
            result = load_sectors()
            print(result)
            results.append(step+": "+str(result))
        elif step == 'SECTOR-MANIPULATIONS': 
            result = sector_manipulations()
            print(result)
            results.append(step+": "+str(result))
        elif step == 'EXPORT-SECTORS': 
            result = export_sectors()
            print(result)
            results.append(step+": "+str(result))
        elif step == 'EXPORT-DISTRICTS': 
            result = export_districts()
            print(result)
            results.append(step+": "+str(result))
        elif step == 'EXPORT-POSTCODES': 
            result = export_postcodes()
            print(result)
            results.append(step+": "+str(result))
        else:
            results.append(step+": Step not found")
    return jsonify(results)

@app.route('/load_all')
def load_all():
    steps = ['ALL']

    return loader(steps)

@app.route('/load_single')
def load_single():
    steps = request.args.getlist('steps')
    print(steps)
    return loader(steps)

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)