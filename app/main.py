from flask import Flask, jsonify, request
from google.cloud import bigquery
from bigquery_handling import *

app = Flask(__name__)

@app.route('/vehicles/<key>', methods=['GET'])
def func(key):
    
    bq_client = bigquery.Client(project = 'peaceful-tide-284813')
    results = get_results(key, bq_client)
    return jsonify(results)
    

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)