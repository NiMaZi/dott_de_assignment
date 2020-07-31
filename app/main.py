from flask import Flask, request
from flask_caching import Cache
from google.cloud import bigquery

try:
    from .bigquery_handling import *
except:
    from bigquery_handling import *

PROJECT = 'peaceful-tide-284813'

app = Flask(__name__)

@app.route('/vehicles/<key>', methods=['GET'])
@cache.cached(timeout = 60)
def func(key):
    
    bq_client = bigquery.Client(project = PROJECT)
    return get_results(key, bq_client)
    

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)