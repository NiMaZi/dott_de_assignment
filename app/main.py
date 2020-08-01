import os
import redis
from flask import Flask, request
from google.cloud import bigquery

try:
    from .bigquery_handling import *
except:
    from bigquery_handling import *

PROJECT = 'peaceful-tide-284813'

app = Flask(__name__)

# redis_host = os.environ.get('REDISHOST')
# redis_port = int(os.environ.get('REDISPORT'))
redis_client = redis.StrictRedis(host = "10.166.182.235", port = 6379)
bq_client = bigquery.Client(project = PROJECT)

@app.route('/vehicles/<key>', methods = ['GET'])
def func(key):
    
    res = redis_client.get(key)
    
    if res is None:
        res = get_results(key, bq_client)
        redis_client.set(key, res, ex = 3600) # 1 hour expiration

    return res
    

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)