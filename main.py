import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="C:\\Users\\Yalun Zheng\\Downloads\\My First Project-09f761785342.json"

from flask import Flask, request, jsonify

from google.cloud import bigquery
from bigquery_handling import *

app = Flask(__name__)

@app.route('/vehicles/<key>', methods=['GET'])
def func(key):
    
    if len(key) == 6:
        return "Do not support QR code yet"

    
    client = bigquery.Client(project='peaceful-tide-284813')
    results = get_results(key, client)

    return jsonify(results)

if __name__ == '__main__':
    app.run()
