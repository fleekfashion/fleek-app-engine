# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Google Cloud Endpoints sample application.

Demonstrates how to create a simple echo API as well as how to deal with
various authentication methods.
"""
import base64
import json
import logging
import os
import psycopg2

from flask import Flask, jsonify, request
from flask_cors import cross_origin
import meilisearch
from six.moves import http_client

from src.utils import hashers
from src.rec import get_batch
from src import rec2
from src.event_upload import upload_event
from src.single_product_info import get_single_product_info

app = Flask(__name__)


DATABASE_USER = "postgres"
PASSWORD = "fleek-app-prod1"
DBNAME = "ktest"
conn = psycopg2.connect(user=DATABASE_USER, password=PASSWORD,
                        host='localhost', port='5431', dbname=DBNAME)

c = meilisearch.Client('http://161.35.113.38/', PASSWORD)
index = c.get_index("prod_products")

@app.route('/')
@app.route('/getUserProductBatch', methods=['GET'])
def getUserProductBatch():
    args = request.args
    user_id = args.get("user_id", -1)
    if user_id != -1:
        user_id = hashers.apple_id_to_user_id_hash(user_id)
    data = get_batch(conn, user_id, request.args)
    return jsonify(data)

@app.route('/')
@app.route('/getUserProductBatchv2', methods=['GET'])
def getUserProductBatchv2():
    args = request.args
    user_id = args.get("user_id", -1)
    if user_id != -1:
        user_id = hashers.apple_id_to_user_id_hash(user_id)
    data = rec2.get_batch(conn, user_id, request.args)
    return jsonify(data)

@app.route('/')
@app.route('/getSimilarItems', methods=['GET'])
def getSimilarItems():
    args = request.args
    product_id= args.get("product_id", -1)
    data = rec2.get_similar_items(conn, product_id)
    return jsonify(data)

@app.route('/getSingleProductInfo', methods=['GET'])
@cross_origin()
def getSingleProductInfo():
    args = request.args
    product_id = args.get("product_id", -1)
    if product_id == -1:
        data = {}
    else:
        data = get_single_product_info(conn, product_id)
    return jsonify(data)

@app.route('/pushUserEvent', methods=['POST'])
def pushUserEvent():
    data = request.get_json(force=True)
    res = upload_event(conn, data)
    return jsonify({'event is': res})

@app.route('/getProductSearchBatch', methods=['GET'])
def getProductSearchBatch():
    args = request.args
    searchString = args.get("searchString", "")

    valid_args = ["offset", "limit"]
    opt_params = {}
    for arg in valid_args:
        if arg in args.keys():
            opt_params[arg] = args.get(arg)
    
    res = index.search(query=searchString, opt_params=opt_params)
    print(res)
    return jsonify(res)

if __name__ == '__main__':
    # This is used when running locally. Gunicorn is used to run the
    # application on Google App Engine. See entrypoint in app.yaml.
    app.run(host='0.0.0.0', port=5000, debug=True)
