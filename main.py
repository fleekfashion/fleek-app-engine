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
import meilisearch

from flask import Flask, jsonify, request
from flask_cors import cross_origin
from six.moves import http_client

from sqlalchemy import create_engine, MetaData, Table

from src.defs.postgres import DATABASE_USER, PASSWORD, DBNAME, PROJECT
from src.utils import hashers, static
from src.rec import get_batch
from src import rec2
from src.productSearch import productSearch
from src.autocomplete import searchSuggestions
from src.trending import trendingSearches, labelSearches
from src.event_upload import upload_event
from src.single_product_info import get_single_product_info
from src.product_price_history import get_product_price_history
import src.boards as b

app = Flask(__name__)


SEARCH_URL = 'http://161.35.113.38/'
SEARCH_PSWD = "fleek-app-prod1"
conn = psycopg2.connect(user=DATABASE_USER, password=PASSWORD,
                        host='localhost', port='5431', dbname=DBNAME)
c = meilisearch.Client(SEARCH_URL, SEARCH_PSWD)
index = c.get_index(f"{PROJECT}_products")
ac_index = c.get_index(f"{PROJECT}_autocomplete")
trending_index = c.get_index(f"{PROJECT}_trending_searches")
label_index = c.get_index(f"{PROJECT}_labels")
del c

@app.route('/getUserProductBatch', methods=['GET'])
def getUserProductBatch():
    args = request.args
    user_id = args.get("user_id", -1)
    if user_id != -1:
        user_id = hashers.apple_id_to_user_id_hash(user_id)
    data = get_batch(conn, user_id, request.args)
    return jsonify(data)

@app.route('/getUserProductBatchv2', methods=['GET'])
def getUserProductBatchv2():
    args = request.args
    user_id = args.get("user_id", -1)
    if user_id != -1:
        user_id = hashers.apple_id_to_user_id_hash(user_id)
    data = rec2.get_batch(conn, user_id, request.args)
    return jsonify(data)

@app.route('/getProductSearchBatch', methods=['GET'])
def getProductSearchBatch():
    data = productSearch(request.args, index)
    return jsonify(data)

@app.route('/getSearchSuggestions', methods=['GET'])
def getSearchSuggestions():
    data = searchSuggestions(request.args, ac_index)
    return jsonify(data)

@app.route('/getTrendingSearches', methods=['GET'])
def getTrendingSearches():
    data = trendingSearches(request.args, trending_index)
    return jsonify(data)

@app.route('/getLabelSearches', methods=['GET'])
def getLabelSearches():
    data = labelSearches(request.args, label_index)
    return jsonify(data)

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

@app.route('/getProductPriceHistory', methods=['GET'])
def getProductPriceHistory():
    data = get_product_price_history(conn, request.args)
    return jsonify(data)


@app.route('/pushUserEvent', methods=['POST'])
def pushUserEvent():
    data = request.get_json(force=True)
    res = upload_event(conn, data)
    return jsonify({'event is': res})

@app.route('/repeat', methods=['POST'])
def repeat():
    """Simple echo service."""
    message = request.get_json().get('message', '')
    return jsonify({'message': "nahhh"})

@app.route('/getStaticSizeOptions', methods=['GET'])
def getStaticSizeOptions():
    return jsonify({'sizes': ["xxs", "xs", "xs/s", "s","s/m", "m", "m/l", "l", "xl", "xxl", "2xl"]})

@app.route('/getAdvertiserNames', methods=['GET'])
def getAdvertiserNames():
    return jsonify(static.get_advertiser_names())

@app.route('/getAdvertiserCounts', methods=['GET'])
def getAdvertiserCounts():
    return jsonify(static.get_advertiser_counts())

@app.route('/createNewBoard', methods=['POST'])
def createNewBoard():
    data = request.get_json(force=True)
    res = b.create_new_board(data)
    return jsonify(res)

@app.route('/writeProductToBoard', methods=['POST'])
def writeProductToBoard():
    data = request.get_json(force=True)
    res = b.write_product_to_board(data)
    return jsonify(res)

if __name__ == '__main__':
    # This is used when running locally. Gunicorn is used to run the
    # application on Google App Engine. See entrypoint in app.yaml.
    app.run(host='0.0.0.0', port=5000, debug=True)
