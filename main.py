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
from src.utils import hashers, static, user_info
from src.rec import get_batch
from src.productSearch import productSearch
from src.autocomplete import searchSuggestions
from src.trending import trendingSearches, labelSearches
from src.event_upload import upload_event
import src.single_product_info as spi 
from src.product_price_history import get_product_price_history
import src.write_user_boards as wub
import src.read_user_boards as rub
import src.user_brand_actions as uba
import src.board_suggestions as bs
from src import orders
from src import loadProducts  
from src.similarProducts import getSimilarProducts

app = Flask(__name__)

SEARCH_URL = 'http://174.138.109.133/'
SEARCH_PSWD = "kian_is_on_fleek"
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
    data = loadProducts.loadProducts(request.args)
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
    data = trendingSearches(request.args, trending_index, index)
    return jsonify(data)

@app.route('/getLabelSearches', methods=['GET'])
def getLabelSearches():
    data = labelSearches(request.args, label_index, index)
    return jsonify(data)

@app.route('/getSimilarItems', methods=['GET'])
def getSimilarItems():
    data = getSimilarProducts(request.args)
    return jsonify(data)

@app.route('/getSingleProductInfo', methods=['GET'])
@cross_origin()
def getSingleProductInfo():
    data = spi.getSingleProductInfo(request.args)
    return jsonify(data)

@app.route('/getProductPriceHistory', methods=['GET'])
def getProductPriceHistory():
    data = get_product_price_history(conn, request.args)
    return jsonify(data)


@app.route('/pushUserEvent', methods=['POST'])
def pushUserEvent():
    data = request.get_json(force=True)
    res = upload_event(data)
    return jsonify({'event is': res})


@app.route('/writeUserFavedBrand', methods=['POST'])
def writeUserFavedBrand():
    data = request.get_json(force=True)
    res = uba.write_user_faved_brand(data)
    return jsonify({'success': res})

@app.route('/writeUserMutedBrand', methods=['POST'])
def writeUserMutedBrand():
    data = request.get_json(force=True)
    res = uba.write_user_muted_brand(data)
    return jsonify({'success': res})

@app.route('/removeUserFavedBrand', methods=['POST'])
def removeUserFavedBrand():
    data = request.get_json(force=True)
    res = uba.rm_user_faved_brand(data)
    return jsonify({'success': res})

@app.route('/removeUserMutedBrand', methods=['POST'])
def removeUserMutedBrand():
    data = request.get_json(force=True)
    res = uba.rm_user_muted_brand(data)
    return jsonify({'success': res})

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
    res = wub.create_new_board(data)
    return jsonify(res)

@app.route('/writeProductToBoard', methods=['POST'])
def writeProductToBoard():
    data = request.get_json(force=True)
    res = wub.write_product_to_board(data)
    return jsonify(res)

@app.route('/getBoardInfo', methods=['GET'])
def getBoardInfo():
    res = rub.getBoardInfo(request.args)
    return jsonify(res)

@app.route('/getBoardProductsBatch', methods=['GET'])
def getBoardProductsBatch():
    res = rub.getBoardProductsBatch(request.args)
    return jsonify(res)

@app.route('/getUserBoardsBatch', methods=['GET'])
def getUserBoardsBatch():
    res = rub.getUserBoardsBatch(request.args)
    return jsonify(res)

@app.route('/getUserFavedBrands', methods=['GET'])
def getUserFavedBrands():
    return jsonify(
        user_info.get_user_fave_brands(
            hashers.apple_id_to_user_id_hash(request.args['user_id']))
    )

@app.route('/getProductColorOptions', methods=['GET'])
def getProductColorOptions():
    return jsonify(
        loadProducts.getProductColorOptions(request.args)
    )

@app.route('/getOrdersForAdvertiser', methods=['GET'])
def getOrdersForAdvertiser():
    res = orders.getOrdersFromAdvertiser(request.args)
    return jsonify(res)

@app.route('/getProductsFromAdvertiser', methods=['GET'])
def getProductsFromAdvertiser():
    res = orders.getProductsFromAdvertiser(request.args)
    return jsonify(res)

@app.route('/getBoardSuggestions', methods=['GET'])
def getBoardSuggestions():
    return jsonify(
        bs.getBoardSuggestions(request.args)
    )

if __name__ == '__main__':
    # This is used when running locally. Gunicorn is used to run the
    # application on Google App Engine. See entrypoint in app.yaml.
    app.run(host='0.0.0.0', port=5000, debug=True)
