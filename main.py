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
from six.moves import http_client

from src.rec import get_batch

app = Flask(__name__)


def _base64_decode(encoded_str):
    # Add paddings manually if necessary.
    num_missed_paddings = 4 - len(encoded_str) % 4
    if num_missed_paddings != 4:
        encoded_str += b'=' * num_missed_paddings
    return base64.b64decode(encoded_str).decode('utf-8')

DATABASE_USER = "postgres"
PASSWORD = "fleek-app-prod1"
DBNAME = "ktest"
conn = psycopg2.connect(user=DATABASE_USER, password=PASSWORD,
                        host='localhost', port='5432', dbname=DBNAME)

@app.route('/testQuery', methods=['GET'])
def test_query():
    data = get_batch(conn, 1, request.args)
    return jsonify(data)

@app.route('/getRecs', methods=['GET'])
def getRecs():
    cur = conn.cursor()

    user_id = 2
    batch = 1

    query = f"SELECT * FROM user_product_recs WHERE user_id={user_id} AND batch={batch}"
    cur.execute(query)
    columns = [desc[0] for desc in cur.description]
    values = cur.fetchone()

    ctov = dict( (c, v) for c, v in zip(columns, values))

    i = 0
    top_p = []

    cname = f"top_products_{i}"
    while ctov.get(cname, 0):
        top_p.append(ctov[cname])
        i+=1
        cname = f"top_products_{i}"

    top_p = tuple(top_p)
    query = f"SELECT * FROM product_info WHERE product_id in {top_p};"

    cur.execute(query)
    columns = [desc[0] for desc in cur.description]
    values = cur.fetchall()

    data = []
    for value in values:
        ctov = dict( (c, v) for c, v in zip(columns, value))
        data.append(ctov)

    pid_to_ind = dict( zip( top_p, range(len(top_p))))
    data = sorted(data, key = lambda x: pid_to_ind[ x["product_id"]] )
    return jsonify(data)

@app.route('/getUsers', methods=['GET'])
def users():
    cur = conn.cursor()
    cur.execute("SELECT * FROM test_users;")
    print(cur.fetchone())
    return jsonify({'message': "hello"})


@app.route('/repeat', methods=['POST'])
def repeat():
    """Simple echo service."""
    message = request.get_json().get('message', '')
    return jsonify({'message': "nahhh"})

@app.route('/sendAction', methods=['POST'])
def sendAction():

    event = request.get_json().get('event', '')
    method = request.get_json().get('method', '')
    itemId = request.get_json().get('itemID', '')
    userId = request.get_json().get('userId', '')
    timestamp = request.get_json().get('timestamp', '')
    print(event + " " + method + " " + itemId + " " + userId + " " + timestamp)
    # do whatever you want with these 
    return jsonify({'event is': event})


# [START endpoints_auth_info_backend]
def auth_info():
    """Retrieves the authenication information from Google Cloud Endpoints."""
    encoded_info = request.headers.get('X-Endpoint-API-UserInfo', None)

    if encoded_info:
        info_json = _base64_decode(encoded_info)
        user_info = json.loads(info_json)
    else:
        user_info = {'id': 'anonymous'}

    return jsonify(user_info)
# [START endpoints_auth_info_backend]


@app.route('/auth/info/googlejwt', methods=['GET'])
def auth_info_google_jwt():
    """Auth info with Google signed JWT."""
    return auth_info()


@app.route('/auth/info/googleidtoken', methods=['GET'])
def auth_info_google_id_token():
    """Auth info with Google ID token."""
    return auth_info()


@app.route('/auth/info/firebase', methods=['GET'])
@cross_origin(send_wildcard=True)
def auth_info_firebase():
    """Auth info with Firebase auth."""
    return auth_info()


@app.errorhandler(http_client.INTERNAL_SERVER_ERROR)
def unexpected_error(e):
    """Handle exceptions by returning swagger-compliant json."""
    logging.exception('An error occured while processing the request.')
    response = jsonify({
        'code': http_client.INTERNAL_SERVER_ERROR,
        'message': 'Exception: {}'.format(e)})
    response.status_code = http_client.INTERNAL_SERVER_ERROR
    return response


if __name__ == '__main__':
    # This is used when running locally. Gunicorn is used to run the
    # application on Google App Engine. See entrypoint in app.yaml.
    app.run(host='0.0.0.0', port=5000, debug=True)
