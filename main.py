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
from src.event_upload import upload_event

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
                        host='localhost', port='5431', dbname=DBNAME)

@app.route('/testPost', methods=['POST'])
def test_post():
    query = query = """
        INSERT INTO user_events
                (user_id, product_id, event_timestamp, event, method)
                    VALUES
                            ( 20, 20, 30, 'test_post', 'jupyter' );
    """
    with conn.cursor() as cur:
        cur.execute(query)
    return jsonify({"wow":True})

@app.route('/testQuery', methods=['GET'])
def test_query():
    data = get_batch(conn, 1, request.args)
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

@app.route('/sendEvent', methods=['POST'])
def sendAction():
    data = request.get_json(force=True)
    print(data)
    res = upload_event(conn, data)
    return jsonify({'event is': res})


if __name__ == '__main__':
    # This is used when running locally. Gunicorn is used to run the
    # application on Google App Engine. See entrypoint in app.yaml.
    app.run(host='0.0.0.0', port=5000, debug=True)
