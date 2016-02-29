#####################################################################################
#
#  Copyright (c) 2016 Eric Burger, Wallflower.cc
# 
#  MIT License (MIT)
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy 
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell 
#  copies of the Software, and to permit persons to whom the Software is 
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in
#  all copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE 
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, 
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE 
#  SOFTWARE.
#
#####################################################################################

'''
 In this example, we will create an object (test-object) and a stream 
 (test-stream) and populate it with random values.
'''
import requests
import json
import random
import time
import datetime

base = 'http://127.0.0.1:5000'
network_id = 'local'
header = {}


query = {
    'object-name': 'Test Object'
}
endpoint = '/networks/'+network_id+'/objects/test-object'
response = requests.request('PUT', base + endpoint, params=query, headers=header, timeout=120 )
resp = json.loads( response.text )
if resp['object-code'] == 201:
    print('Create object test-object: ok')
else:
    print('Create object test-object: error')
    print( response.text )
    
query = {
    'stream-name': 'Test Stream',
    'points-type': 'i' # 'i', 'f', or 's'
}
endpoint = '/networks/'+network_id+'/objects/test-object/streams/test-stream'
response = requests.request('PUT', base + endpoint, params=query, headers=header, timeout=120 )
resp = json.loads( response.text )
if resp['stream-code'] == 201:
    print('Create stream test-stream: ok')
else:
    print('Create stream test-stream: error')
    print( response.text )


print("Start sending random points (Ctrl+C to stop)")
endpoint = '/networks/local/objects/test-object/streams/test-stream/points'
while True:
    query = {
        'points-value': random.randint(0, 10),
        'points-at': datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    }
    response = requests.request('POST', base + endpoint, params=query, headers=header, timeout=120 )
    resp = json.loads( response.text )
    if resp['points-code'] == 200:
        print( 'Update test-stream points: ok')
    else:
        print( 'Update test-stream points: error')
        print( response.text )
    time.sleep(2)