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

"""
 The program below provides checks for the Wallflower.Pico API endpoints
 and common errors.
"""

import requests
import json

base = 'http://127.0.0.1:5000'
network_id = 'local'
header = {}

print('')
print("Basic Tests")
print('')


query = {
    'object-name': 'Test Object'
}
endpoint = '/networks/'+network_id+'/objects/test-object'
response = requests.request('PUT', base + endpoint, params=query, headers=header, timeout=120 )
resp = json.loads( response.text )
if resp['object-code'] == 201:
    print('Create test object: ok')
else:
    print('Create test object: error')
    print(response.text)

query = {
    'stream-name': 'Test Stream',
    'points-type': 'f' # 'i', 'f', or 's'
}
endpoint = '/networks/'+network_id+'/objects/test-object/streams/test-stream'
response = requests.request('PUT', base + endpoint, params=query, headers=header, timeout=120 )
resp = json.loads( response.text )
if resp['stream-code'] == 201:
    print('Create test stream: ok')
else:
    print('Create test stream: error')
    print(response.text)

query = {
    'object-name': 'Test Object Update'
}
endpoint = '/networks/'+network_id+'/objects/test-object'
response = requests.request('POST', base + endpoint, params=query, headers=header, timeout=120 )
resp = json.loads( response.text )
if resp['object-code'] == 200:
    print('Update test object: ok')
else:
    print('Update test object: error')
    print(response.text)

query = {
    'stream-name': 'Test Stream Update'
}
endpoint = '/networks/'+network_id+'/objects/test-object/streams/test-stream'
response = requests.request('POST', base + endpoint, params=query, headers=header, timeout=120 )
resp = json.loads( response.text )
if resp['stream-code'] == 200:
    print('Update test stream: ok')
else:
    print('Update test stream: error')
    print(response.text)

query = {
    'points-value': 1.1,
    'points-at': '2016-01-01T12:00:00.000Z'
}
endpoint = '/networks/'+network_id+'/objects/test-object/streams/test-stream/points'
response = requests.request('POST', base + endpoint, params=query, headers=header, timeout=120 )
query = {
    'points-value': 1.1,
    'points-at': '2016-01-02T12:00:00.000Z'
}
response = requests.request('POST', base + endpoint, params=query, headers=header, timeout=120 )
query = {
    'points-value': 1.1,
    'points-at': '2016-01-03T12:00:00.000Z'
}
response = requests.request('POST', base + endpoint, params=query, headers=header, timeout=120 )
query = {
    'points-value': 1.1,
    'points-at': '2016-01-04T12:00:00.000Z'
}
response = requests.request('POST', base + endpoint, params=query, headers=header, timeout=120 )
resp = json.loads( response.text )
if resp['points-code'] == 200:
    print('Update test stream points: ok')
else:
    print('Update test stream points: error')
    print(response.text)

endpoint = '/networks/'+network_id+''
response = requests.request('GET', base + endpoint, headers=header, timeout=120 )
resp = json.loads( response.text )
if resp['network-code'] == 200:
    print('Read network: ok')
else:
    print('Read network: error')
    print(response.text)

query = {}
endpoint = '/networks/'+network_id+'/objects/test-object'
response = requests.request('GET', base + endpoint, params=query, headers=header, timeout=120 )
resp = json.loads( response.text )
if resp['object-code'] == 200:
    print('Read test object: ok')
else:
    print('Read test object: error')
    print(response.text)

query = {}
endpoint = '/networks/'+network_id+'/objects/test-object/streams/test-stream'
response = requests.request('GET', base + endpoint, params=query, headers=header, timeout=120 )
resp = json.loads( response.text )
if resp['stream-code'] == 200:
    print('Read test stream: ok')
else:
    print('Read test stream: error')
    print(response.text)

query = {}
endpoint = '/networks/'+network_id+'/objects/test-object/streams/test-stream/points'
response = requests.request('GET', base + endpoint, params=query, headers=header, timeout=120 )
resp = json.loads( response.text )
if resp['points-code'] == 200:
    print('Read test stream points: ok')
else:
    print('Read test stream points: error')
    print(response.text)

query = {
    'points-limit': 2
   }
endpoint = '/networks/'+network_id+'/objects/test-object/streams/test-stream/points'
response = requests.request('GET', base + endpoint, params=query, headers=header, timeout=120 )
resp = json.loads( response.text )
if resp['points-code'] == 200 and len(resp['points']) == 2:
    print('Read test stream points limit 2: ok')
else:
    print('Read test stream points limit 2: error')
    print(response.text)

query = {
    'points-start': '2016-01-03T12:00:00.000Z'
   }
endpoint = '/networks/'+network_id+'/objects/test-object/streams/test-stream/points'
response = requests.request('GET', base + endpoint, params=query, headers=header, timeout=120 )
resp = json.loads( response.text )
if resp['points-code'] == 200 and len(resp['points']) == 2:
    print('Read test stream points since... : ok')
else:
    print('Read test stream points since... : error')
    print(response.text)

query = {
    'points-end': '2016-01-03T12:00:00.000Z'
   }
endpoint = '/networks/'+network_id+'/objects/test-object/streams/test-stream/points'
response = requests.request('GET', base + endpoint, params=query, headers=header, timeout=120 )
resp = json.loads( response.text )
if resp['points-code'] == 200 and len(resp['points']) == 3:
    print('Read test stream points till... : ok')
else:
    print('Read test stream points till... : error')
    print(response.text) 

query = {
    'points-start': '2016-01-02T12:00:00.000Z',
    'points-end': '2016-01-03T12:00:00.000Z'
   }
endpoint = '/networks/'+network_id+'/objects/test-object/streams/test-stream/points'
response = requests.request('GET', base + endpoint, params=query, headers=header, timeout=120 )
resp = json.loads( response.text )
if resp['points-code'] == 200 and len(resp['points']) == 2:
    print('Read test stream points from/till... : ok')
else:
    print('Read test stream points from/till... : error')
    print(response.text)

query = {}
endpoint = '/networks/'+network_id+'/objects/test-object/streams/test-stream'
response = requests.request('DELETE', base + endpoint, params=query, headers=header, timeout=120 )
resp = json.loads( response.text )
if resp['stream-code'] == 200:
    print('Delete test stream: ok')
else:
    print('Delete test stream: error')
    print(response.text) 
    
query = {}
endpoint = '/networks/'+network_id+'/objects/test-object'
response = requests.request('DELETE', base + endpoint, params=query, headers=header, timeout=120 )
resp = json.loads( response.text )
if resp['object-code'] == 200:
    print('Delete test object: ok')
else:
    print('Delete test object: error')
    print(response.text)
    
    


print('')
print("Error Tests")
print('')

query = {
    'object-name': 'Test Object'
}
endpoint = '/networks/'+network_id+'/objects/test!object'
response = requests.request('PUT', base + endpoint, params=query, headers=header, timeout=120 )
resp = json.loads( response.text )
if resp['object-code'] == 400:
    print('Create object with invalid id: ok')
else:
    print('Create object with invalid id: error')
    print(response.text)
    
query = {
    'stream-name': 'Test Stream',
    'points-type': 'f' # 'i', 'f', or 's'
}
endpoint = '/networks/'+network_id+'/objects/test-object/streams/test$stream'
response = requests.request('PUT', base + endpoint, params=query, headers=header, timeout=120 )
resp = json.loads( response.text )
if resp['stream-code'] == 400:
    print('Create stream with invalid id: ok')
else:
    print('Create stream with invalid id: error')
    print(response.text)

query = {
    'stream-name': 'Test Stream',
    'points-type': 'f' # 'i', 'f', or 's'
}
endpoint = '/networks/'+network_id+'/objects/test-object/streams/test-stream'
response = requests.request('PUT', base + endpoint, params=query, headers=header, timeout=120 )
resp = json.loads( response.text )
if resp['stream-code'] == 400 and resp['object-code'] == 404:
    print('Create stream for nonexistent object: ok')
else:
    print('Create stream for nonexistent object: error')
    print(response.text)
    
query = {
    'object-name': 'Test Object'
}
endpoint = '/networks/'+network_id+'/objects/test-object'
response = requests.request('POST', base + endpoint, params=query, headers=header, timeout=120 )
resp = json.loads( response.text )
if resp['object-code'] == 404:
    print('Update nonexistent object: ok')
else:
    print('Update nonexistent object: error')
    print(response.text)
    
query = {}
endpoint = '/networks/'+network_id+'/objects/test-object'
response = requests.request('GET', base + endpoint, params=query, headers=header, timeout=120 )
resp = json.loads( response.text )
if resp['object-code'] == 404:
    print('Read nonexistent object: ok')
else:
    print('Read nonexistent object: error')
    print(response.text)
    
query = {}
endpoint = '/networks/'+network_id+'/objects/test-object'
response = requests.request('DELETE', base + endpoint, params=query, headers=header, timeout=120 )
resp = json.loads( response.text )
if resp['object-code'] == 404:
    print('Delete nonexistent object: ok')
else:
    print('Delete nonexistent object: error')
    print(response.text)
    
query = {
    'object-name': 'Test Object'
}
endpoint = '/networks/'+network_id+'/objects/test-object'
response = requests.request('PUT', base + endpoint, params=query, headers=header, timeout=120 )
resp = json.loads( response.text )
if resp['object-code'] == 201:
    print('Create test object: ok')
else:
    print('Create test object: error')
    print(response.text)

query = {
    'stream-name': 'Test Stream'
}
endpoint = '/networks/'+network_id+'/objects/test-object/streams/test-stream'
response = requests.request('POST', base + endpoint, params=query, headers=header, timeout=120 )
resp = json.loads( response.text )
if resp['stream-code'] == 404:
    print('Update nonexistent stream: ok')
else:
    print('Update nonexistent stream: error')
    print(response.text)

query = {}
endpoint = '/networks/'+network_id+'/objects/test-object/streams/test-stream'
response = requests.request('GET', base + endpoint, params=query, headers=header, timeout=120 )
resp = json.loads( response.text )
if resp['stream-code'] == 404:
    print('Read nonexistent stream: ok')
else:
    print('Read nonexistent stream: error')
    print(response.text)  

query = {}
endpoint = '/networks/'+network_id+'/objects/test-object/streams/test-stream'
response = requests.request('DELETE', base + endpoint, params=query, headers=header, timeout=120 )
resp = json.loads( response.text )
if resp['stream-code'] == 404:
    print('Delete nonexistent stream: ok')
else:
    print('Delete nonexistent stream: error')
    print(response.text)

query = {
    'stream-name': 'Test Stream',
    'points-type': 'f' # 'i', 'f', or 's'
}
endpoint = '/networks/'+network_id+'/objects/test-object/streams/test-stream'
response = requests.request('PUT', base + endpoint, params=query, headers=header, timeout=120 )
resp = json.loads( response.text )
if resp['stream-code'] == 201:
    print('Create test stream: ok')
else:
    print('Create test stream: error')
    print(response.text)

query = {
    'points-value': 'invalid'
}
endpoint = '/networks/'+network_id+'/objects/test-object/streams/test-stream/points'
response = requests.request('POST', base + endpoint, params=query, headers=header, timeout=120 )
resp = json.loads( response.text )
if resp['points-code'] == 406: 
    print('Update test stream with invalid points value: ok')
else:
    print('Update test stream with invalid points value: error')
    print(response.text)

query = {
    'points-value': 1.1,
    'points-at': 'invalid'
}
endpoint = '/networks/'+network_id+'/objects/test-object/streams/test-stream/points'
response = requests.request('POST', base + endpoint, params=query, headers=header, timeout=120 )
resp = json.loads( response.text )
if resp['points-code'] == 400:
    print('Update test stream with invalid points at: ok')
else:
    print('Update test stream with invalid points at: error')
    print(response.text)    

query = {}
endpoint = '/networks/'+network_id+'/objects/test-object'
response = requests.request('DELETE', base + endpoint, params=query, headers=header, timeout=120 )
resp = json.loads( response.text )
if resp['object-code'] == 200:
    print('Delete test object: ok')
else:
    print('Delete test object: error')
    print(response.text)
