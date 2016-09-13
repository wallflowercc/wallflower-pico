#####################################################################################
#
#  Copyright (c) 2016 Eric Burger, Wallflower.cc
#
#  GNU Affero General Public License Version 3 (AGPLv3)
#
#  Should you enter into a separate license agreement after having received a copy of
#  this software, then the terms of such license agreement replace the terms below at
#  the time at which such license agreement becomes effective.
#
#  In case a separate license agreement ends, and such agreement ends without being
#  replaced by another separate license agreement, the license terms below apply
#  from the time at which said agreement ends.
#
#  LICENSE TERMS
#
#  This program is free software: you can redistribute it and/or modify it under the
#  terms of the GNU Affero General Public License, version 3, as published by the
#  Free Software Foundation. This program is distributed in the hope that it will be
#  useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
#  See the GNU Affero General Public License Version 3 for more details.
#
#  You should have received a copy of the GNU Affero General Public license along
#  with this program. If not, see <http://www.gnu.org/licenses/agpl-3.0.en.html>.
#
#####################################################################################

__version__ = '0.0.1'

import sys
import json

from flask import Flask, request, jsonify, make_response, send_from_directory, render_template, g
from wallflower_pico_db import WallflowerDB

#import re
import datetime
import sqlite3

# Load config
config = {
    'network-id': 'local',
    'enable_ws': False,
    'http_port': 5000,
    'ws_port': 5050,
    'database': 'wallflower_db'
}

try:
    with open('wallflower_config.json', 'rb') as f:
        wallflower_config = json.load(f)
        config.update( wallflower_config )
except:
    print( "Invalid wallflower_config.json file" )
    
# Add WebSocket port
if config['enable_ws']:
    from twisted.internet import reactor
    from twisted.python import log
    from twisted.web.server import Site
    
    from twisted.web.wsgi import WSGIResource
    
    from autobahn.twisted.websocket import WebSocketServerFactory, \
        WebSocketServerProtocol, \
        listenWS
    
    from autobahn.twisted.resource import WebSocketResource, WSGIRootResource


app = Flask(__name__)

@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()


# Routes
# Route index/dashboard html file
@app.route('/', methods=['GET'])
def root():
    # Return WebSocket or non-WebSocket Interface
    data = {}
    data['enable_ws'] = False
    if config['enable_ws']:
        data['enable_ws'] = True
    return render_template('pico/index.html', data=data)

# Route static font files
@app.route('/fonts/<path:filename>')
def send_font_file(filename):
    filename = 'fonts/'+filename
    response = make_response(send_from_directory('static', filename))
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response
    
# Route static files
@app.route('/<path:filename>')
def send_file(filename):
    return send_from_directory('static', filename)

# Route Network Requests
@app.route('/networks/<network_id>', methods=['GET'])
def networks(network_id):
    pico_db = WallflowerDB()
    pico_db.database = config['database']

    # Check network id
    if network_id != config['network-id']:
        response = {
            'network-error': "The Wallflower-Pico server only allows for one network with the id "+config['network-id'],
            'network-code': 400
        }
        return jsonify(**response)
        
    at = datetime.datetime.utcnow().isoformat() + 'Z'
    
    response = {
        'network-id': config['network-id']
    }
    
    if request.method == 'GET':
        # Read Network Details
        network_request = {
            'network-id': config['network-id']
        }
        
        pico_db.do(network_request,'read','network',(config['network-id'],),at)
        response.update( pico_db.db_message )
        
        return jsonify(**response)

# Route Object Requests
@app.route('/networks/'+config['network-id']+'/objects/<object_id>', methods=['GET','PUT','POST','DELETE'])
def objects(object_id):
    pico_db = WallflowerDB()
    pico_db.database = config['database']
    
    at = datetime.datetime.utcnow().isoformat() + 'Z'

    object_request = {
        'object-id': object_id
    }
    
    response = {
        'network-id': config['network-id'],
        'object-id': object_id
    }
    
    if request.method == 'GET': # Read
        # Read Object Details
        pico_db.do(object_request,'read','object',(config['network-id'],object_id),at)
        response.update( pico_db.db_message )
        
    elif request.method == 'PUT': # Create
        # Create Object
        object_request['object-details'] = {
            'object-name': object_id
        }
        object_name = request.args.get('object-name',None,type=str)
        if object_name is not None:
            object_request['object-details']['object-name'] = object_name

        pico_db.do(object_request,'create','object',(config['network-id'],object_id),at)
        response.update( pico_db.db_message )
        
        # Broadcast response over websocket
        if config['enable_ws'] and response['object-code'] == 201:
            response['response-type'] = 'object-create'
            factory.broadcast( json.dumps(response) )
        
    elif request.method == 'POST': 
        # Update Object Details
        object_request['object-details'] = {
            'object-name': object_id
        }
        object_name = request.args.get('object-name',None,type=str)
        if object_name is not None:
            object_request['object-details']['object-name'] = object_name
            
        pico_db.do(object_request,'update','object',(config['network-id'],object_id),at)
        response.update( pico_db.db_message )
        
        # Broadcast response over websocket
        if config['enable_ws'] and response['object-code'] == 200:
            response['response-type'] = 'object-update'
            factory.broadcast( json.dumps(response) )
        
    elif request.method == 'DELETE': 
        # Delete Object
        pico_db.do(object_request,'delete','object',(config['network-id'],object_id),at)
        response.update( pico_db.db_message )
        
        if config['enable_ws'] and response['object-code'] == 200:
            response['response-type'] = 'object-delete'
            factory.broadcast( json.dumps(response) )
        
    return jsonify(**response)


# Route Object Requests
@app.route('/networks/'+config['network-id']+'/objects/<object_id>/streams/<stream_id>', methods=['GET','PUT','POST','DELETE'])
def streams(object_id,stream_id):
    pico_db = WallflowerDB()
    pico_db.database = config['database']
    
    at = datetime.datetime.utcnow().isoformat() + 'Z'
    
    stream_request = {
        'stream-id': stream_id
    }
    
    response = {
        'network-id': config['network-id'],
        'object-id': object_id,
        'stream-id': stream_id
    }
    
    if request.method == 'GET': # Read
        # Read Object Details
        pico_db.do(stream_request,'read','stream',(config['network-id'],object_id,stream_id),at)
        response.update( pico_db.db_message )
        
    elif request.method == 'PUT': # Create
        # Create Stream
        stream_request['stream-details'] = {
            'stream-name': object_id,
            'stream-type': 'data'
        }
        stream_request['points-details'] = {
            'points-type': 'i',
            'points-length': 0
        }
        stream_name = request.args.get('stream-name',None,type=str)
        if stream_name is not None:
            stream_request['stream-details']['stream-name'] = stream_name
            
        points_type = request.args.get('points-type',None,type=str)
        if stream_name is not None and points_type in ['i','f','s']:
            stream_request['points-details']['points-type'] = points_type
        
        pico_db.do(stream_request,'create','stream',(config['network-id'],object_id,stream_id),at)
        response.update( pico_db.db_message )
        
        if config['enable_ws'] and response['stream-code'] == 201:
            response['response-type'] = 'stream-create'
            factory.broadcast( json.dumps(response) )
        
    elif request.method == 'POST': 
        # Update Object Details
        stream_request['stream-details'] = {
            'object-name': object_id
        }
        stream_name = request.args.get('stream-name',None,type=str)
        if stream_name is not None:
            stream_request['stream-details']['stream-name'] = stream_name

        pico_db.do(stream_request,'update','stream',(config['network-id'],object_id,stream_id),at)
        response.update( pico_db.db_message )
        
        if config['enable_ws'] and response['stream-code'] == 200:
            response['response-type'] = 'stream-update'
            factory.broadcast( json.dumps(response) )
        
    elif request.method == 'DELETE': 
        # Delete Object
        pico_db.do(stream_request,'delete','stream',(config['network-id'],object_id,stream_id),at)
        response.update( pico_db.db_message )

        if config['enable_ws'] and response['stream-code'] == 200:
            response['response-type'] = 'stream-delete'
            factory.broadcast( json.dumps(response) )
        
    return jsonify(**response)
    



# Route Stream Requests
@app.route('/networks/'+config['network-id']+'/objects/<object_id>/streams/<stream_id>/points', methods=['GET','POST'])
def points(object_id,stream_id):
    pico_db = WallflowerDB()
    pico_db.database = config['database']
    
    at = datetime.datetime.utcnow().isoformat() + 'Z'
    
    points_request = {
        'stream-id': stream_id,
        'points': []
    }

    response = {
        'network-id': config['network-id'],
        'object-id': object_id,
        'stream-id': stream_id
    }
    
    if request.method == 'GET':
        # Read Points (Use Search Instead Of Read)
        # Max number of data points (Optional)
        limit = request.args.get('points-limit',None,type=int)
        # Start date/time (Optional)
        start = request.args.get('points-start',None,type=str)
        # End date/time (Optional)
        end = request.args.get('points-end',None,type=str)
        
        # Points Search Input
        point_search = {}
        if limit is not None and isinstance(limit,int):
            point_search['limit'] = limit
        if start is not None and isinstance(start,str):
            point_search['start'] = start
        if end is not None and isinstance(end,str):
            point_search['end'] = end
        
        points_request['points'] = point_search
        
        pico_db.do(points_request,'search','points',(config['network-id'],object_id,stream_id),at)
        response.update( pico_db.db_message )
        
    elif request.method == 'POST':
        # Update Points
        # Point value (Required)
        point_value = request.args.get('points-value',None)
        if point_value is None:
            response['points-code'] = 406
            response['points-message'] = 'No value received'
            return jsonify(**response)
            
        # At date/time (Optional)
        point_at = request.args.get('points-at',at,type=str)
        try:
            datetime.datetime.strptime(point_at, "%Y-%m-%dT%H:%M:%S.%fZ")
        except:
            response['points-code'] = 400
            response['points-message'] = 'Invalid timestamp'
            return jsonify(**response)
        
        points = [{
            'value': point_value,
            'at': point_at
        }]
        
        points_request['points'] = points
        
        pico_db.do(points_request,'update','points',(config['network-id'],object_id,stream_id),at)
        response.update( pico_db.db_message )
        
        if config['enable_ws'] and response['points-code'] == 200:
            response['response-type'] = 'points-update'
            factory.broadcast( json.dumps(response) )
        
    return jsonify(**response)


@app.errorhandler(500)
def internal_error(error):
    return jsonify(**{'server-message':'An unknown internal error occured','server-code':500})

@app.errorhandler(404)
def not_found(error):
    return jsonify(**{'server-message':'Not a valid endpoint','server-code':404})



if config['enable_ws']:
    # Protocol for websocket broadcast
    class BroadcastServerProtocol(WebSocketServerProtocol):
    
        def onOpen(self):
            self.factory.register(self)
    
        def onMessage(self, payload, isBinary):
            # Ignore all incoming messages
            pass
    
        def connectionLost(self, reason):
            WebSocketServerProtocol.connectionLost(self, reason)
            self.factory.unregister(self)
    
    # Twisted factory for web socket broadcast
    class BroadcastServerFactory(WebSocketServerFactory):
    
        """
        Simple broadcast server broadcasting any message it receives to all
        currently connected clients.
        """
    
        def __init__(self, url):
            WebSocketServerFactory.__init__(self, url)
            self.clients = []
    
        def register(self, client):
            if client not in self.clients:
                print("Registered client {}".format(client.peer))
                self.clients.append(client)
    
        def unregister(self, client):
            if client in self.clients:
                print("Unregistered client {}".format(client.peer))
                self.clients.remove(client)
    
        def broadcast(self, msg):
            print("Broadcasting message '{}' ..".format(msg))
            for c in self.clients:
                c.sendMessage(msg.encode('utf8'))
                print("Message sent to {}".format(c.peer))



if __name__ == '__main__':
    # Check if the network exists and create, if necessary
    at = datetime.datetime.utcnow().isoformat() + 'Z'
    
    with app.app_context():
        pico_db = WallflowerDB()
        pico_db.database = config['database']
        
        # Create wcc_networks table, if necessary
        pico_db.execute( 'CREATE TABLE IF NOT EXISTS wcc_networks (timestamp date, network_id text, network_record text)' )
        
        # Check if default network exists
        exists = pico_db.loadNetworkRecord(config['network-id'])
        if not exists:
            # Create the default network
            network_request = {
                'network-id': config['network-id'],
                'network-details': {
                    'network-name': 'Local Wallflower.cc Network'
                }
            }
            pico_db.do(network_request,'create','network',(config['network-id'],),at)
    
    # Add WebSocket
    if config['enable_ws']:
        # Setup the Twisted server with Flask app
        log.startLogging(sys.stdout)
        
        # Set factory and begin listening to ws
        factory = BroadcastServerFactory(u"ws://0.0.0.0:"+str(config["ws_port"])+"/network/local")
        factory.protocol = BroadcastServerProtocol
        listenWS(factory)
        wsResource = WebSocketResource(factory)
        
        # Create a Twisted WSGI resource for Flask app
        wsgiResource = WSGIResource(reactor, reactor.getThreadPool(), app)
        
        # Create a root resource serving everything via WSGI/Flask, but
        # the path "/ws" served by our WebSocket
        rootResource = WSGIRootResource(wsgiResource, {'ws': wsResource})
        
        # create a Twisted Web Site and run everything
        site = Site(rootResource)
        reactor.listenTCP(config["http_port"], site)

        # Start the Twisted reactor 
        # TODO: Allow for shutdown without ending/restarting python instance
        reactor.run()
    else:
        # Start the Flask app
        app.run(host='0.0.0.0',port=config["http_port"])