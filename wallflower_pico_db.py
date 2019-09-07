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

import sqlite3
import json
import sys
import datetime
import copy
import re

from base.wallflower_packet import WallflowerPacket
from flask import g

# For Python 2.* and 3.* support.
try:
  basestring
except NameError:
  basestring = str

class WallflowerDB:
    
    debug = True
    # Internal db messages
    db_message = None
    
    # Response(s)
    # For read, response contains requested data
    # For create, update, delete, response contains the
    # validated request (which has been executed by the db)
    response = None
    networks = {}        

    # Functions for connecting to SQLite database with Flask
    def connect_to_database(self):
        return sqlite3.connect( self.database+'.sqlite' )
        
    def execute(self, query, query_params={}):
        try:
            db = getattr(g, '_database', None)
            if db is None:
                db = g._database = self.connect_to_database()
            db = self.connect_to_database()
            cursor = db.cursor()
            cursor.execute( query, query_params )
            db.commit()
            cursor.close()
            #db.close()
            return True
                
        except sqlite3.OperationalError as err:
            self.debug( err )
            db.rollback()
            cursor.close()
            #db.close()
            return False
            
        except:
            self.debug( "Unexpected error (execute):"+str(sys.exc_info()) )
            return False
            
    def query(self, query, query_params={}):
        try:
            db = getattr(g, '_database', None)
            if db is None:
                db = g._database = self.connect_to_database()
            db = self.connect_to_database()
            cursor = db.cursor()
            cursor.execute( query, query_params )
            content = cursor.fetchall()
            cursor.close()
            #db.close()
            return content, True
                
        except sqlite3.OperationalError as err:
            self.debug( err )
            db.rollback()
            cursor.close()
            #db.close()
            return None, False
            
        except:
            self.debug( "Unexpected error (query):"+str(sys.exc_info()) )
            return None, False
            
    '''
    Print Messages
    '''
    def debug(self,text):
        if self.debug:
            print( text )
    
    
    def getCombinedResponse(self, request_packet ):
        def merge(a, b, path=None):
            "merges b into a"
            if path is None: path = []
            for key in b:
                if key in a:
                    if isinstance(a[key], dict) and isinstance(b[key], dict):
                        merge(a[key], b[key], path + [str(key)])
                    elif a[key] == b[key]:
                        pass # same leaf value
                    else:
                        raise Exception('Conflict at %s' % '.'.join(path + [str(key)]))
                else:
                    a[key] = b[key]
            return a
        return merge( self.db_message, request_packet.message_packet )
    
    '''
    Execute Network, Object, Stream, or Points Request
    '''
    def do(self,request,request_type,request_level,ids,at=None):
        if at is None:
            at = datetime.datetime.utcnow().isoformat() + 'Z'
        
        self.completed_request_tuple = ()
        self.db_message = {}
        
        request_packet = WallflowerPacket()
        request_packet.loadRequest(request,request_type,request_level)
        
        # Check if packet has request
        has_request, the_request = request_packet.hasRequest(request_level)
        self.debug('Has '+request_level+" "+request_type+" request: "+str(has_request))
        if not has_request:
            self.db_message.update({
                request_level+'-error': 'Invalid request',
                request_level+'-code': 400
            })
            self.db_message.update( request_packet.schema_packet  )
            self.debug( "Invalid request or schema error" )
            return self.db_message
        
        # Check if necessary elements/parents do or do not exist
        do_continue = self.doChecks(request_type,request_level,ids)
        if not do_continue:
            if request_level+'-code' not in self.db_message:
                self.db_message.update({
                    request_level+'-error': request_level.title()+' request could not be completed',
                    request_level+'-code': 400
                })
            return self.db_message
        
        # Finally, do request
        done = self.doRequest(the_request,request_type,request_level,ids,at)
        if not done:
            if request_level+'-code' not in self.db_message:
                self.db_message.update({
                    request_level+'-error': request_level.title()+' request could not be completed',
                    request_level+'-code': 400
                })
            #return self.db_message
        return self.db_message
        
    
    """
    Check if necessary elements do or do not exist
    """
    def doChecks(self, request_type, request_level, ids ):
        the_id = '.'.join(ids)
         
        if request_level == 'network':
            network_id = ids[0]
            # Try loading the network
            network_exists = self.loadNetworkRecord(network_id)
            if network_exists and request_type in ['create']:
                # Already exists
                self.db_message[request_level+'-message'] =\
                    request_level.title()+' '+the_id+' already exists. No changes made.'
                self.db_message[request_level+'-code'] = 304
                self.debug( self.db_message[request_level+'-message'] )
                return False
            elif not network_exists and request_type in ['read','update','delete','search']:
                # Does not exist.
                self.db_message[request_level+'-error'] =\
                    request_level.title()+' '+the_id+' does not exist and '+\
                    request_type+' request cannot be completed.'
                self.db_message[request_level+'-code'] = 404
                self.debug( self.db_message[request_level+'-error'] )
                return False
                
        elif request_level == 'object':
            network_id,object_id = ids
            # Try loading the network
            network_exists = self.loadNetworkRecord(network_id)
            if not network_exists:
                # Does not exist.
                self.db_message['network-error'] =\
                    'Network '+network_id+' does not exist and '+\
                    request_level+' '+request_type+' request cannot be completed.'
                self.db_message['network-code'] = 404
                self.debug( self.db_message['network-error'] )
                return False
            
            # Check for the object
            object_exists = self.objectExists(ids)
            if object_exists and request_type in ['create']:
                # Already exists
                self.db_message[request_level+'-message'] =\
                    request_level.title()+' '+the_id+' already exists. No changes made.'
                self.db_message[request_level+'-code'] = 304
                self.debug( self.db_message[request_level+'-message'] )
                return False
            elif not object_exists and request_type in ['read','update','delete','search']:
                # Does not exist.
                self.db_message[request_level+'-error'] =\
                    request_level.title()+' '+the_id+' does not exist and '+\
                    request_type+' request cannot be completed.'
                self.db_message[request_level+'-code'] = 404
                self.debug( self.db_message[request_level+'-error'] )
                return False
            
        elif request_level == 'stream' or request_level == 'points':
            network_id,object_id,stream_id = ids
            # Try loading the network
            network_exists = self.loadNetworkRecord(network_id)
            if not network_exists:
                # Does not exist.
                self.db_message['network-error'] =\
                    'Network '+network_id+' does not exist and '+\
                    request_level+' '+request_type+' request cannot be completed.'
                self.db_message['network-code'] = 404
                self.debug( self.db_message['network-error'] )
                return False
            
            # Check for the object
            object_exists = self.objectExists((network_id,object_id))
            if not object_exists:
                # Does not exist.
                self.db_message['object-error'] =\
                    'Object '+network_id+'.'+object_id+' does not exist and '+\
                    request_level+' '+request_type+' request cannot be completed.'
                self.db_message['object-code'] = 404
                self.debug( self.db_message['object-error'] )
                return False
            
            # Check for the stream
            stream_exists = self.streamExists(ids)
            if stream_exists and request_type in ['create']:
                # Already exists
                self.db_message[request_level+'-message'] =\
                    request_level.title()+' '+the_id+' already exists. No changes made.'
                self.db_message[request_level+'-code'] = 304
                self.debug( self.db_message[request_level+'-message'] )
                return False
            elif not stream_exists and request_type in ['read','update','delete','search']:
                # Does not exist.
                self.db_message[request_level+'-error'] =\
                    request_level.title()+' '+the_id+' does not exist and '+\
                    request_type+' request cannot be completed.'
                self.db_message[request_level+'-code'] = 404
                self.debug( self.db_message[request_level+'-error'] )
                return False
        
        return True
        

    '''
    Route requests
    '''
    def doRequest(self,request,request_type,request_level,ids,at):
        if request_level == 'network':
            if request_type == 'create':
                return self.createNetwork(ids,request,at)
            elif request_type == 'read':
                return self.readNetwork(ids,request,at)
            elif request_type == 'update':
                return self.updateNetwork(ids,request,at)
            elif request_type == 'delete':
                return self.deleteNetwork(ids,request,at)
            elif request_type == 'search':
                return self.searchNetwork(ids,request,at)
        elif request_level == 'object':
            if request_type == 'create':
                return self.createObject(ids,request,at)
            elif request_type == 'read':
                return self.readObject(ids,request,at)
            elif request_type == 'update':
                return self.updateObject(ids,request,at)
            elif request_type == 'delete':
                return self.deleteObject(ids,request,at)
            elif request_type == 'search':
                return self.searchObject(ids,request,at)
        elif request_level == 'stream':
            if request_type == 'create':
                return self.createStream(ids,request,at)
            elif request_type == 'read':
                return self.readStream(ids,request,at)
            elif request_type == 'update':
                return self.updateStream(ids,request,at)
            elif request_type == 'delete':
                return self.deleteStream(ids,request,at)
            elif request_type == 'search':
                return self.searchStream(ids,request,at)
        elif request_level == 'points':
            if request_type == 'read':
                return self.readPoints(ids,request,at)
            elif request_type == 'update':
                return self.updatePoints(ids,request,at)
            elif request_type == 'search':
                return self.searchPoints(ids,request,at)
                
        return False
                
    '''
    Route [__]Exists requests    
    '''
    def checkExists(self,request_level,ids):
        if request_level == 'network':
            return self.networkExists(ids)
        elif request_level == 'object':
            return self.objectExists(ids)
        elif request_level == 'stream':
            return self.streamExists(ids)
        elif request_level == 'points':
            return self.streamExists(ids)
            


        
    '''
    Create network. 
    Network must not already exist. 
    Assumes network info well formatted.
    '''
    def createNetwork(self,ids,create_network_request,at):
        network_id = ids[0]
        created = False
        
        try:
            # Assert network does not exist
            assert (network_id not in self.networks or \
                'network-id' not in self.networks[network_id])
        except:
            return False
            
        try:                
            # Automatically include...
            create_network_request['network-details']['created-at'] = at
            create_network_request['objects'] = {}
            
            query = "INSERT INTO wcc_networks "
            query += "(timestamp, network_id, network_record) VALUES "
            query += "(:at, :network_id, :network_record)"
            query_params = {
                'at': str(at),
                'network_id': network_id,
                'network_record': json.dumps( create_network_request )
            }
            success = self.execute( query, query_params )
            
            if success:
                created = True
                self.debug( "Network "+network_id+" Created" )
                self.db_message['network-message'] =\
                    "Network "+network_id+" Created"
                self.db_message['network-code'] = 201
                self.db_message['network-details'] =\
                    create_network_request['network-details']
                
                '''
                # Successful. Store request.
                completed_request = {
                    'type': 'create',
                    'level': 'network',
                    'network-id': network_id,
                    'request':  json.loads( json.dumps( create_network_request ) )
                }
                self.completed_request_tuple += (completed_request,)
                '''
                
                # Network has been load. Record changes locally
                self.networks[network_id] = create_network_request
            else:
                self.db_message['network-error'] =\
                    "Network "+network_id+" Not Created"
                self.db_message['network-code'] = 400
                self.debug( "Error: Network "+network_id+" Not Created" )
        except:
            self.db_message['network-error'] =\
                "Network "+network_id+" Not Created"
            self.db_message['network-code'] = 400
            self.debug( "Error: Network "+network_id+" Not Created" )
            self.debug( "Unexpected error (0):"+str(sys.exc_info()) )
                        
        return created
        

            
    '''
    Create object. 
    Object must not already exist. Network must exist. 
    Assume object info well formatted.
    '''
    def createObject(self,ids,create_object_request,at):
        network_id,object_id = ids
        created = False
        
        try:
            # Assert object does not exist
            assert object_id not in self.networks[network_id]['objects']
        except:
            return False

        try:
            # Create object by updating database
            create_object_request['object-details']['created-at'] = at
            create_object_request['streams'] = {}
            self.networks[network_id]['objects'][object_id] = create_object_request
            
            # Form SQL Query
            query = "UPDATE wcc_networks SET timestamp=:at, "
            query += "network_record=:network_record "
            query += "WHERE network_id=:network_id"
            query_params = {
                'at': str(at),
                'network_id': network_id,
                'network_record': json.dumps( self.networks[network_id] )
            }
            success = self.execute( query, query_params )
            
            if success:
                created = True
                self.db_message['object-message'] =\
                    "Object "+network_id+"."+object_id+" Created"
                self.db_message['object-code'] = 201
                self.db_message['object-details'] = \
                    create_object_request['object-details']
                
                self.debug( "Object "+network_id+"."+object_id+" Created" )
                
                '''
                # Successful. Store request(s).
                completed_request = {
                    'type': 'create',
                    'level': 'object',
                    'network-id': network_id,
                    'object-id': object_id,
                    'request':  json.loads( json.dumps( create_object_request ) )
                }
                self.completed_request_tuple += (completed_request,)
                '''
                
            else:
                self.db_message['object-error'] =\
                    "Object "+network_id+"."+object_id+" Not Created"
                self.db_message['object-code'] = 400
                self.debug( "Error: Object "+network_id+"."+object_id+" Not Created" )
            
        except:
            self.db_message['object-error'] =\
                "Object "+network_id+"."+object_id+" Not Created"
            self.db_message['object-code'] = 400
            self.debug( "Error: Object "+network_id+"."+object_id+" Not Created" )
            self.debug( "Unexpected error (1):"+str(sys.exc_info()) )
        
        # Something went wrong
        if not created and object_id in self.networks[network_id]['objects']:
            del(self.networks[network_id]['objects'][object_id])
            
        return created
        
    '''
    Create stream and stream db. 
    Stream must not already exist. Network and Object must exist.
    Assume stream info well formated.
    '''
    def createStream(self,ids,create_stream_request,at):
        network_id,object_id,stream_id = ids
        
        created = False
        
        try:
            # Assert stream does not exist
            assert stream_id not in self.networks[network_id]['objects'][object_id]['streams']
            # Assert IDs do not contain prohibited characters
            assert len( re.findall( "[^a-zA-Z0-9\-\_]", network_id+object_id+stream_id ) ) == 0
        except:
            return False
        
        try:
            # Create Table
            # Note: To prevent SQL injection, ids
            # should have already been validated.
            table_name = network_id+'.'+object_id+'.'+stream_id
            
            query = "CREATE TABLE IF NOT EXISTS "
            query += "'"+table_name+"'"
            query += "(timestamp date"
            
            points_details = create_stream_request['points-details']
            python_type = WallflowerPacket().getPythonType( points_details['points-type']  )
            
            # Check the data type
            if 0 == points_details['points-length']:
                if python_type is basestring:
                    query += ', value text'
                elif python_type is int:
                    query += ', value integer'
                elif python_type is float:
                    query += ', value real'
                elif python_type is bool:
                    query += ', value integer'
            else:
                if python_type is basestring:
                    query += ', value text'
                elif python_type is int:
                    for i in range(points_details['points-length']):
                        query += ', value'+str(i)+' integer'
                elif python_type is float:
                    for i in range(points_details['points-length']):
                        query += ', value'+str(i)+' real'
                elif python_type is bool:
                    for i in range(points_details['points-length']):
                        query += ', value'+str(i)+' integer'
            query = query + ')'
            
            success = self.execute(query)
            
            if success:
            
                create_stream_request['stream-details']['created-at'] = at
                create_stream_request['points'] = []
                self.networks[network_id]['objects'][object_id]['streams'][stream_id] = create_stream_request
                
                # Form SQL Query
                query = "UPDATE wcc_networks SET timestamp=:at, "
                query += "network_record=:network_record "
                query += "WHERE network_id=:network_id"
                query_params = {
                    'at': str(at),
                    'network_id': network_id,
                    'network_record': json.dumps( self.networks[network_id] )
                }
                success = self.execute( query, query_params )
                
                if success:
                    created = True
                    
                    self.db_message['stream-message'] =\
                        "Stream "+network_id+"."+object_id+"."+stream_id+" Created"
                    self.db_message['stream-code'] = 201
                    self.db_message['stream-details'] =\
                        create_stream_request['stream-details']
                    self.db_message['points-details'] =\
                        create_stream_request['points-details']
                    
                    self.debug( "Stream "+network_id+"."+object_id+"."+stream_id+" Created" )
                    
                    '''
                    # Successful. Store request(s).
                    completed_request = {
                        'type': 'create',
                        'level': 'stream',
                        'network-id': network_id,
                        'object-id': object_id,
                        'stream-id': stream_id,
                        'request':  json.loads( json.dumps( create_stream_request ) )
                    }
                    self.completed_request_tuple += (completed_request,)
                    '''
                    
            if not created:
                self.db_message['stream-error'] =\
                    "Stream "+network_id+"."+object_id+"."+stream_id+" Not Created"
                self.db_message['stream-code'] = 400
                self.debug( "Error: Stream "+network_id+"."+object_id+"."+stream_id+" Not Created" )
    
        except:
            # There was an error.
            self.db_message['stream-error'] =\
                "Stream "+network_id+"."+object_id+"."+stream_id+" Not Created"
            self.db_message['stream-code'] = 400
            self.debug( "Unexpected error (2):"+str(sys.exc_info()) )
                
            
        # Something went wrong
        if not created and stream_id in self.networks[network_id]['objects'][object_id]['streams']:
            del(self.networks[network_id]['objects'][object_id]['streams'][stream_id])
            
        return created
                
    '''
    Read Network
    '''
    def readNetwork(self,ids,read_network_request,at):
        network_id = ids[0]
        
        try:
            # Assert network exists
            assert ('network-id' in self.networks[network_id])
            self.db_message = copy.deepcopy( self.networks[network_id] )
            self.db_message['network-message'] = "Network "+network_id+" Read"
            self.db_message['network-code'] = 200
            self.debug( "Network "+network_id+" Read" )
            return True
        except:
            self.db_message['network-error'] = "Network "+network_id+" Not Read"
            self.db_message['network-code'] = 400
            self.debug( "Error: Network "+network_id+" Not Read" )
            self.debug( "Unexpected error (3):"+str(sys.exc_info()) )
            
        return False

        
    '''
    Read Object
    '''
    def readObject(self,ids,read_object_request,at):
        network_id,object_id = ids
        try:
            # Assert object exists
            assert (object_id in self.networks[network_id]['objects'])
            self.db_message =\
                copy.deepcopy( self.networks[network_id]['objects'][object_id] )
            self.db_message['object-message'] =\
                "Object "+network_id+"."+object_id+" Read"
            self.db_message['object-code'] = 200
            self.debug( "Object "+network_id+"."+object_id+" Read" )
            return True
        except:
            self.db_message['object-error'] =\
                "Object "+network_id+"."+object_id+" Not Read"
            self.db_message['object-code'] = 400
            self.debug( "Error: Object "+network_id+"."+object_id+" Not Read" )
            self.debug( "Unexpected error (4):"+str(sys.exc_info()) )            
            
        return False
                
    '''
    Read stream.
    '''
    def readStream(self,ids,read_stream_request,at):
        network_id,object_id,stream_id = ids
        try:
            # Assert stream exists
            assert (stream_id in self.networks[network_id]['objects'][object_id]['streams'])
            self.db_message =\
                copy.deepcopy( self.networks[network_id]['objects'][object_id]['streams'][stream_id] )
            self.db_message['stream-message'] =\
                "Stream "+network_id+"."+object_id+"."+stream_id+" Read"
            self.db_message['stream-code'] = 200
            self.debug( "Stream "+network_id+"."+object_id+"."+stream_id+" Read" )
            return True
        except:
            self.db_message['stream-error'] =\
                "Stream "+network_id+"."+object_id+"."+stream_id+" Not Read"
            self.db_message['stream-code'] = 400
            self.debug( "Error: Stream "+network_id+"."+object_id+"."+stream_id+" Not Read" )
            self.debug( "Unexpected error (5):"+str(sys.exc_info()) )
            
        return False
                
    '''
    Read points from stream.
    '''
    def readPoints(self,ids,read_points_request,at):
        network_id,object_id,stream_id = ids
        
        try:
            # Assert points exist
            assert ('points' in self.networks[network_id]['objects'][object_id]['streams'][stream_id])
            self.db_message['points'] =\
                copy.deepcopy( self.networks[network_id]['objects'][object_id]['streams'][stream_id]['points'] )
            self.db_message['points-message'] =\
                "Points "+network_id+"."+object_id+"."+stream_id+".points Read"
            self.db_message['points-code'] = 200
            self.debug( "Points "+network_id+"."+object_id+"."+stream_id+".points Read" )
            return True
        except:
            self.db_message['points-error'] =\
                "Points "+network_id+"."+object_id+"."+stream_id+".points Not Read"
            self.db_message['points-code'] = 400
            self.debug( "Error: Points "+network_id+"."+object_id+"."+stream_id+".points Not Read" )
            self.debug( "Unexpected error (6):"+str(sys.exc_info()) )
            
        return False
        

        
    '''
    Update network. Assumes network info well formatted.
    '''
    def updateNetwork(self,ids,update_network_request,at):
        network_id = ids[0]
        updated = False
        
        try:
            # Assert network exists
            assert 'network-id' in self.networks[network_id]
        except:
            return False
            
        old_details = None

        try:
            # Update network
            old_details = copy.deepcopy( self.networks[network_id]['network-details'] )
            update_network_request['network-details']['updated-at'] = at
            for key in update_network_request['network-details']:
                self.networks[network_id]['network-details'][key] =\
                    update_network_request['network-details'][key]
            
            # Form SQL Query
            query = "UPDATE wcc_networks SET timestamp=:at, "
            query += "network_record=:network_record "
            query += "WHERE network_id=:network_id"
            query_params = {
                'at': str(at),
                'network_id': network_id,
                'network_record': json.dumps( self.networks[network_id] )
            }
            success = self.execute( query, query_params )
            
            if success:
                updated = True
                
                self.db_message['network-message'] = "Network "+network_id+" Updated"
                self.db_message['network-code'] = 200
                # Return only updated details
                self.db_message['network-details'] =\
                    update_network_request['network-details']
                self.debug( "Network "+network_id+" Updated" )
                
                '''
                # Successful. Store request.
                completed_request = {
                    'type': 'update',
                    'level': 'network',
                    'network-id': network_id,
                    'request':  json.loads( json.dumps( update_network_request ) )
                }
                self.completed_request_tuple += (completed_request,)
                '''
            else:
                self.db_message['network-error'] = "Network "+network_id+" Not Updated"
                self.db_message['network-code'] = 400
                self.debug( "Error: Network "+network_id+" Not Updated" )
            
        except:
            self.db_message['network-error'] = "Network "+network_id+" Not Updated"
            self.db_message['network-code'] = 400
            self.debug( "Error: Network "+network_id+" Not Updated" )
            self.debug( "Unexpected error (7):"+str(sys.exc_info()) )
        
        # Something went wrong
        if not updated and old_details is not None:
            self.networks[network_id]['network-details'] = old_details
            
        return updated

        
    '''
    Update object. Assumes object info well formatted.
    '''
    def updateObject(self,ids,update_object_request,at):
        network_id,object_id = ids
        updated = False

        try:
            # Assert object exists
            assert object_id in self.networks[network_id]['objects']
        except:
            return False
        
        old_details = None

        try:
            # Store old details
            the_object = self.networks[network_id]['objects'][object_id]
            old_details = copy.deepcopy( the_object['object-details'] )
        
            # Update object by updating database
            update_object_request['object-details']['updated-at'] = at
            for key in update_object_request['object-details']:
                the_object['object-details'][key] =\
                update_object_request['object-details'][key]
            
            # Form SQL Query
            query = "UPDATE wcc_networks SET timestamp=:at, "
            query += "network_record=:network_record "
            query += "WHERE network_id=:network_id"
            query_params = {
                'at': str(at),
                'network_id': network_id,
                'network_record': json.dumps( self.networks[network_id] )
            }
            success = self.execute( query, query_params )
            
            if success:
                updated = True
                
                self.db_message['object-message'] =\
                    "Object "+network_id+"."+object_id+" Updated"
                self.db_message['object-code'] = 200
                # Return only updated details
                self.db_message['object-details'] =\
                    update_object_request['object-details']
                self.debug( "Object "+network_id+"."+object_id+" Updated" )
                
                '''
                # Successful. Store request(s).
                completed_request = {
                    'type': 'update',
                    'level': 'object',
                    'network-id': network_id,
                    'object-id': object_id,
                    'request':  json.loads( json.dumps( update_object_request ) )
                }
                self.completed_request_tuple += (completed_request,)
                '''
            else:
                self.db_message['object-error'] =\
                    "Object "+network_id+"."+object_id+" Not Updated"
                self.db_message['object-code'] = 400
                self.debug( "Error: Object "+network_id+"."+object_id+" Not Updated" )
            
        except:
            self.db_message['object-error'] =\
                "Object "+network_id+"."+object_id+" Not Updated"
            self.db_message['object-code'] = 400
            self.debug( "Error: Object "+network_id+"."+object_id+" Not Updated" )
            self.debug( "Unexpected error (8):"+str(sys.exc_info()) )

        # Something went wrong
        if not updated and old_details is not None:
            self.networks[network_id]['objects'][object_id]['object-details'] = old_details
            
        return updated
        
    '''
    Update stream. Assumes points-details and points well formatted.
    '''
    def updateStream(self,ids,update_stream_request,at,check_if_exists=True):
        network_id,object_id,stream_id = ids
        
        updated = False
        
        try:
            # Assert stream exists
            assert stream_id in self.networks[network_id]['objects'][object_id]['streams']
        except:
            return False
            
        old_details = None
        

        try:
            # Store old details
            the_stream = self.networks[network_id]['objects'][object_id]['streams'][stream_id]
            old_details = copy.deepcopy( the_stream['stream-details'] )
        
            # Update object by updating database
            update_stream_request['stream-details']['updated-at'] = at
            for key in update_stream_request['stream-details']:
                the_stream['stream-details'][key] = update_stream_request['stream-details'][key]
            
            # Form SQL Query
            query = "UPDATE wcc_networks SET timestamp=:at, "
            query += "network_record=:network_record "
            query += "WHERE network_id=:network_id"
            query_params = {
                'at': str(at),
                'network_id': network_id,
                'network_record': json.dumps( self.networks[network_id] )
            }
            success = self.execute( query, query_params )
            
            if success:
                updated = True
                
                self.db_message['stream-message'] =\
                    "Stream "+network_id+"."+object_id+"."+stream_id+" Updated"
                self.db_message['stream-code'] = 200
                # Return only updated details
                self.db_message['stream-details'] =\
                    update_stream_request['stream-details']            
                self.debug( "Stream "+network_id+"."+object_id+"."+stream_id+" Updated" )
                
                '''
                # Successful. Store request(s).
                completed_request = {
                    'type': 'update',
                    'level': 'stream',
                    'network-id': network_id,
                    'object-id': object_id,
                    'stream-id': stream_id,
                    'request':  json.loads( json.dumps( update_stream_request ) )
                }
                self.completed_request_tuple += (completed_request,)
                '''
            else:
                self.db_message['stream-error'] =\
                    "Stream "+network_id+"."+object_id+"."+stream_id+" Not Updated"
                self.db_message['stream-code'] = 400
                self.debug( "Error: Stream "+network_id+"."+object_id+"."+stream_id+" Not Updated" )
            
        except:
            self.db_message['stream-error'] =\
                "Stream "+network_id+"."+object_id+"."+stream_id+" Not Updated"
            self.db_message['stream-code'] = 400
            self.debug( "Error: Stream "+network_id+"."+object_id+"."+stream_id+" Not Updated" )
            self.debug( "Unexpected error (9):"+str(sys.exc_info()) )

            
        # Something went wrong
        if not updated and old_details is not None:
            self.networks[network_id]['objects'][object_id]['streams'][stream_id]['stream-details'] = old_details
            
        return updated
        
    '''
    Update stream. Assumes points_details and points well formatted.
    '''
    def updatePoints(self,ids,update_points_request,at,check_if_exists=True):
        network_id,object_id,stream_id = ids
        
        updated = False
        
        try:
            # Assert stream exists
            assert stream_id in self.networks[network_id]['objects'][object_id]['streams']
            # Assert IDs do not contain prohibited characters
            assert len( re.findall( "[^a-zA-Z0-9\-\_]", network_id+object_id+stream_id ) ) == 0
        except:
            return False

        
        try:
            # The stream
            the_stream = self.networks[network_id]['objects'][object_id]['streams'][stream_id]
            the_stream['stream-details']['updated-at'] = at
            points_details = the_stream['points-details']
            python_type = WallflowerPacket().getPythonType( points_details['points-type']  )

            # The new points                
            the_points_update = update_points_request['points']
            
            new_points = []
            for point in the_points_update:                 
                # Update database table                
                point_at = at
                if 'at' in point:
                    point_at = point['at']
                payload = point['value']
                
                found_type = None
                try:
                    found_type = type(payload)
                    if points_details['points-length'] > 0:
                        assert isinstance(payload,(list,tuple))
                        for i in range(len(payload)):
                            found_type = type(payload[i])
                            try:
                                assert isinstance(payload[i],python_type)
                            except:
                                # TODO: Generature Warning
                                if python_type is basestring:
                                    payload[i] = basestring( payload[i] )
                                elif python_type == int:
                                    payload[i] = int( payload[i] )
                                elif python_type == float:
                                    payload[i] = float( payload[i] )
                                elif python_type == bool:
                                    payload[i] = bool( payload[i] )
                    else:
                        found_type = type(payload)
                        try:
                            assert isinstance(payload,python_type)
                        except:
                            # TODO: Generature Warning
                            if python_type is basestring:
                                payload = basestring( payload )
                            elif python_type == int:
                                payload = int( payload )
                            elif python_type == float:
                                payload = float( payload )
                            elif python_type == bool:
                                payload = bool( payload )
                except:
                    self.db_message['points-error'] =\
                        "Stream "+network_id+"."+object_id+"."+\
                        stream_id+" Point Value Not "+str(python_type)
                    self.db_message['points-code'] = 406
                    self.debug( "Stream "+network_id+"."+object_id+"."+stream_id+ \
                        " Point Value Should Be "+str(python_type)+\
                        ", Not "+str(found_type) )
                    return False
                
                new_points.append({'value':payload,'at':point_at})

                # Create Table
                # Note: To prevent SQL injection, ids
                # should have already been validated.
                table_name = network_id+'.'+object_id+'.'+stream_id
            
                query = "INSERT INTO '"+table_name+"' "
                query_params = {}
                
                # Check the data type
                if 0 == points_details['points-length']:
                    query += "(timestamp,value) VALUES (:at,:value)"
                    query_params['at'] = str(point_at)
                    if python_type is basestring:
                        query_params['value'] = str(payload)
                    elif python_type == int:
                        query_params['value'] = str(payload)
                    elif python_type == float:
                        query_params['value'] = str(payload)
                    elif python_type == bool:
                        query_params['value'] = str(int(payload))
                """
                TODO: Lists
                else:
                    if python_type == basestring:
                        query += "(timestamp,value) VALUES ('"+\
                        str(point_at)+"','"+payload+"')"
                    elif python_type == int:
                        ids = '(timestamp'
                        vals = "('"+str(point_at)+"'"
                        for i in range(points_details['points-length']):
                            ids += ',value'+str(i)
                            vals += ','+str(payload[i])
                        query += ids+') VALUES '+vals+')'
                    elif python_type == float:
                        ids = '(timestamp'
                        vals = "('"+str(point_at)+"'"
                        for i in range(points_details['points-length']):
                            ids += ',value'+str(i)
                            vals += ','+str(payload[i])
                        query += ids+') VALUES '+vals+')' 
                    elif python_type == bool:
                        ids = '(timestamp'
                        vals = "('"+str(point_at)+"'"
                        for i in range(points_details['points-length']):
                            ids += ',value'+str(i)
                            vals += ','+str(int(payload[i]))
                        query += ids+') VALUES '+vals+')'
                """
                success = self.execute( query, query_params )
            
                # TODO: Should use executemany, but HTTP API only supports one point updates
            
            if 'points' in the_stream:
                the_points = the_stream['points'] + new_points
                the_points = sorted(the_points, key=lambda k: k['at'],reverse=True)
                if len(the_points) > 5:
                    the_points = the_points[:5]
                the_stream['points'] = the_points
            else:
                the_points = sorted(new_points, key=lambda k: k['at'],reverse=True)
                if len(the_points) > 5:
                    the_points = the_points[:5]
                the_stream['points'] = the_points
            
            # Update min and max
            if len(new_points) > 0 and isinstance(new_points[0]['value'],(int,float)):
                min_val = new_points[0]['value']
                max_val = new_points[0]['value']
                if all(k in points_details for k in ("min-value","max-value")):
                    min_val = points_details['min-value']
                    max_val = points_details['max-value']
                    
                for i in range(len(new_points)):
                    if new_points[i]['value'] > max_val:
                        max_val = new_points[i]['value']
                    elif new_points[i]['value'] < min_val:
                        min_val = new_points[i]['value']
                
                points_details['min-value'] = min_val
                points_details['max-value'] = max_val
            
            # Form SQL Query
            query = "UPDATE wcc_networks SET timestamp=:at, "
            query += "network_record=:network_record "
            query += "WHERE network_id=:network_id"
            query_params = {
                'at': str(at),
                'network_id': network_id,
                'network_record': json.dumps( self.networks[network_id] )
            }
            success = self.execute( query, query_params )
            
            if success:
                updated = True
                
                self.db_message['points-message'] =\
                    "Stream "+network_id+"."+object_id+"."+stream_id+".points Updated"
                self.db_message['points-code'] = 200
                # Return only updated details
                self.db_message['points'] = new_points
                self.debug( "Stream "+network_id+"."+object_id+"."+stream_id+".points Updated" )
                
                # Successful. Store request(s).
                update_points_request['points'] = new_points
                
                '''
                completed_request = {
                    'type': 'update',
                    'level': 'points',
                    'network-id': network_id,
                    'object-id': object_id,
                    'stream-id': stream_id,
                    'request': json.loads( json.dumps( update_points_request ) )
                }
                self.completed_request_tuple += (completed_request,)
                '''
            else:
                self.db_message['points-error'] =\
                    "Points "+network_id+"."+object_id+"."+stream_id+".points Not Updated"
                self.db_message['points-code'] = 400
                self.debug( "Points "+network_id+"."+object_id+"."+stream_id+".points Not Updated" )
            
        except:
            self.db_message['points-error'] =\
                "Points "+network_id+"."+object_id+"."+stream_id+".points Not Updated"
            self.db_message['points-code'] = 400
            self.debug( "Points "+network_id+"."+object_id+"."+stream_id+".points Not Updated" )
            self.debug( "Unexpected error (10):"+str(sys.exc_info()) )
            
        return updated


    '''
    Delete network. 
    '''
    def deleteNetwork(self,ids,delete_network_request,at,update_message=True):
        network_id = ids[0]
        
        deleted = False
        
        try:
            # Assert network exists
            assert 'network-id' in self.networks[network_id]
        except:
            return False
        

        try:
            if 'objects' in self.networks[network_id]:
                for object_id in self.networks[network_id]['objects'].keys():
                    self.deleteObject((network_id,object_id),None,at,False)                             

            # Form SQL Query
            query = "DELETE FROM wcc_networks "
            query += "WHERE network_id=:network_id"
            query_params = {
                'network_id': network_id
            }
            success = self.execute( query, query_params )
            
            if success:
                deleted = True
                
                if update_message:
                    self.db_message['network-message'] =\
                        "Network "+network_id+" Deleted"
                    self.db_message['network-code'] = 200
                self.debug( "Network "+network_id+" Deleted" )
    
                # Delete local record
                self.networks[network_id] = {}
            else:
                if update_message:
                    self.db_message['network-error'] =\
                        "Network "+network_id+" Not Deleted"
                    self.db_message['network-code'] = 400
                self.debug( "Error: Network "+network_id+" Not Deleted" )
    
        except:
            # There was an error.
            if update_message:
                self.db_message['network-error'] =\
                    "Network "+network_id+" Not Deleted"
                self.db_message['network-code'] = 400
            self.debug( "Unexpected error (11):"+str(sys.exc_info()) )
            
            
        return deleted

    '''
    Delete object. 
    '''
    def deleteObject(self,ids,delete_object_request,at,update_message=True):
        network_id,object_id = ids
        
        deleted = False
        
        try:
            # Assert object exists
            assert object_id in self.networks[network_id]['objects']
        except:
            return False
        
        # Connect to database

        try:
            if 'streams' in self.networks[network_id]['objects'][object_id]:
                for stream_id in self.networks[network_id]['objects'][object_id]['streams'].keys():
                    self.deleteStream((network_id,object_id,stream_id),None,at,False)
            
            # If fails to update database, do nothing
            del(self.networks[network_id]['objects'][object_id])
            
            # Update database                
            query = "UPDATE wcc_networks SET timestamp=:at, "
            query += "network_record=:network_record "
            query += "WHERE network_id=:network_id"
            query_params = {
                'at': str(at),
                'network_id': network_id,
                'network_record': json.dumps( self.networks[network_id] )
            }
            success = self.execute( query, query_params )
            
            if success:
                deleted = True
                if update_message:
                    self.db_message['object-message'] =\
                        "Object "+network_id+"."+object_id+" Deleted"
                    self.db_message['object-code'] = 200
                self.debug( "Object "+network_id+"."+object_id+" Deleted" )
            
            else:
                if update_message:
                    self.db_message['object-error'] =\
                        "Object "+network_id+"."+object_id+" Not Deleted"
                    self.db_message['object-code'] = 400
                self.debug( "Error: Object "+network_id+"."+object_id+" Not Deleted" )
    
        except:
            # There was an error.
            if update_message:
                self.db_message['object-error'] =\
                    "Object "+network_id+"."+object_id+" Not Deleted"
                self.db_message['object-code'] = 400
            self.debug( "Unexpected error (12):"+str(sys.exc_info()) )
                

        # If fails to update database, do nothing
            
        return deleted
        
    '''
    Delete stream. 
    '''
    def deleteStream(self,ids,create_stream_request,at,update_message=True):
        network_id,object_id,stream_id = ids
        
        deleted = False
                
        try:
            # Assert stream exists
            assert stream_id in self.networks[network_id]['objects'][object_id]['streams']
            # Assert IDs do not contain prohibited characters
            assert len( re.findall( "[^a-zA-Z0-9\-\_]", network_id+object_id+stream_id ) ) == 0
        except:
            return False

        try:

            # Drop table
            table_name = network_id+'.'+object_id+'.'+stream_id      
            query = "DROP TABLE '"+table_name+"'"
            success = self.execute(query)
            
            if success:
                self.debug( "Stream "+network_id+"."+object_id+"."+stream_id+" DB Deleted" )
                
                # If fails to update database, do nothing
                del(self.networks[network_id]['objects'][object_id]['streams'][stream_id])
                
                # Update database                
                query = "UPDATE wcc_networks SET timestamp=:at, "
                query += "network_record=:network_record "
                query += "WHERE network_id=:network_id"
                query_params = {
                    'at': str(at),
                    'network_id': network_id,
                    'network_record': json.dumps( self.networks[network_id] )
                }
                success = self.execute( query, query_params )
                
                if success:
                    deleted = True
                    if update_message:
                        self.db_message['stream-message'] =\
                            "Stream "+network_id+"."+object_id+"."+stream_id+" Deleted"
                        self.db_message['stream-code'] = 200
                    self.debug( "Stream "+network_id+"."+object_id+"."+stream_id+" Deleted" )
                    
            if not deleted:
                if update_message:
                    self.db_message['stream-error'] =\
                        "Stream "+network_id+"."+object_id+"."+stream_id+" Not Deleted"
                    self.db_message['objects']['stream-code'] = 400
                self.debug( "Error: Stream "+network_id+"."+object_id+"."+stream_id+" Not Deleted" )
    
        except:
            # There was an error.
            if update_message:
                self.db_message['stream-error'] =\
                    "Stream "+network_id+"."+object_id+"."+stream_id+" Not Deleted"
                self.db_message['stream-code'] = 400
            self.debug( "Unexpected error (13):"+str(sys.exc_info()) )
            
        
        # If fails to update database, do nothing
        
        return deleted


    '''
    Search Network
    '''
    def searchNetwork(self,ids,search_network_request,at):
        network_id = ids[0]
        # TODO:
        try:
            # Assert network exists
            assert ('network-id' in self.networks[network_id])
            self.db_message = copy.deepcopy( self.networks[network_id] )
            self.db_message['network-message'] =\
                "Network "+network_id+" Searched"
            self.db_message['network-code'] = 200
            self.debug( "Network "+network_id+" Searched" )
            return True
        except:
            self.db_message['network-error'] =\
                "Network "+network_id+" Not Searched"
            self.db_message['network-code'] = 400
            self.debug( "Error: Network "+network_id+" Not Searched" )
            self.debug( "Unexpected error (14):"+str(sys.exc_info()) )
        return False

        
    '''
    Search Object
    '''
    def searchObject(self,ids,search_object_request,at):
        network_id,object_id = ids
        # TODO:
        try:
            # Assert object exists
            assert (object_id in self.networks[network_id]['objects'])
            self.db_message =\
                copy.deepcopy( self.networks[network_id]['objects'][object_id] )
            self.db_message['object-message'] =\
                "Object "+network_id+"."+object_id+" Searched"
            self.db_message['object-code'] = 200
            self.debug( "Object "+network_id+"."+object_id+" Searched" )
            return True
        except:
            self.db_message['object-error'] =\
                "Object "+network_id+"."+object_id+" Not Searched"
            self.db_message['object-code'] = 400
            self.debug( "Error: Object "+network_id+"."+object_id+" Not Searched" )
            self.debug( "Unexpected error (15):"+str(sys.exc_info()) )
        return False
                
    '''
    Search stream.
    '''
    def searchStream(self,ids,search_stream_request,at):
        network_id,object_id,stream_id = ids
        # TODO:
        try:
            # Assert stream exists
            assert (stream_id in self.networks[network_id]['objects'][object_id]['streams'])
            self.db_message =\
                copy.deepcopy( self.networks[network_id]['objects'][object_id]['streams'][stream_id] )
            self.db_message['stream-message'] =\
                "Stream "+network_id+"."+object_id+"."+stream_id+" Searched"
            self.db_message['stream-code'] = 200
            self.debug( "Stream "+network_id+"."+object_id+"."+stream_id+" Searched" )
            return True
        except:
            self.db_message['stream-error'] =\
                "Stream "+network_id+"."+object_id+"."+stream_id+" Not Searched"
            self.db_message['stream-code'] = 400
            self.debug( "Error: Stream "+network_id+"."+object_id+"."+stream_id+" Not Searched" )
            self.debug( "Unexpected error (16):"+str(sys.exc_info()) )
        return False
                
    '''
    Search points from stream.
    '''
    def searchPoints(self,ids,search_points_request,at):
        network_id,object_id,stream_id = ids
        points_details = {}
        search_points_details = {}
        try:
            # Assert points exist
            assert ('points' in self.networks[network_id]['objects'][object_id]['streams'][stream_id])
            points_details = self.networks[network_id]['objects'][object_id]['streams'][stream_id]['points-details']
            search_points_details = search_points_request['points']
            # Assert IDs do not contain prohibited characters
            assert len( re.findall( "[^a-zA-Z0-9\-\_]", network_id+object_id+stream_id ) ) == 0
        except:
            return False
            
        searched = False

        points = []

        try:
            
            # Get historic data
            # Note: To prevent SQL injection, ids
            # should have already been validated.
            table_name = network_id+'.'+object_id+'.'+stream_id
            
            query = 'SELECT timestamp'
            if 0 == points_details['points-length']:
                query += ', value'
            else:
                for i in range(points_details['points-length']):
                    query += ', value'+str(i)
            
            query += " FROM '"+table_name+"'"
            query_params = {}
            
            if 'start' in search_points_details and 'end' in search_points_details:
                query += " WHERE timestamp >= :start AND timestamp <= :end"
                query_params['start'] = str(search_points_details['start'])
                query_params['end'] = str(search_points_details['end'])
            elif 'start' in search_points_details:
                query += " WHERE timestamp >= :start"
                query_params['start'] = str(search_points_details['start'])
            elif 'end' in search_points_details:
                query += " WHERE timestamp <= :end"
                query_params['end'] = str(search_points_details['end'])
            
            limit = 100
            if 'limit' in search_points_details:
                if search_points_details['limit'] < 1000:
                    limit = search_points_details['limit'] 
                else:
                    limit = 1000
                    
            query += " ORDER BY timestamp DESC LIMIT :limit"
            query_params['limit'] = str(limit)
            
            contents, success = self.query( query, query_params )
            
            if success:
                for point in contents:
                    if 0 == points_details['points-length']:
                        points.append({'at':point[0],'value':point[1]})
                    else:
                        points.append({'at':point[0],'value':point[1:]})
                
                self.db_message['points-details'] =\
                    copy.deepcopy( self.networks[network_id]['objects'][object_id]['streams'][stream_id]['points-details'] )
                if len(points) > 1 and isinstance(points[0]['value'],(int,float)):
                    min_val = points[0]['value']
                    max_val = points[0]['value']
                    for i in range(1,len(points)):
                        if points[i]['value'] > max_val:
                            max_val = points[i]['value']
                        elif points[i]['value'] < min_val:
                            min_val = points[i]['value']
                    self.db_message['points-details']['search-min-value'] = min_val
                    self.db_message['points-details']['search-max-value'] = max_val
                
                searched = True
                self.db_message['points'] = points
                self.db_message['points-message'] =\
                    "Points "+network_id+"."+object_id+"."+stream_id+".points Searched"
                self.db_message['points-code'] = 200
                self.debug( "Points "+network_id+"."+object_id+"."+stream_id+".points Searched" )
                    
            else:
                self.db_message['points-error'] =\
                    "Points "+network_id+"."+object_id+"."+stream_id+".points Not Searched"
                self.db_message['points-code'] = 400
                self.debug( "Error: Points "+network_id+"."+object_id+"."+stream_id+".points Not Searched" )
                
        except:
            self.db_message['points-error'] =\
                "Points "+network_id+"."+object_id+"."+stream_id+".points Not Searched"
            self.db_message['points-code'] = 400
            self.debug( "Error: Points "+network_id+"."+object_id+"."+stream_id+".points Not Searched" )
            self.debug( "Unexpected error (17):"+str(sys.exc_info()) )
                
        return searched

    '''
    Try loading the network from the database
    '''
    def loadNetworkRecord(self,network_id):
        exists = False

        try:
            # Try to query database
            query = "SELECT network_record FROM wcc_networks "
            query += "WHERE network_id=:network_id "
            query += "ORDER BY date(timestamp) DESC Limit 1"
            query_params = {
                'network_id': network_id
            }
            contents, success = self.query( query, query_params )
            
            if success and len(contents) > 0:
                # Get most recent network information
                self.networks[network_id] = json.loads( contents[0][0] )
                self.debug( "Network "+network_id+" Loaded" )
                exists = True
            else:
                self.debug( "Network "+network_id+" Not Loaded" )
                            
        except:
            self.debug( "Unexpected error (18):"+str(sys.exc_info()) )
            self.debug( "Network "+network_id+" Not Loaded" )
        
        return exists
        
    '''
    Check if network exists.    
    '''
    def networkExists(self,ids):
        network_id = ids[0]
        try:
            assert self.networks[network_id]['network-id'] == network_id
            self.debug( "Network "+network_id+" Found" )
            return True
        except:
            self.debug( "Network "+network_id+" Not Found" )
        return False
        
    '''
    Check if object exists.
    '''
    def objectExists(self,ids):
        network_id,object_id = ids
        
        try:
            assert self.networks[network_id]['objects'][object_id]['object-id'] == object_id
            self.debug( "Object "+network_id+"."+object_id+" Found" )
            return True
        except:
            self.debug( "Object "+network_id+"."+object_id+" Not Found" )
        return False
        
    '''
    Check if stream exists.
    '''
    def streamExists(self,ids):
        network_id,object_id,stream_id = ids
                        
        try:
            assert stream_id == self.networks[network_id]['objects'][object_id]['streams'][stream_id]['stream-id']
            # Assert IDs do not contain prohibited characters
            assert len( re.findall( "[^a-zA-Z0-9\-\_]", network_id+object_id+stream_id ) ) == 0
            
            db_exists = False
            
            # Try to query database
            table_name = network_id+'.'+object_id+'.'+stream_id
            query = "SELECT * FROM '"+table_name+"'"
            contents, success = self.query(query)
            if success:
                self.debug( "Stream "+network_id+"."+object_id+"."+stream_id+" DB Found" )
                db_exists = True
            else:
                self.debug( "Stream "+network_id+"."+object_id+"."+stream_id+" DB Not Found" )

            if db_exists:
                self.debug( "Stream "+network_id+"."+object_id+"."+stream_id+" Found" )
                return True
            
        except:
            pass
        
        self.debug( "Stream "+network_id+"."+object_id+"."+stream_id+" Not Found" )        
        return False
        