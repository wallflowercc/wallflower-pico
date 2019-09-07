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

import datetime
import re

# For Python 2.* and 3.* support.
try:
  basestring
except NameError:
  basestring = str

class SchemaError(Exception):

    """Error during Schema validation."""

    def __init__(self, autos, errors, details={}):
        self.autos = autos if type(autos) is list else [autos]
        self.errors = errors if type(errors) is list else [errors]
        Exception.__init__(self, self.code)

    @property
    def code(self):
        def uniq(seq):
            seen = set()
            seen_add = seen.add
            return [x for x in seq if x not in seen and not seen_add(x)]
        a = uniq(i for i in self.autos if i is not None)
        e = uniq(i for i in self.errors if i is not None)
        if e:
            return '\n'.join(e)
        return '\n'.join(a)

    def get_last_error(self):
        last = None
        
        for i, e in enumerate(reversed(self.errors)):
            if e is not None:
                last = e
                break
                
        for i, e in enumerate(reversed(self.autos)):
            if e is not None:
                if last is None:
                    return e
                    
                last += ": " + e
                return last
                
        return last
          
         
class Base(object):
    
    def __init__(self, *args, **kw):
        self._args = args
        assert list(kw) in (['error'], [])
        self._error = kw.get('error')

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__,
                           ', '.join(repr(a) for a in self._args))
                           
class And(Base):
    
    def validate(self, data):
        for s in [Schema(s, error=self._error) for s in self._args]:
            data = s.validate(data)
        return data


class Or(And):

    def validate(self, data):
        x = SchemaError([], [])
        for s in [Schema(s, error=self._error) for s in self._args]:
            try:
                return s.validate(data)
            except SchemaError as _x:
                x = _x
        raise SchemaError(['%r did not validate %r' % (self, data)] + x.autos,
                          [self._error] + x.errors)


class Use(object):

    def __init__(self, callable_, error=None):
        assert callable(callable_)
        self._callable = callable_
        self._error = error

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self._callable)

    def validate(self, data):
        try:
            return self._callable(data)
        except SchemaError as x:
            raise SchemaError([None] + x.autos, [self._error] + x.errors)
        except BaseException as x:
            f = self._callable.__name__
            raise SchemaError('%s(%r) raised %r' % (f, data, x), self._error)


def priority(s):
    """Return priority for a give object."""
    if hasattr(s, '_priority') and isinstance( s._priority, int ):
        return s._priority
    if type(s) in (list, tuple, set, frozenset):
        return 6
    if type(s) is dict:
        return 5
    if hasattr(s, 'validate'):
        return 4
    if issubclass(type(s), type):
        return 3
    if callable(s):
        return 2
    else:
        return 1


class Schema(object):

    def __init__(self, schema, error=None, priority=None ):
        self._schema = schema
        self._error = error
        self._priority = priority
        
    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self._schema)

    def validate(self, data):
        s = self._schema
        e = self._error
        if type(s) in (list, tuple, set, frozenset):
            data = Schema(type(s), error=e).validate(data)
            return type(s)(Or(*s, error=e).validate(d) for d in data)
        if type(s) is dict:
            data = Schema(dict, error=e).validate(data)
            
            new = type(data)()  # new - is a dict of the validated values
            x = None
            coverage = set()  # non-optional schema keys that were matched
            # for each key and value find a schema entry matching them, if any
            sorted_skeys = list(sorted(s, key=priority))
            
            for key, value in data.items():
                valid = False
                skey = None
                for skey in sorted_skeys:
                    svalue = s[skey]
                    try:
                        nkey = Schema(skey, error=e).validate(key)
                    except SchemaError:
                        pass
                    else:
                        try:
                            nvalue = Schema(svalue, error=e).validate(value)
                        except SchemaError as _x:
                            x = _x
                            raise
                        else:
                            coverage.add(skey)
                            valid = True
                            break
                if valid:
                    new[nkey] = nvalue
                elif skey is not None:
                    if x is not None:
                        raise SchemaError(['invalid value for key %r' % key] +
                                          x.autos, [e] + x.errors)
            coverage = set(k for k in coverage if type(k) is not Optional)
            required = set(k for k in s if type(k) is not Optional)
            if coverage != required:
                s_missing_keys = ', '.join('%r' % k for k in (required - coverage))
                raise SchemaError('Missing key(s) %s' % s_missing_keys, e)
            if len(new) != len(data):
                wrong_keys = set(data.keys()) - set(new.keys())
                s_wrong_keys = ', '.join('%r' % k for k in sorted(wrong_keys))
                #raise SchemaError('wrong key(s) %s in %r' % (s_wrong_keys, data),e)
                raise SchemaError('Invalid key(s) %s' % (s_wrong_keys),e)
            return new
        if hasattr(s, 'validate'):
            try:
                return s.validate(data)
            except SchemaError as x:
                raise SchemaError([None] + x.autos, [e] + x.errors)
            except BaseException as x:
                raise SchemaError('%r.validate(%r) raised %r' % (s, data, x),
                                  self._error)
        if issubclass(type(s), type):
            if isinstance(data, s):
                return data
            else:
                raise SchemaError('%r should be instance of %r' % (data, s), e)
        if callable(s):
            f = s.__name__
            try:
                if s(data):
                    return data
            except SchemaError as x:
                raise SchemaError([None] + x.autos, [e] + x.errors)
            except BaseException as x:
                raise SchemaError('%s(%r) raised %r' % (f, data, x),
                                  self._error)
            raise SchemaError('%s(%r) should evaluate to True' % (f, data), e)
        if s == data:
            return data
        else:
            raise SchemaError('%r does not match %r' % (s, data), e)


class Optional(Schema):

    """Marker for an optional part of Schema."""
    
class Timestamp(Base):
    
    """ISO 8601 Timestamp."""
    
    def validate(self, data):
        try:
            datetime.datetime.strptime(data, self._args[0])
            return data
        except:
            pass
        raise SchemaError('Invalid timestamp %s' % (data), self._error)
      
class Alphanumeric(Base):
    
    """ Check that string only contains alphanumeric characters """

    def validate(self, data):
        non_alphanumeric = list(set(re.findall( "[^a-zA-Z0-9]", data)))
        if len(non_alphanumeric) > 0:
            chars = ', '.join(non_alphanumeric)
            raise SchemaError('Not permitted to contain the '+ \
            'character(s) %s' % (chars), self._error )
        return data
      
class AlphanumericWithExceptions(Base):
    
    """ Check that string only contains alphanumeric characters, 
        with some exceptions passed in as a list """

    def validate(self, data):
        non_alphanumeric = list(set(re.findall( "[^a-zA-Z0-9]", data)))
        if len(non_alphanumeric) > 0:
            exceptions = self._args[0]
            not_permitted = []
            for char in non_alphanumeric:
                if char not in exceptions:
                    not_permitted.append(char)
            if len(not_permitted) > 0:
                chars = ', '.join(not_permitted)
                raise SchemaError('Not permitted to contain the '+\
                'character(s) %s' % (chars), self._error )
        return data
        
class CheckRegularExpression(Base):
    
    """ Check that string only contains certain characters given by a
        regular expression. If any character is removed from the string,
        an error is raised. """

    def validate(self, data):
        filtered_string = list(set(re.findall( self._args[0], data)))
        if set(filtered_string) != set(data):
            not_permitted = [x for x in set(data) if x not in set(filtered_string)]
            chars = ', '.join(not_permitted)
            raise SchemaError('Not permitted to contain '+\
            'the character(s) %s' % (chars), self._error )
        return data


        
class In(Base):
    
    """In list or tuple"""
        
    def validate(self, data):
        if data in self._args[0]:
            return data
        raise SchemaError('%r not in %r' % (data, self._args[0]), self._error)

class AtLeastOne(Base):
    
    """A dictionary must have at least one of the given keys"""
        
    def validate(self, data):
        for key in self._args[1]:
            if key in data:
                # Found
                return Schema(self._args[0], error=self._error).validate(data)
        raise SchemaError('None of the keys %r found in dict' % (self._args[1]), self._error)
        
class ExactlyOne(Base):
    
    """A dictionary must have exactly one of the given keys"""
        
    def validate(self, data):
        count = 0
        for key in self._args[1]:
            if key in data:
                # Found
                count += 1
        if 1 == count:
            return Schema(self._args[0], error=self._error).validate(data)
        raise SchemaError('Dict may contain only one of of the keys %r' % (self._args[1]), self._error)

class NoneOf(Base):
    
    """A dictionary must have none of the given keys"""
        
    def validate(self, data):
        for key in self._args[1]:
            if key in data:
                raise SchemaError('Key %r not allowed in dict' % (key), self._error)
                
        return Schema(self._args[0], error=self._error).validate(data)
        

class TypeOr(Base):

    """Check a list of possible types"""    
    
    def validate(self, data):
        for s in [Schema(s, error=self._error) for s in self._args]:
            try:
                return s.validate(data)
            except SchemaError:
                pass
        raise SchemaError('Valid type not found', self._error)
        
                  
class LowerUpperBound(Base):
    
    """Filter according to lower and upper bound"""
    
    def validate(self, data):
        if data < self._args[0]:
            data = self._args[0]
        elif data > self._args[1]:
            data = self._args[1]
        return data

class RemoveAll(Base):
    
    """Remove the given keys, but do not raise error."""
        
    def validate(self, data):
        for key in self._args[1]:
            if key in data:
                del(data[key])
                
        return Schema(self._args[0], error=self._error).validate(data)


'''
Python Schema for Wallflower API

'''
class WallflowerSchema():
    # Regular expression for accepted id characters
    #id_chars_re = "[^a-zA-Z0-9\-\_]"
    # Non-alphanumeric characters accepted in ids
    # Only checked for create requests
    id_chars = ['-','_']

    # Accepted ISO 8601 datetime format
    datetime_format_full = '%Y-%m-%dT%H:%M:%S.%fZ'
    datetime_format_min = '%Y%m%dT%H%M%S%fZ'
    # Read up to 500 points
    read_hard_limit = 500
    
    # Interpret data_type=0 as signed char
    # Ref: https://docs.python.org/2/library/struct.html
    stream_type_list = ['data','event']
    data_type_list = ['b','?','c','b','B','h','H','i','I','q','Q','f','d','s']
    
    # TODO: Incorporate...
    data_type_list_full = [
        'b',
        'boolean',
        'char',
        'signed char',
        'unsigned char',
        'short',
        'unsigned short',
        'int',
        'unsigned int',
        'long',
        'unsigned long',
        'float',
        'double'
        'string']
    
    '''
    
    Schemas For All Request Types
    
    '''
    
    network_level = Schema({
        Optional('method'):basestring,
        'network-id': basestring,
        Optional('network-details'): dict,
        Optional('objects'): dict
    }, error="Network Level Error, no network or object request(s) found")
    
    object_level = Schema({
        'object-id': basestring,
        Optional('object-details'): dict,
        Optional('streams'): dict
    }, error="Object Level Error, no object or stream request(s) found")
    
    stream_level = Schema({
        'stream-id': basestring,
        Optional('stream-details'): dict,
        Optional('points'): Or(list,dict),
        Optional('points-details'): dict
    }, error="Stream Level Error, no stream or point request(s) found")
        
    '''
    
    Schemas For Creating a Network, Object, or Stream
    
    '''
    
    network_details_create = Schema({  
        'network-name': basestring,
        Optional(basestring,priority=5): object
    }, error = 'Invalid network details')
    
    network_create = Schema({    
        'network-id': And(
            basestring,
            AlphanumericWithExceptions(
                id_chars, error="Invalid network-id"
            )
        ),
        'network-details': network_details_create,
        Optional('objects'): {
            basestring: dict   
        }
    }, error = 'Invalid network create request')

    object_details_create = Schema({  
        'object-name': basestring,
        Optional(basestring,priority=5): object
    }, error = 'Invalid object details')
    
    object_create = Schema({   
        'object-id': And(
            basestring,
            AlphanumericWithExceptions(
                id_chars, error="Invalid object-id"
            )
        ),
        'object-details': object_details_create,
        Optional('streams'): {
            basestring: dict   
        }
    }, error = 'Invalid object create request')
    
    stream_type = Or(
        And(int, In(range(0,4,1))),
        And(basestring, In(stream_type_list)),
        error='Invalid stream type'
    )
    
    data_type = Or(
        And(int, In(range(0,16,1))),
        And(basestring, In(data_type_list)),
        error='Invalid data type'
    )
    
    stream_details_create = Schema({
        'stream-name': basestring,
        'stream-type': stream_type,
        Optional('units'): basestring,
        Optional(basestring,priority=5): object
    }, error = 'Invalid stream details')

    points_details_create = Schema({
        'points-type': data_type,
        'points-length': int,
        Optional(basestring,priority=5): object
    }, error = 'Invalid points details')
    
    # TODO: Make points_details optional
    stream_create = Schema(RemoveAll({ 
        'stream-id': And(
            basestring,
            AlphanumericWithExceptions(
                id_chars, error="Invalid stream-id"
            )
        ),
        'stream-details': stream_details_create,
        'points-details': points_details_create,
    },['points']), error = 'Invalid stream create request')
    
    create = Schema(AtLeastOne({
        'network-id': And(
            basestring,
            AlphanumericWithExceptions(id_chars)
        ),
        Optional('network-details'): network_details_create,
        Optional('objects'): {
            basestring: AtLeastOne({   
                'object-id': And(
                    basestring,
                    AlphanumericWithExceptions(id_chars)
                ),
                Optional('object-details'): object_details_create,
                Optional('streams'): {
                    basestring: RemoveAll({    
                        'stream-id': And(
                            basestring,
                            AlphanumericWithExceptions(id_chars)
                        ),
                        'stream-details': stream_details_create,
                        'points-details': points_details_create,
                    },['points'])
                }
            }, ['object-details','streams'], error="No object or stream create request(s) found") 
        }
    }, ['network-details','objects'], error="No network or object create request(s) found"))
    
    '''
    
    Schemas For Reading a Network, Object, or Stream
    
    '''
    
    network_read = Schema({
        'network-id': basestring,
    }, error = 'Invalid network read request')
    
    object_read = Schema({ 
        'object-id': basestring,
    }, error = 'Invalid object read request')
    
    stream_read = Schema({ 
        'stream-id': basestring,
    }, error = 'Invalid stream read request')
    
    points_read = Schema([])
    
    read = Schema({
        'network-id': basestring,
        Optional('objects'): Schema({
            basestring: {   
                'object-id': basestring,
                Optional('streams'):  {
                    basestring: Schema({ 
                        'stream-id': basestring,
                        Optional('points'): points_read
                    }, error="No stream or point read request(s) found")
                }
            }
        }, error="No object or stream read request(s) found") 
    }, error="No network or object read request(s) found")
    
    
    '''
    
    Schemas For Updating a Network, Object, or Stream
    
    '''
    
    network_details_update = Schema({  
        Optional('network-name'): basestring,
        Optional(basestring,priority=5): object
    }, error = 'Invalid network details update')
    
    network_update = Schema({    
        'network-id': basestring,
        'network-details': network_details_update,
        Optional('objects'): {
            basestring: dict   
        }
    }, error = 'Invalid network update request')

    object_details_update = Schema({  
        Optional('object-name'): basestring,
        Optional(basestring,priority=5): object
    }, error = 'Invalid object details update')
    
    object_update = Schema({   
        'object-id': basestring,
        'object-details': object_details_update,
        Optional('streams'): {
            basestring: dict   
        }
    }, error = 'Invalid object update request')
            
    stream_details_update = Schema({
        Optional('stream-name'): basestring,
        Optional('stream-type'): stream_type,
        Optional('units'): basestring,
        Optional(basestring,priority=5): object
    }, error = 'Invalid stream details update')
    
    stream_update = Schema(RemoveAll({ 
        'stream-id': basestring,
        'stream-details': stream_details_update,
        Optional('points'): list
    }, ['points-details'], error = 'Invalid stream update request'))
    
    points_update = Schema([{
        'value': TypeOr(
            basestring,
            int,
            float,
            bool,
            [int],
            [float],
            [int,float], # Allow mixed numerical list
            [bool],
            error = 'Invalid point update request'
        ),
        Optional('at'): And(
            basestring,
            Or(
                Timestamp(datetime_format_full),
                Timestamp(datetime_format_min)
            )
        )
    }], error = 'Invalid point update request')
    
    update = Schema(AtLeastOne({
        'network-id': basestring,
        Optional('network-details'): network_details_update,
        Optional('objects'): {
            basestring: AtLeastOne({   
                'object-id': basestring,
                Optional('object-details'): object_details_update,
                Optional('streams'): {
                    basestring: AtLeastOne({ 
                        'stream-id': basestring,
                        Optional('stream-details'): stream_details_update,
                        Optional('points'): points_update
                    }, ['stream-details','points'], error="No stream or point update request(s) found")
                }
            }, ['object-details','streams'], error="No object or stream update request(s) found") 
        }
    }, ['network-details','objects'], error="No network or object update request(s) found"))
    
    
    '''
    
    Schemas For Deleting a Network, Object, or Stream
    
    '''
    
    network_delete = Schema({    
        'network-id': basestring,
    }, error = 'Invalid network delete request')
    
    object_delete = Schema({   
        'object-id': basestring,
    }, error = 'Invalid object delete request')
    
    stream_delete = Schema({ 
        'stream-id': basestring,
    }, error = 'Invalid stream delete request')
    
    delete = Schema({
        'network-id': basestring,
        Optional('objects'): Schema({
            basestring: {   
                'object-id': basestring,
                Optional('streams'):  {
                    basestring:{ 
                        'stream-id': basestring,
                    }
                }
            }
        }, error="No object or stream delete request(s) found") 
    }, error="No network or object delete request(s) found")
    

    '''
    
    Schemas For Searching for a Network, Object, Stream, or Points
    
    '''
    # TODO:
    network_search = Schema({
        'network-id': basestring,
    }, error = 'Invalid network search request')
    # TODO:
    object_search = Schema({ 
        'object-id': basestring,
    }, error = 'Invalid object search request')
    # TODO:
    stream_search = Schema({ 
        'stream-id': basestring,
    }, error = 'Invalid stream search request')
    
    points_search = Schema({
        Optional('start'): And(
            basestring,
            Or(
                Timestamp(datetime_format_full),
                Timestamp(datetime_format_min)
            )
        ),
        Optional('end'): And(
            basestring,
            Or(
                Timestamp(datetime_format_full),
                Timestamp(datetime_format_min)
            )
        ),
        Optional('limit'): And(int,LowerUpperBound(0,read_hard_limit))
    }, error = 'Invalid points search request')
    
    search = Schema({
        'network-id': basestring,
        Optional('objects'): Schema({
            basestring: {   
                'object-id': basestring,
                Optional('streams'):  {
                    basestring: Schema({ 
                        'stream-id': basestring,
                        Optional('points'): points_search
                    }, error="No stream or point search request(s) found")
                }
            }
        }, error="No object or stream search request(s) found") 
    }, error="No network or object search request(s) found")
    
        
    '''
    
    All Schemas
    
    '''
    schemas_dict = {
        'network-level': network_level,
        'object-level': object_level,
        'stream-level': stream_level,
        
        'create': create,
        'network-create': network_create,
        'object-create': object_create,
        'stream-create': stream_create,
        
        'read': read,
        'network-read': network_read,
        'object-read': object_read,
        'stream-read': stream_read,
        'points-read': points_read,
        
        'update': update,
        'network-update': network_update,
        'object-update': object_update,
        'stream-update': stream_update,
        'points-update': points_update,
        
        'delete': delete,
        'network-delete': network_delete,
        'object-delete': object_delete,
        'stream-delete': stream_delete,
        
        'search': search,
        'network-search': network_search,
        'object-search': object_search,
        'stream-search': stream_search,
        'points-search': points_search
    }
    
    
    # Try to validate the points request
    def validatePointsRequest(self,request,request_type,points_details=None):
        message_packet = None
        validated_request = None
        # Check points
        # TODO: Should request just be list of points?
        if 'points-'+request_type in self.schemas_dict:
            
            message_packet = {}
            validated_request = {}
            
            try:
                # Check
                validated_request['points'] = \
                    self.schemas_dict['points-'+request_type].validate(
                        request['points']
                    )
                
                # TODO: Check points-type
                
                message_packet['points-schema-message'] = 'Valid points '+request_type+' request found'
                message_packet['points-valid-request'] = True                                           
                message_packet['points-code'] = 200 
                
            except SchemaError as e:
                # Points level error.
                message_packet['points-schema-error'] = e.get_last_error()
                message_packet['points-valid-request'] = False
                message_packet['points-code'] = 400
        else:
            message_packet = {}
            message_packet['points-schema-error'] = 'Invalid Points Request'
            message_packet['points-valid-request'] = False
            message_packet['points-code'] = 400
            
        return validated_request, message_packet
    
    # Try to validate the stream request
    def validateStreamRequest(self,request,request_type,stream_details=None):
        message_packet = None
        validated_request = None
        # Check points
        if 'stream-'+request_type in self.schemas_dict:
                
            message_packet = {}
            validated_request = {}
            
            try:
                # Check
                validated_request = \
                    self.schemas_dict['stream-'+request_type].validate(
                        request
                    )
                    
                message_packet['stream-schema-message'] = 'Valid stream '+request_type+' request found'
                message_packet['stream-valid-request'] = True
                message_packet['stream-code'] = 200
            
            except SchemaError as e:
                # Points level error.
                message_packet['stream-schema-error'] = e.get_last_error()
                message_packet['stream-valid-request'] = False
                message_packet['stream-code'] = 400
        else:
            message_packet = {}
            message_packet['stream-schema-error'] = 'Invalid Stream Request'
            message_packet['stream-valid-request'] = False
            message_packet['stream-code'] = 400
            
        return validated_request, message_packet
        
    # Try to validate the object request
    def validateObjectRequest(self,request,request_type,object_details=None):
        message_packet = None
        validated_request = None
        # Check points
        if 'object-'+request_type in self.schemas_dict:
                
            message_packet = {}
            validated_request = {}
            
            try:
                # Check
                validated_request = \
                    self.schemas_dict['object-'+request_type].validate(
                        request
                    )
                    
                message_packet['object-schema-message'] = 'Valid object '+request_type+' request found'
                message_packet['object-valid-request'] = True
                message_packet['object-code'] = 200
            
            except SchemaError as e:
                # Points level error.
                message_packet['object-schema-error'] = e.get_last_error()
                message_packet['object-valid-request'] = False
                message_packet['object-code'] = 400
        else:
            message_packet = {}
            message_packet['object-schema-error'] = 'Invalid Object Request'
            message_packet['object-valid-request'] = False
            message_packet['object-code'] = 400
            
        return validated_request, message_packet
        
    # Try to validate the network request
    def validateNetworkRequest(self,request,request_type,network_details=None):
        message_packet = None
        validated_request = None
        # Check points
        if 'network-'+request_type in self.schemas_dict:
                
            message_packet = {}
            validated_request = {}
            
            try:
                # Check
                validated_request = \
                    self.schemas_dict['network-'+request_type].validate(
                        request
                    )
                    
                message_packet['network-schema-message'] = 'Valid network '+request_type+' request found'
                message_packet['network-valid-request'] = True
                message_packet['network-code'] = 200
            
            except SchemaError as e:
                # Points level error.
                message_packet['network-schema-error'] = e.get_last_error()
                message_packet['network-valid-request'] = False
                message_packet['network-code'] = 400
        else:
            message_packet = {}
            message_packet['network-schema-error'] = 'Invalid Network Request'
            message_packet['network-valid-request'] = False
            message_packet['network-code'] = 400
            
        return validated_request, message_packet
        
        
        
        
    # Try to validate the create request
    def validateMultipleCreateRequests(self,request,verbose=False):           
        return self.validateMultipleRequests(request,'create',verbose)

    # Try to validate the read request
    def validateMultipleReadRequests(self,request,verbose=False):
        return self.validateMultipleRequests(request,'read',verbose)
            
    # Try to validate the update request
    def validateMultipleUpdateRequests(self,request,verbose=False):
        return self.validateMultipleRequests(request,'update',verbose)
            
    # Try to validate the update request
    def validateMultipleDeleteRequests(self,request,verbose=False):
        return self.validateMultipleRequests(request,'delete',verbose)
        
    # Try to validate the update request
    def validateMultipleSearchRequests(self,request,verbose=False):
        return self.validateMultipleRequests(request,'search',verbose)
        
    # Try to validate the request
    # TODO: Use validateNetworkRequest, validateObjectRequest, etc.
    def validateMultipleRequests(self,request,request_type,verbose=False):
        validated_request = {}
        message = {}
        if not verbose:
            # Try everything in one go
            try:
                validated_request = self.schemas_dict[request_type].validate(request)
                return validated_request, {'valid-request':True}
            except SchemaError as e:
                return {}, {'schema-error': e.get_last_error()}
            except:
                return {}, {'schema-error':''}
        else:
            # Break things down.
            try:
                # Check if the request is even well formatted
                validated_request = self.schemas_dict['network-level'].validate(request)
                
                # Check for network request
                try:
                    validated_request = self.schemas_dict['network-'+request_type].validate(request)
                    message['network-schema-message'] = 'Network '+request_type+' request found'
                    message['valid-request'] = True
                except SchemaError as e:
                    # Invalid network details
                    if 'network-details' in request:
                        del(validated_request['network-details'])
                    message['network-schema-error'] = e.get_last_error()

                # Check objects
                if 'objects' in request:
                    message['objects'] = {}
                    
                    all_object_ids = request['objects'].keys()
                    for object_id in all_object_ids:
                        message['objects'][object_id] = {}
                        
                        try:
                            # Check if the request is even well formatted
                            request['objects'][object_id]['object-id'] = object_id
                            validated_request['objects'][object_id] =\
                                self.schemas_dict['object-level'].validate(request['objects'][object_id])
                            
                            # Check for object request
                            try:
                                validated_request['objects'][object_id] =\
                                    self.schemas_dict['object-'+request_type].validate(request['objects'][object_id])
                                message['objects'][object_id]['object-schema-message'] = 'Object '+request_type+' request found'
                                message['valid-request'] = True
                            except SchemaError as e:
                                # Invalid objects details
                                if 'object-details' in request['objects'][object_id]:
                                    del(validated_request['objects'][object_id]['object-details'])
                                message['objects'][object_id]['object-schema-error'] = e.get_last_error()

                            # Check streams
                            if 'streams' in request['objects'][object_id]:
                                message['objects'][object_id]['streams'] = {}
                                
                                all_stream_ids = request['objects'][object_id]['streams'].keys()
                                for stream_id in all_stream_ids:
                                    message['objects'][object_id]['streams'][stream_id] = {}
                                    
                                    try:
                                        # Check if the request is even well formatted
                                        request['objects'][object_id]['streams'][stream_id]['stream-id'] = stream_id
                                        
                                        validated_request['objects'][object_id]['streams'][stream_id] =\
                                            self.schemas_dict['stream-level'].validate(request['objects'][object_id]['streams'][stream_id])

                                        # Check for stream request
                                        try:
                                            validated_request['objects'][object_id]['streams'][stream_id] =\
                                                self.schemas_dict['stream-'+request_type].validate(request['objects'][object_id]['streams'][stream_id])
                                            message['objects'][object_id]['streams'][stream_id]['stream-schema-message'] = 'Stream '+request_type+' request found'
                                            message['valid-request'] = True
                                        except SchemaError as e:
                                            # Invalid stream details
                                            if 'stream-details' in request['objects'][object_id]['streams'][stream_id]:
                                                del(validated_request['objects'][object_id]['streams'][stream_id]['stream-details'])
                                                message['objects'][object_id]['streams'][stream_id]['stream-schema-error'] = e.get_last_error()


                                        # Check points
                                        if 'points' in request['objects'][object_id]['streams'][stream_id] and 'points-'+request_type in self.schemas_dict:
                                            message['objects'][object_id]['streams'][stream_id]['points'] = {}

                                            try:
                                                # Check
                                                validated_request['objects'][object_id]['streams'][stream_id]['points'] =\
                                                    self.schemas_dict['points-'+request_type].validate(request['objects'][object_id]['streams'][stream_id]['points'])
                                                                                                
                                                message['objects'][object_id]['streams'][stream_id]['points']['points-schema-message'] = 'Points '+request_type+' request found'
                                                message['valid-request'] = True                                                
                                            
                                            except SchemaError as e:
                                                # Points level error. Entirely remove points
                                                del(validated_request['objects'][object_id]['streams'][stream_id]['points'])
                                                message['objects'][object_id]['streams'][stream_id]['points']['points-schema-error'] = e.get_last_error()
                                         
                                    except SchemaError as e:
                                        # Stream level error. Entirely remove stream
                                        del(validated_request['objects'][object_id]['streams'][stream_id])
                                        message['objects'][object_id]['streams'][stream_id]['stream-schema-error'] = e.get_last_error()
                            
                        except SchemaError as e:
                            # Object level error. Entirely remove object
                            del(validated_request['objects'][object_id])
                            message['objects'][object_id]['object-schema-error'] = e.get_last_error()
            
            except SchemaError as e:
                # Network level error. Entirely remove network
                validated_request = {}
                message['network-schema-error'] = e.get_last_error()
            
            return validated_request, message
            
