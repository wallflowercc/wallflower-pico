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
 In this example, we listen to the responses being broadcast through the WebSocket.
'''

import json
from twisted.internet import reactor
from autobahn.twisted.websocket import WebSocketClientProtocol, \
    WebSocketClientFactory, \
    connectWS
    
base = 'ws://127.0.0.1:5050'
network_id = 'local'

class WallflowerClientProtocol(WebSocketClientProtocol):

    def onConnect(self, response):
        print("Connected: {}".format(response.peer))

    def onOpen(self):
        print("WebSocket connection open.")

    def onMessage(self, payload, isBinary):
        if isBinary:
            print("Binary message received: {} bytes".format(len(payload)))
        else:
            try:
                json_response = json.loads( payload.decode('utf8') )
                print( "JSON message received:" )
                print( json.dumps(json_response,indent=4) )
            except:
                print("Text message received: {}".format(payload.decode('utf8')))

    def onClose(self, wasClean, code, reason):
        print("WebSocket connection closed: {}".format(reason))


if __name__ == '__main__':
    # Start the WebSocket client
    factory = WebSocketClientFactory(base+'/network/'+network_id, debug=False)
    factory.protocol = WallflowerClientProtocol
    connectWS(factory)
    reactor.run()