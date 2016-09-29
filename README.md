# Wallflower.Pico Server

Wallflower.Pico is the first server released by [Wallflower.cc][wcc] and provides a simple and easy-to-learn introduction to the WCC platform. The Wallflower.Pico server implements the HTTP API and includes a JS/jQuery web interface. A demo of the interface can be found at [Wallflower.cc/pico-demo][wccdemo] The server is written in Python and the HTTP API is built upon a Flask app. To enable real-time communication, Wallflower.Pico also supports the addition of a WebSocket port using an Autobahn/Twisted server. An SQLite database is employed for storage. We have elected to leave the API exposed in the server, demo, and interface code to enable easy modification and experimentation.

#### Quick Start Guide

Wallflower.Pico requires Flask, which can be installed with pip
```sh
$ pip install Flask
```
To start the server on your computer, run
```sh
$ python wallflower_pico_server.py
```
Open a web browser and navigate to http://127.0.0.1:5000/ to view the interactive dashboard. By default, the server will also be publically available on your network and accessible via the IP address of your computer (i.e. http://IP_ADDRESS:5000/).

The Wallflower.Pico server is capable of broadcasting changes to the network over a WebSocket port. This allows devices and interfaces to be updated in real-time by receiving all successful responses to create, update, and delete requests. Before enabling the WebSocket funtionality, you will need to install Autobahn|Python, an open-source server for the WebSocket protocol, which can be installed with pip
```sh
$ pip install autobahn[twisted]
```
To start the server with the WebSocket enabled, edit the wallflower_config.json file and change
```sh
"enable_ws": false,
```
to
```sh
"enable_ws": true,
```
Then start the server by running
```sh
$ python wallflower_pico_server.py
```
Note that the WebSocket port is not capable of receiving requests and any messages sent to the port will be ignored.

The wallflower_demo.py file includes sample Python code for creating objects and streams and for sending new datapoints. Running the demo code with the WebSocket port enabled will illustrate the capabilities of the interactive dashboard. The wallflower_demo_ws.py file includes sample code for listening to messages broadcast by the WebSocket port using an Autobahn client.

The Wallflower.Pico server is still in beta development, so if you find a bug, please let us know.

#### License

The Wallflower.Pico source code is licensed under the [AGPL v3][agpl]. You can find a reference to this license at the top of each source code file.

Components which connect to the server via the API are not affected by the AGPL. This extends to the Python example code and the HTML, JS, and CSS code of the web interface, which are licensed under the [MIT license][mit].

In summary, any modifications to the Wallflower.Pico source code must be distributed according to the terms of the AGPL v3. Any code that connects to a Wallflower.cc server via an API is recognized as a seperate work (not a derivative work) irrespective of where it runs. Lastly, you are free to modify the HTML, JS, and CSS code of the web interface without restrictions, though we would appreciate you sharing what you have created.


[wcc]: <http://wallflower.cc>
[wccdemo]: <http://wallflower.cc/pico-demo>
[mit]: <https://opensource.org/licenses/MIT>
[agpl]: <https://opensource.org/licenses/AGPL-3.0>
