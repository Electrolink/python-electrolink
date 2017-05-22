import electroServer.electrolink as electrolink
import modules.electroFiles as electroFiles
from json import loads
import time

config = loads((open("config.json", "r").read()))

# Give board a server name
e = electrolink.Electrolink(config["name"])

# extend Electrolink with additional fnctions
e.addCallbacks(electroFiles.callbacks)

# assuming that machine is already connected to Internet

# Broker MQTT server, mqtt protocol default port 1883
e.connectToServer(config["broker_server"])

while True:
    # blocking function, waiting for new message
    #e.waitForMessage()
    time.sleep(0.5)
    # or use non-blocking message to do something else in this file
    # while checking for new messages
    #e.checkForMessage()
