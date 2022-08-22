import os
import paho.mqtt.client as mqtt
import json
from influxdb import InfluxDBClient
from datetime import datetime

# Get config file
def read_config():
    dir_name = os.path.dirname(__file__)
    filename = os.path.join(dir_name, 'config.json')
    try:
        with open(filename, mode='r') as f:
            return json.loads(f.read())
    except FileNotFoundError:
        return {}

config = read_config()

# Select Database
dbclient = InfluxDBClient('localhost', 8086)
dbclient.switch_database(config['db-name'])

# MQTT broker credentials
USER = config['broker-user']
PASS = config['broker-pass']

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("Ridgeway/Office2/#")

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    payload = json.loads(msg.payload.decode())
    topic = msg.topic
    deviceid = topic.split('/')[3]
    slash = topic.rfind('/')
    measurement = topic[:slash]
    #print(measurement)
    #print(deviceid)
    json_payload = []
    data = {
        "measurement": measurement,
        "tags": {"deviceid": deviceid},
        "fields": payload
            }
    json_payload.append(data)
    # Send payload to influxdb
    dbclient.write_points(json_payload)

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.username_pw_set(USER, PASS)

client.connect("localhost", 1883, 60)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()
