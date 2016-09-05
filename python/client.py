import sqlite3
import time

import google.protobuf
import paho.mqtt.client as mqtt

import protocol_pb2

# Number of inserts before we commit to the SQLite database.
# Updates come every ~200 ms, so this is ~20 seconds of data
COMMIT_AFTER = 100

conn = sqlite3.connect('temp.db')
c = conn.cursor()


# Callback for connecting to the MQTT server
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

    client.subscribe("temp/#")


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    data = protocol_pb2.TemperatureMessage()

    # We expect only TemperatureMessages on this bus, but protect against other messages
    try:
        data.ParseFromString(msg.payload)

        fahrenheit = data.temperatureValue*0.0625*9/5+32
        print(msg.topic+" deg F: "+str(fahrenheit))

        client = str.split(msg.topic, '/')[1]

        t = (client, time.time(), fahrenheit,)
        c.execute('INSERT INTO Temperature VALUES (?,?,?)', t)
        on_message.execute_count += 1

        if on_message.execute_count >= COMMIT_AFTER:
            conn.commit()
            on_message.execute_count = 0
    except google.protobuf.message.DecodeError:
        print(msg.topic+" Expected TemperatureMessage but got: "+str(msg.payload))
on_message.execute_count = 0


# Create the MQTT client and connect
mqttClient = mqtt.Client()

mqttClient.on_connect = on_connect
mqttClient.on_message = on_message

mqttClient.connect("localhost", 1883, 60)

mqttClient.loop_forever()
