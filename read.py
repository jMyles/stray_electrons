from __future__ import print_function
import subprocess
import json
import datetime
import time
import os

from push_to_datastores import Announcement

DEBUG = False
PENALTY = .15

import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient

# InfluxDB connections settings
host = '10.0.80.30'
dbname = 'energy'

INFLUX_CLIENT = InfluxDBClient(host, database=dbname)

MQTT_CLIENT = mqtt.Client()
MQTT_CLIENT.connect("10.0.80.30", 1883, 60)

shunt_announcement = Announcement("shunt_readings", INFLUX_CLIENT, MQTT_CLIENT, mqtt_topic_prefix="house/power/")

transmit_time = time.time() + 2

while True:
    reader = subprocess.Popen(os.path.expanduser('~/git/pmcomm/libpmcomm/example/example'), stdout=subprocess.PIPE)
    json_power_dict, error = reader.communicate()
    try:
        power_dict = json.loads(json_power_dict)
    except ValueError:
            if DEBUG:
                print("Unable to read JSON: %s" % json_power_dict, error)
            continue

    shunt_announcement.include_for_average("shunt_1", power_dict['a1'])
    shunt_announcement.include_for_average("shunt_2", power_dict['a2'])
    shunt_announcement.include_for_average("shunt_3", power_dict['a3'])

    net = power_dict['a1'] + power_dict['a2'] + power_dict['a3']
    if net < 0:
        penalty = net * PENALTY
    else:
        penalty = 0

    shunt_announcement.include_for_average("penalty", penalty)

    if time.time() > transmit_time:
        shunt_announcement.transmit()
        for topic, value in shunt_announcement.mqtt_payload.items():
            MQTT_CLIENT.publish("house/power/" + topic.replace("shunt", "watts"), round(value * 12, 3))
        MQTT_CLIENT.publish("house/power/net", round(sum(shunt_announcement.mqtt_payload.values()) * 12, 3))
        shunt_announcement = Announcement("shunt_readings", INFLUX_CLIENT, MQTT_CLIENT, mqtt_topic_prefix="house/power/")
        transmit_time = time.time() + 2




