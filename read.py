from __future__ import print_function
import subprocess
import json
import datetime
import time
import os

from push_to_datastores import Announcement

DEBUG = False
PENALTY = .18

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

    a1_reading = power_dict['a1']
    a2_reading = power_dict['a2']
    a3_reading = power_dict['a3']

    if a1_reading > 90:
        a1_reading = 0

    if a2_reading > 90:
        a2_reading = 0

    if a3_reading > 90:
        a3_reading = 0

    shunt_announcement.include_for_average("shunt_1", a1_reading)
    shunt_announcement.include_for_average("shunt_2", a2_reading)
    shunt_announcement.include_for_average("shunt_3", a3_reading)

    net = a1_reading + a2_reading + a3_reading
    if net < 0:
        penalty = net * PENALTY
    else:
        penalty = 0

    shunt_announcement.include_for_average("penalty", penalty)

    if time.time() > transmit_time:
        shunt_announcement.do_averages()
        shunt_announcement.transmit()
        for topic, value in shunt_announcement.mqtt_payload.items():
            MQTT_CLIENT.publish("house/power/" + topic.replace("shunt", "watts"), round(value * 12, 3))
        MQTT_CLIENT.publish("house/power/net", round(sum(shunt_announcement.mqtt_payload.values()) * 12, 3))
        shunt_announcement = Announcement("shunt_readings", INFLUX_CLIENT, MQTT_CLIENT, mqtt_topic_prefix="house/power/")
        transmit_time = time.time() + 2




