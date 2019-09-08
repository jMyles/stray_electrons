import datetime
from statistics import mean

import maya

from aggregate import EnergyAggregator
from client import INFLUX_CLIENT, MQTT_CLIENT
from push_to_datastores import Announcement
aggregator = EnergyAggregator()

now = maya.now()
cursor = yesterday = maya.when("yesterday")
_24h_earlier = at_the_pogo_lounge = yesterday - datetime.timedelta(hours=24)

readings = list(aggregator.take_readings(begin=at_the_pogo_lounge.datetime(naive=True), end=now.datetime(naive=True)))
v_response = INFLUX_CLIENT.query(
    "select battery_voltage FROM solar_controller_readings WHERE time > {}s ORDER BY time ASC".format(
        int(at_the_pogo_lounge.epoch)))

amp_averages = {}

cleaned_readings = {}

for reading in readings:
    dt = maya.parse(reading.pop('time'))
    cleaned_readings[dt] = reading

while cursor < now:
    early_readings = []
    penalty_whours = []
    shunt_1_whours = []
    shunt_2_whours = []
    shunt_3_whours = []
    late_readings = []
    for dt, reading in cleaned_readings.items():
        if dt < _24h_earlier:
            # This reading is too early.
            early_readings.append(reading)
            continue
        elif dt > cursor:
            # This reading is too late
            late_readings.append(reading)
            continue
        else:
            penalty_whours.append(reading['penalty'])
            shunt_1_whours.append(reading['shunt_1'])
            shunt_2_whours.append(reading['shunt_2'])
            shunt_3_whours.append(reading['shunt_3'])

    amp_averages[cursor] = {
        "penalty": mean(penalty_whours),
        "shunt_1": mean(shunt_1_whours),
        "shunt_2": mean(shunt_2_whours),
        "shunt_3": mean(shunt_3_whours),
    }
    cursor = cursor + datetime.timedelta(minutes=1)
    _24h_earlier = cursor - datetime.timedelta(hours=24)
    continue


    _24h_voltages = [r['battery_voltage'] for r in v_response.get_points()]
    mean_voltage = mean(_24h_voltages)
    min_voltage = min(_24h_voltages)
    voltage_announcement = Announcement("voltage_24h_averages", INFLUX_CLIENT, MQTT_CLIENT, mqtt_topic_prefix="solar/")
    voltage_announcement.include_as_definitive("24h_average_voltage", round(mean_voltage, 3))
    voltage_announcement.include_as_definitive("24h_min_voltage", round(min_voltage, 3))
    voltage_announcement.transmit()

    # Get the whours for each value.
    # We take the average amps through the past 24 hours.
    penalty_whours_24 = aggregator.amp_averages[0][1]['penalty']
    shunt_1_whours_24h = aggregator.amp_averages[0][1]['shunt_1']
    shunt_2_whours_24h = aggregator.amp_averages[0][1]['shunt_2']
    shunt_3_whours_24h = aggregator.amp_averages[0][1]['shunt_3']
    whours_announcement = Announcement("shunt_readings_24h_average", INFLUX_CLIENT, MQTT_CLIENT,
                                       mqtt_topic_prefix="energy/")

    whours_announcement.include_as_definitive("24h_penalty_whours", round(penalty_whours_24, 3))
    whours_announcement.include_as_definitive("24h_shunt1_whours", round(shunt_1_whours_24h, 3))
    whours_announcement.include_as_definitive("24h_shunt2_whours", round(shunt_2_whours_24h, 3))
    whours_announcement.include_as_definitive("24h_shunt3_whours", round(shunt_3_whours_24h, 3))
    whours_announcement.transmit()
