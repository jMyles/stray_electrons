from __future__ import print_function

from statistics import mean

import maya
import time
import datetime

from aggregate import EnergyAggregator
from push_to_datastores import Announcement
from client import CHARGE_CONTROLLER, INFLUX_CLIENT, MQTT_CLIENT

DEBUG = False
SECONDS_BETWEEN_AVERAGES = 20

import paho.mqtt.client as mqtt

CHARGE_STATES = (
    "START",
    "NIGHT_CHECK",
    "DISCONNECT",
    "NIGHT",
    "FAULT",
    "MPPT",
    "ABSORPTION",
    "FLOAT",
    "EQUALIZE",
    "SLAVE"
)


class CurrentReader(object):

    def __init__(self, high, low, bitdiv):
        self.bitdiv = bitdiv
        fractional = float(low) / float(2^16)
        self.scale = float(high) + fractional

    def __call__(self, reading):
        return float(reading) * self.scale / self.bitdiv


class VoltageReader(object):

    def __init__(self, high, low, bitdiv):
        self.bitdiv = bitdiv
        fractional = float(low) / float(2^16)
        self.scale = float(high) + fractional

    def __call__(self, reading):
        return float(reading) * self.scale / self.bitdiv

################
announcement = Announcement("solar_controller_readings", INFLUX_CLIENT, MQTT_CLIENT, mqtt_topic_prefix="solar/")
# whours_announcement = Announcement("whours_today", INFLUX_CLIENT, MQTT_CLIENT, mqtt_topic_prefix="house/power/")

transmit_time = time.time()

previous_aggregate_time = datetime.datetime.utcnow()
next_aggregate_time = datetime.datetime.utcnow()
print("Will aggregate at %s" % next_aggregate_time)
next_charge_state_check_time = maya.now()

readings_since_problem = 0

while True:
    before = time.time()
    try:
        result_reading = CHARGE_CONTROLLER.read_holding_registers(0, 68, unit=0x01)
        try:
            result_list = result_reading.registers
        except AttributeError as e:
            print("Got {} instead of a good reading (after {} good readings).".format(result_reading, readings_since_problem))
            readings_since_problem = 0
            continue
    except Exception as e:
        print("Encountered ({}) while reading (after {} good readings)".format(f, readings_since_problem))
        readings_since_problem = 0
        continue
    else:
        if DEBUG or readings_since_problem == 0:
            print("Good reading: {}".format(result_list))
        elif not readings_since_problem % 100:
            print("{} consecutive good readings.".format(readings_since_problem))
        readings_since_problem += 1
    finally:
        CHARGE_CONTROLLER.close()
    after = time.time()
    if DEBUG:
        print("Took %s seconds to read charge controller" % (after - before))

    ## Voltages
    voltage_reader = VoltageReader(high=result_list[0], low=result_list[1], bitdiv=32768)
    battery_voltage_reading = float(result_list[24])
    array_voltage_reading = float(result_list[27])

    battery_voltage = voltage_reader(battery_voltage_reading)
    array_voltage = voltage_reader(array_voltage_reading)

    ## Current
    current_reader = CurrentReader(high=result_list[2], low=result_list[3], bitdiv=32768)
    array_current = current_reader(result_list[29])
    battery_current = current_reader(result_list[28])

    amp_hours_today = result_list[67]

    announcement.include_for_average("battery_voltage", battery_voltage)
    announcement.include_for_average("array_voltage", array_voltage)
    announcement.include_for_average("input_current", array_current)
    announcement.include_for_average("output_current", battery_current)

    if time.time() > transmit_time:
        target_voltage = voltage_reader(result_list[51])
        charge_state = CHARGE_STATES[result_list[50]]
        announcement.include_as_definitive("target_voltage", round(target_voltage, 3))
        announcement.include_as_definitive("charge_state", charge_state)
        announcement.do_averages()
        average_voltage = announcement.influx_payload['fields']['battery_voltage']
        # Get yesterday's voltage at this time
        v_response = INFLUX_CLIENT.query(
            "select battery_voltage FROM solar_controller_readings WHERE time > {}s ORDER BY time ASC LIMIT 1".format(int(maya.when('yesterday').epoch)))
        _24h_ago_voltage = list(v_response.get_points())[0]['battery_voltage']
        voltage_diff = round(average_voltage - _24h_ago_voltage, 3)
        announcement.include_as_definitive("24h_voltage_change", voltage_diff)
        announcement.transmit()

        announcement = Announcement("solar_controller_readings", INFLUX_CLIENT, MQTT_CLIENT, mqtt_topic_prefix="solar/")
        transmit_time = time.time() + SECONDS_BETWEEN_AVERAGES

        # CHARGE_STATE
        # See if it's charge_state time.

        # TODO: Charge states are a shitshow.
        # if maya.now() > next_charge_state_check_time:
        #
        #     response = INFLUX_CLIENT.query(
        #         "select charge_state FROM charge_states ORDER BY time DESC LIMIT 1")
        #     points = response.get_points()
        #
        #     try:
        #         previous_charge_state_dict = list(points)[0]
        #         previous_charge_state = previous_charge_state_dict['charge_state']
        #     except IndexError:  # ie, we have no previous charge state.
        #         previous_charge_state = "UNKNOWN"
        #
        #     if charge_state != previous_charge_state:
        #         INFLUX_CLIENT.write_points([{"measurement": "charge_states",
        #                                      "time": datetime.datetime.utcnow(),
        #                                      "fields": {"charge_state": charge_state}}])
        #         next_charge_state_check_time = maya.now().add(minutes=10)

        now = datetime.datetime.utcnow()
        if now > next_aggregate_time:
            print("Time to aggregate!")
            zero_out = charge_state == "FLOAT"
            aggregator = EnergyAggregator()
            yesterday = maya.when("yesterday").datetime(naive=True)
            aggregator.take_readings(begin=yesterday, end=now)
            aggregator.get_amp_averages(begin=yesterday, end_minute=now)
            previous_aggregate_time = next_aggregate_time

            # Average voltage over last 24 hours.
            v_response = INFLUX_CLIENT.query(
                "select battery_voltage FROM solar_controller_readings WHERE time > {}s ORDER BY time ASC".format(
                    int(maya.when('yesterday').epoch)))
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

            # Here we used to aggregate whours since last float.  This has proven not to be terribly useful.
            # next_aggregate_time = aggregator.go(next_aggregate_time, zero_out=zero_out)
            next_aggregate_time = now + datetime.timedelta(minutes=1)
            print("Average voltage for past 24 hours is {}".format(mean_voltage))

