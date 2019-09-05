from __future__ import print_function
import maya
import time
import datetime
from pymodbus.exceptions import ConnectionException
from pymodbus.client.sync import ModbusTcpClient

from aggregate import EnergyAggregator
from push_to_datastores import Announcement

DEBUG = False
# root = logging.getLogger()
# root.setLevel(logging.DEBUG)
#
# ch = logging.StreamHandler(sys.stdout)
# ch.setLevel(logging.DEBUG)
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# ch.setFormatter(formatter)
# root.addHandler(ch)

import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient

# InfluxDB connections settings
host = "10.0.80.30"
dbname = 'energy'

INFLUX_CLIENT = InfluxDBClient(host, database=dbname)

MQTT_CLIENT = mqtt.Client()
MQTT_CLIENT.connect(host, 1883, 60)


try:
    CHARGE_CONTROLLER = ModbusTcpClient('10.0.80.101')
    CHARGE_CONTROLLER.read_holding_registers(0, 68, unit=0x01)
except ConnectionException:
    CHARGE_CONTROLLER = ModbusTcpClient('10.0.80.102')


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

transmit_time = time.time() + 2

previous_aggregate_time = datetime.datetime.utcnow()
next_aggregate_time = datetime.datetime.utcnow() + datetime.timedelta(minutes=1)
print("Will aggregate at %s" % next_aggregate_time)
next_charge_state_check_time = maya.now()

while True:
    before = time.time()
    try:
        result_reading = CHARGE_CONTROLLER.read_holding_registers(0, 68, unit=0x01)
        result_list = result_reading.registers
    except Exception as e:
        print("Encountered (%s) while reading" % (e))
        continue
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
        announcement.transmit()

        announcement = Announcement("solar_controller_readings", INFLUX_CLIENT, MQTT_CLIENT, mqtt_topic_prefix="solar/")
        transmit_time = time.time() + 2

        # CHARGE_STATE
        # See if it's charge_state time.
        if maya.now() > next_charge_state_check_time:

            response = INFLUX_CLIENT.query(
                "select charge_state FROM charge_states ORDER BY time DESC LIMIT 1")
            points = response.get_points()

            try:
                previous_charge_state_dict = list(points)[0]
                previous_charge_state = previous_charge_state_dict['charge_state']
            except IndexError:  # ie, we have no previous charge state.
                previous_charge_state = "UNKNOWN"

            if charge_state != previous_charge_state:
                INFLUX_CLIENT.write_points([{"measurement": "charge_states",
                                             "time": datetime.datetime.utcnow(),
                                             "fields": {"charge_state": charge_state}}])
                next_charge_state_check_time = maya.now().add(minutes=10)

        if datetime.datetime.utcnow() > next_aggregate_time:
            print("Time to aggregate!")
            zero_out = charge_state == "FLOAT"
            aggregator = EnergyAggregator()
            aggregator.take_readings(begin=previous_aggregate_time)
            aggregator.get_amp_averages()
            previous_aggregate_time = next_aggregate_time
            next_aggregate_time = aggregator.go(next_aggregate_time, zero_out=zero_out)

