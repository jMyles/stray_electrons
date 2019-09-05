from pymodbus.exceptions import ConnectionException
from pymodbus.client.sync import ModbusTcpClient
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

