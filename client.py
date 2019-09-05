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


possible_controller_addresses = ('10.0.80.101', '10.0.80.102')

for address in possible_controller_addresses:
    try:
        CHARGE_CONTROLLER = ModbusTcpClient(address)
        reading = CHARGE_CONTROLLER.read_holding_registers(0, 68, unit=0x01)
        assert reading.registers
        print("Found controller at {}".format(address))
        CHARGE_CONTROLLER.close()
        break
    except (ConnectionException, AttributeError) as e:
        print("No controller at {}, got {}".format(address, e))
        continue
else:
    raise ConnectionError("Can't find a controller.")


