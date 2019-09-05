DEBUG = False
import datetime


class Announcement(object):

    def __init__(self, measurement, influx_client, mqtt_client, timestamp=None, tags=None, mqtt_topic_prefix=None, *args, **kwargs):
        self.measurement = measurement
        self.influx_client = influx_client
        self.mqtt_client = mqtt_client
        self.timestamp = timestamp or datetime.datetime.utcnow()
        self.mqtt_topic_prefix = mqtt_topic_prefix or ""
        self.averages = {}
        self.mqtt_payload = {}
        self.tags = tags or {}
        self.influx_payload = {"measurement": self.measurement,
                               "time": self.timestamp,
                               "tags": self.tags,
                               "fields": {}
                               }

    def include_as_definitive(self, topic, content):
        self.influx_payload['fields'][topic] = content
        self.mqtt_payload[topic] = content

    def include_for_average(self, topic, content):
        if not topic in self.averages.keys():
            self.averages[topic] = []

        self.averages[topic].append(float(content))

    def do_averages(self):
        for topic, data_points in self.averages.items():
            if topic in self.mqtt_payload.keys():
                raise ValueError("%s included both as definitive and average.  Not sure what to do about that." % topic)
            else:
                average_for_this_data_point = sum(data_points) / max(len(data_points), 1)
                self.include_as_definitive(topic, round(average_for_this_data_point, 3))

    def transmit(self):

        if DEBUG:
            print("Averaging %s data points" % len(self.averages.values()[0]))
        self.influx_client.write_points([self.influx_payload])
        for topic, content in self.mqtt_payload.items():
            self.mqtt_client.publish(self.mqtt_topic_prefix + topic, content)