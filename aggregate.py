import datetime
# InfluxDB connections settings
from constants import VOLTAGE
from push_to_datastores import Announcement

import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
import maya

# InfluxDB connections settings
from times import first_moment_today_utc

host = "10.0.80.30"
dbname = 'energy'

INFLUX_CLIENT = InfluxDBClient(host, database=dbname)

# MQTT_CLIENT = mqtt.Client()
# MQTT_CLIENT.connect(host, 1883, 60)
# announcement = Announcement("whours_today", INFLUX_CLIENT, MQTT_CLIENT)

DEBUG = True

MEASUREMENTS_TO_AGGREGATE = ("shunt_1", "shunt_2", "shunt_3", "penalty")  # TODO: Move to a setting or whatever


class EnergyAggregator(object):

    def __init__(self):
        self.measurement_list = []

    def most_recent_whour_count(self):
        response = INFLUX_CLIENT.query("select * FROM cumulative_whours_by_minute ORDER BY time DESC LIMIT 1")
        points = response.get_points()
        try:
            points_dict = list(points)[0]
        except IndexError:
            points_dict = {'time': maya.now().rfc2822() }
            points_dict.update({k:0 for k in MEASUREMENTS_TO_AGGREGATE})
        return points_dict

    def go(self, aggregate_time, zero_out=False):
        whours_so_far = self.most_recent_whour_count()
        most_recent_reading_dt = maya.MayaDT.from_rfc2822(whours_so_far.pop('time')).datetime(
            naive=True)

        self.aggregate_whours_for_timeframe(begin=most_recent_reading_dt, end=aggregate_time,
                                            zero_out=zero_out, commit_to_influx=True)
        return aggregate_time + datetime.timedelta(minutes=1)

    def get_earliest_reading(self):
        first_reading = min(self.readings, key=lambda r: r['time'])
        return first_reading

    def take_readings(self, begin, end=None):
        begin = begin + datetime.timedelta(seconds=1)  # Because influx requires us to strip off microseconds.
        if not end:
            end = datetime.datetime.utcnow()
        query = """SELECT * FROM "shunt_readings" WHERE time <= '%s' AND time > '%s' ORDER BY time ASC;""" % (
            end.strftime('%Y-%m-%dT%H:%M:%SZ'), begin.strftime('%Y-%m-%dT%H:%M:%SZ'))
        results = INFLUX_CLIENT.query(query)
        self.readings = list(results.get_points())

    def get_amp_averages(self, begin=None):
        """
        Get amp averages for readings taken so far, with granularity of 1 minute.

        If begin is provided, starts with begin.  Otherwise, starts with earliest reading.
        """
        if not begin:
            try:
                earliest_reading_time_str = self.get_earliest_reading()['time']
                begin_maya = maya.MayaDT.from_rfc2822(earliest_reading_time_str)
            except ValueError:
                begin_maya = maya.now()
            begin = begin_maya.datetime(naive=True)
        # Initial loop setup
        begin_minute = begin
        end_minute = begin + datetime.timedelta(minutes=1)
        amp_averages = []
        amp_readings = {}

        for result in self.readings:

            result_time = result.pop('time')
            reading_time = maya.MayaDT.from_rfc2822(result_time).datetime(naive=True)

            if (reading_time < begin_minute):
                raise ValueError("Result (%s) was outside analysis boundary (%s to %s).  What the heck?" % (
                reading_time, begin_minute, end_minute))

            if reading_time > end_minute or result == self.readings[-1]:
                # If this reading is beyond the end of the minute we're analyzing,
                # we must have finished this minute.  Or maybe it's the very last reading.
                # So, let's take an average for this minute and then move on if there's more.
                amp_averages_this_minute = {}
                if amp_readings.items():
                    for shunt, amp_counts in amp_readings.items():
                        amp_averages_this_minute[shunt] = sum(amp_counts) / max(len(amp_counts), 1)

                    amp_averages.append((begin_minute, amp_averages_this_minute))
                else:
                    pass # We didn't have any readings this minute.  No choice but to move on.  # TODO: Try this reading on a future minute

                # Cool, now let's adjust out minutes and keep going.
                begin_minute = end_minute
                end_minute = begin_minute + datetime.timedelta(minutes=1)
                amp_readings = {}

            for shunt, current in result.items():

                if not shunt in amp_readings.keys():
                    amp_readings[shunt] = []
                amp_readings[shunt].append(current)

        self.amp_averages = amp_averages


    def get_most_recent_aggregation(self, before_dt):
        """
        Get last aggregation prior to before_dt
        """
        query = "SELECT * FROM cumulative_whours_by_minute WHERE time >= '%s' ORDER BY time DESC LIMIT 1" % before_dt.strftime(
            '%Y-%m-%dT%H:%M:%SZ')
        result = INFLUX_CLIENT.query(query)
        try:
            most_recent_aggregation = list(result.get_points())[0]
        except IndexError:
            most_recent_aggregation = {'time': maya.now().rfc2822() }
            most_recent_aggregation.update({k:0 for k in MEASUREMENTS_TO_AGGREGATE})
        return most_recent_aggregation


    def aggregate_whours_for_timeframe(self, end, begin=None, zero_out=False, commit_to_influx=False):
        end = end.replace(second=0, microsecond=0)
        self.total_whours = []

        # For the first time through, establish zeroes for previous minute.
        total_whours_as_of_previous_minute = {}
        if zero_out or begin == first_moment_today_utc():
            print("Zeroing out all shunts.")
            for shunt in MEASUREMENTS_TO_AGGREGATE:
                total_whours_as_of_previous_minute[shunt] = 0
        else:
            begin = begin.replace(second=0, microsecond=0)
            most_recent_aggregation = self.get_most_recent_aggregation(begin)
            dt_of_most_recent_aggregation = maya.MayaDT.from_rfc2822(most_recent_aggregation.pop('time')).datetime(naive=True)

            for shunt in MEASUREMENTS_TO_AGGREGATE:
                total_whours_as_of_previous_minute[shunt] = most_recent_aggregation.get(shunt) or 0.0

        if commit_to_influx:
            self.measurement_list = []

        for dt, amp_average in self.amp_averages:
            whours_of_this_reading = {}
            cumm_whours_by_minute = {}
            for shunt, reading in amp_average.items():
                watts_now = float(reading * VOLTAGE)
                whours_of_this_reading[shunt] = watts_now
                cumm_whours_by_minute[shunt] = total_whours_as_of_previous_minute[shunt] + watts_now / 60

            if commit_to_influx:
                self.append_measurement("watt_averages_by_minute", dt, whours_of_this_reading)
                self.append_measurement("cumulative_whours_by_minute", dt, cumm_whours_by_minute)

        if commit_to_influx:
            self.push_to_influx()

    def aggregate_whours_today(self, commit_to_influx=False):
        return self.aggregate_whours_for_timeframe(first_moment_today_utc(),
                                               datetime.datetime.utcnow(),
                                               commit_to_influx=commit_to_influx)

    def append_measurement(self, measurement, dt, fields):
        self.measurement_list.append({"measurement": measurement,
                                 "time": dt,
                                 "fields": fields,
                                 })

    def push_to_influx(self):
        INFLUX_CLIENT.write_points(self.measurement_list)

        print("Pushing aggregation: %s" % self.measurement_list)

    def zero_out(self, dt=None):
        if dt is None:
            dt = datetime.datetime.utcnow()
        zero_whours = {}
        for shunt in MEASUREMENTS_TO_AGGREGATE:
            zero_whours[shunt] = 0.0
        self.append_measurement("cumulative_whours_by_minute", dt, zero_whours)