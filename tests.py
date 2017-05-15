import unittest

from aggregate import EnergyAggregator
from test_data import fake_shunt_readings_3_minutes, \
    fake_shunt_readings_average_of_3_negative_5_and_7, fake_averages


class Aggregationtessts(unittest.TestCase):

    def test_basic_aggregation(self):
        aggregator = EnergyAggregator()
        aggregator.readings = fake_shunt_readings_average_of_3_negative_5_and_7
        averages = aggregator.get_amp_averages()
        self.assertEqual(aggregator.amp_averages[0][1]['shunt_1'], 3)
        self.assertEqual(aggregator.amp_averages[0][1]['shunt_2'], -5)
        self.assertEqual(aggregator.amp_averages[0][1]['shunt_3'], 7)

    def test_whours_calculation(self):
        aggregator = EnergyAggregator()
        aggregator.amp_averages = fake_averages
        last_dt = max(aggregator.amp_averages, key=lambda a: a[0])[0]
        aggregator.aggregate_whours_for_timeframe(end=last_dt, zero_out=True)
        final_whours = aggregator.total_whours[-1]
        self.assertEqual(final_whours[1]["shunt_1"], -1.0)
        self.assertEqual(final_whours[1]["shunt_2"], -5.0)
        self.assertEqual(final_whours[1]["shunt_3"], 6.0)


class TimeSeriesTests(unittest.TestCase):

    def test_commit_to_influx(self):
        aggregator = EnergyAggregator()
        aggregator.amp_averages = fake_averages
        last_dt = max(aggregator.amp_averages, key=lambda a: a[0])[0]
        aggregator.aggregate_whours_for_timeframe(end=last_dt, zero_out=True, commit_to_influx=True)
