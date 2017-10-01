import datetime
from aggregate import EnergyAggregator

aggregator = EnergyAggregator()
aggregator.zero_out()
aggregator.push_to_influx()