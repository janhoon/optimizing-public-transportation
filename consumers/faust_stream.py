"""Defines trends calculations for stations"""
import logging
from dataclasses import dataclass

import faust

logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
@dataclass
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
@dataclass
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


# TODO: Define a Faust Stream that ingests data from the Kafka Connect stations topic and
#   places it into a new topic with only the necessary information.
app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
topic = app.topic("connect_jdbc_stations", value_type=Station)
out_topic = app.topic("station", value_type=TransformedStation, partitions=1)
table = app.Table(
    "transformed_stations",
    default=TransformedStation,
    partitions=1,
    changelog_topic=out_topic
)


def set_station_color(station: Station):
    color = ""
    if station.red:
        color = 'red'
    elif station.green:
        color = 'green'
    else:
        color = 'blue'

    ts = TransformedStation(
        station_id=station.station_id,
        station_name=station.station_name,
        order=station.order,
        line=color
    )

    return ts


@app.agent(topic)
async def transform_stations(stations: [Station]):
    stations.add_processor(set_station_color)
    async for station in stations:
        logger.info(station)
        table[station.station_id] = station


if __name__ == "__main__":
    app.main()
