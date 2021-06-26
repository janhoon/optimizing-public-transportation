"""Defines trends calculations for stations"""
import logging
from abc import ABC

import faust

logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record, ABC):
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
class TransformedStation(faust.Record, ABC):
    station_id: int
    station_name: str
    order: int
    line: str


# TODO: Define a Faust Stream that ingests data from the Kafka Connect stations topic and
#   places it into a new topic with only the necessary information.
app = faust.App("stations_stream", broker="kafka://localhost:9092", store="memory://")
in_topic = app.topic("org.chicago.cta.connect.stations", key_type=str, value_type=Station)
out_topic = app.topic("org.chicago.cta.stations.table.v1", key_type=str, value_type=TransformedStation, partitions=1)
table = app.Table(
    "transformed_stations",
    default=TransformedStation,
    partitions=1,
    changelog_topic=out_topic
)


def set_station_color(station: Station) -> TransformedStation:
    color = ""
    if station.red:
        color = 'red'
    elif station.green:
        color = 'green'
    else:
        color = 'blue'

    return TransformedStation(
        station_id=station.station_id,
        station_name=station.station_name,
        order=station.order,
        line=color
    )


@app.agent(in_topic)
async def transform_stations(stations):
    stations.add_processor(set_station_color)
    async for station in stations:
        table[str(station.station_id)] = station


if __name__ == "__main__":
    app.main()
