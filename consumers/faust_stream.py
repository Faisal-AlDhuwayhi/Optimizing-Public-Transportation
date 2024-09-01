"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
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
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


# Define a Faust Stream that ingests data from the Kafka Connect stations topic and
#   places it into a new topic with only the necessary information.
app = faust.App("stations-stream-process", broker="kafka://localhost:9092", store="memory://")

# Define the input Kafka Topic
in_topic = app.topic("org.chicago.cta.conn-stations", value_type=Station)

# Define the output Kafka Topic
out_topic = app.topic("org.chicago.cta.conn-stations.transformed", partitions=1, value_type=TransformedStation)

# Define a Faust Table
table = app.Table(
   "stations.transformed.table",
   default=int,
   partitions=1,
   changelog_topic=out_topic,
)

# transform input `Station` records into `TransformedStation` records
@app.agent(in_topic)
async def transform_station(stations):
    async for station in stations:
        line = 'red' if station.red else 'blue' if station.blue else 'green' if station.green else None
        
        transformed_station = TransformedStation(
            station_id=station.station_id,
            station_name=station.station_name,
            order=station.order,
            line=line
        )
        
        table[station.station_id] = transformed_station  
        logging.info(f"Transform Station with station_id ({transformed_station.station_id})")


if __name__ == "__main__":
    app.main()
