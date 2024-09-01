"""Configures KSQL to combine station and turnstile data"""
import json
import logging

import requests

import topic_check


logger = logging.getLogger(__name__)


KSQL_URL = "http://localhost:8088"


# For the following KSQL statements:
# For the first statement, create a `turnstile` table from your turnstile topic.
# For the second statment, create a `turnstile_summary` table by selecting from the `turnstile` table and grouping on station_id.

KSQL_STATEMENT = """
CREATE TABLE turnstile (
    station_id int,
    station_name varchar,
    line varchar
) WITH (
    kafka_topic = 'turnstile',
    value_format = 'avro',
    key = 'station_id'
);

CREATE TABLE turnstile_summary
    WITH (
        kafka_topic = 'turnstile_summary',
        value_format = 'json') AS
    SELECT station_id, COUNT(*) AS count
    FROM turnstile
    GROUP BY station_id;
"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists("turnstile_summary") is True:
        return

    resp = requests.post(
        f"{KSQL_URL}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
            }
        ),
    )

    try:
        resp.raise_for_status()
        logging.info("executing ksql statement...")
    except Exception as e:
        logging.error(f"Failed to execute ksql statement - error: {e}")


if __name__ == "__main__":
    execute_statement()
