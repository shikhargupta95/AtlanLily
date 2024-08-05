import os
import argparse

from core.connection_wrappers.cassandra_wrapper import CassandraContext
from core.connection_wrappers.kafka_wrapper import KafkaProducerContext

from plugins.fetch_from_monte_carlo import MonteCarloConnector
from core.utils.constants import CASSANDRA_SEEDS, KAFKA_SEEDS, CONNECTOR_CONFIG_TABLE, AUTH_VARIABLES


'''
CLI Params:
--user_id 
--connector_id4
--topic

'''

def bootstrap(user_id, connector_id, topic):
    cassandra_auth = {"username": os.environ.get(AUTH_VARIABLES["username"]),
                      "password": os.environ.get(AUTH_VARIABLES["password"])}
    cassandra_ctx = CassandraContext(CASSANDRA_SEEDS, **{"auth": cassandra_auth})
    producer_ctx = KafkaProducerContext(KAFKA_SEEDS)
    plugin_obj = MonteCarloConnector(user_id=user_id,
                                     connector_id=connector_id,
                                     cassandra_ctx=cassandra_ctx,
                                     producer_ctx=producer_ctx,
                                     config_table=CONNECTOR_CONFIG_TABLE,
                                     topic=topic)
    plugin_obj.execute()
    producer_ctx.close()
    cassandra_ctx.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog='MonteCarloConnector',
        description='Crawl Incident Data from Monte Carlo',
    )
    parser.add_argument('--user_id', help="The user_id that triggered the connector", required=True)
    parser.add_argument('--connector_id', help="The unique identifier for the service", required=True)
    parser.add_argument("--topic", help="The topic to which the crawled records are written", required=True)

    args = parser.parse_args()
    bootstrap(user_id=args.user_id,
              connector_id=args.connector_id,
              topic=args.topic)
