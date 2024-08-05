import os
import logging
import argparse
import traceback

from core.connection_wrappers.kafka_wrapper import KafkaConsumerContext
from core.connection_wrappers.cassandra_wrapper import CassandraContext

from core.utils.constants import KAFKA_SEEDS, CASSANDRA_SEEDS, CASSANDRA_SOURCE_CONFIG_TABLE, AUTH_VARIABLES
from monte_carlo_loader import MonteCarloLoader

'''
CLI Params:
--topic
--group_id
--target_namespace
--doc_type
'''

def bootstrap(topic, group_id, target_namespace, doc_type):
    cassandra_auth = {"username": os.environ.get(AUTH_VARIABLES["username"]),
                      "password": os.environ.get(AUTH_VARIABLES["password"])}
    cassandra_ctx = CassandraContext(CASSANDRA_SEEDS, **{"auth": cassandra_auth})

    consumer_ctx = KafkaConsumerContext(seeds=KAFKA_SEEDS,
                                        topic=topic,
                                        group_id=group_id)
    try:
        plugin_obj = MonteCarloLoader(cassandra_ctx=cassandra_ctx,
                                      consumer_ctx=consumer_ctx,
                                      target_namespace=target_namespace,
                                      doc_type=doc_type,
                                      config_table=CASSANDRA_SOURCE_CONFIG_TABLE

        )
        plugin_obj.execute()
    except Exception as e:
        logging.error(e)
        logging.error(traceback.format_exc())
    finally:
        consumer_ctx.commit()
        consumer_ctx.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog='MonteCarlo Cassandra Loader',
        description='Ingest Monte Carlo data to Cassandra',
    )
    parser.add_argument("--topic", help="The topic to which the crawled records are written", required=True)
    parser.add_argument("--group_id", help="The consumer group id for the service", required=True)
    parser.add_argument("--target_namespace", help="The cassandra namespace to which the document is to be written to",
                        required=True)
    parser.add_argument("--doc_type", help="The document type to be consumed from Kafka",
                        required=False)

    args = parser.parse_args()
    bootstrap(topic=args.topic,
              group_id=args.group_id,
              target_namespace=args.target_namespace,
              doc_type=args.doc_type)
