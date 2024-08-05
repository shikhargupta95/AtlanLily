import os
import json
import argparse

from core.connection_wrappers.kafka_wrapper import KafkaConsumerContext, KafkaProducerContext

from core.utils.constants import KAFKA_SEEDS
from notification_service import AlertsService

'''
CLI Params:
--user_id 
--connector_id4
--topic
--mode
--group_id

'''

def bootstrap(user_id, connector_id, topic, mode, group_id, retry_topic=None):
    consumer_ctx = KafkaConsumerContext(seeds=KAFKA_SEEDS,
                                        topic=topic,
                                        group_id=group_id)
    plugin_obj = AlertsService(user_id=user_id,
                               connector_id=connector_id,
                               consumer_ctx=consumer_ctx,
                               mode=mode,
                               topic=topic)
    plugin_obj.execute()
    consumer_ctx.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog='MonteCarloConnector',
        description='Crawl Incident Data from Monte Carlo',
    )
    parser.add_argument('--user_id', help="The user_id that triggered the connector", required=True)
    parser.add_argument('--connector_id', help="The unique identifier for the service", required=True)
    parser.add_argument("--topic", help="The topic to which the crawled records are written", required=True)
    parser.add_argument("--group_id", help="The consumer group id for the service", required=True)
    parser.add_argument("--mode", help="Mode in which the service has to run. Accepted Values: Retry/Normal", required=True)
    args = parser.parse_args()
    bootstrap(user_id=args.user_id,
              connector_id=args.connector_id,
              topic=args.topic,
              mode=args.mode,
              group_id=args.group_id)
