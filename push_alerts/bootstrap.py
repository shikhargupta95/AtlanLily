import os
import logging
import argparse
import traceback

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
--retry_topic
'''

def bootstrap(user_id, connector_id, topic, mode, group_id, retry_topic=None):
    consumer_ctx = KafkaConsumerContext(seeds=KAFKA_SEEDS,
                                        topic=topic,
                                        group_id=group_id)
    if mode.upper() == "RETRY":
        retry_topic = topic

    if not retry_topic:
        producer_ctx = None
    else:
        producer_ctx = KafkaProducerContext(seeds=KAFKA_SEEDS)

    try:
        plugin_obj = AlertsService(user_id=user_id,
                                   connector_id=connector_id,
                                   consumer_ctx=consumer_ctx,
                                   producer_ctx=producer_ctx,
                                   mode=mode,
                                   topic=topic)
        plugin_obj.execute()
    except Exception as e:
        logging.error(e)
        logging.error(traceback.format_exc())
    finally:
        if producer_ctx:
            producer_ctx.flush()
            producer_ctx.close()
        consumer_ctx.commit()
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
    parser.add_argument("--mode", help="Mode in which the service has to run. Accepted Values: Retry/Normal",
                        required=True)
    parser.add_argument("--retry_topic", help="The topic to which the records to be retried are to be written",
                        required=False)

    args = parser.parse_args()
    bootstrap(user_id=args.user_id,
              connector_id=args.connector_id,
              topic=args.topic,
              mode=args.mode,
              group_id=args.group_id,
              retry_topic=args.retry_topic)
