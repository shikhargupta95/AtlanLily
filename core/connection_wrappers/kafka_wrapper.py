#!/usr/bin/env python

import json
import logging

from confluent_kafka import Producer
from confluent_kafka import Consumer
from confluent_kafka import KafkaError

from core.exceptions.exceptions import KafkaProducerContextNotInitializedException
from core.exceptions.exceptions import KafkaConsumerContextNotInitializedException
from core.exceptions.exceptions import KafkaConnectionException, KafkaInvalidRequestException
from core.exceptions.exceptions import KafkaProducerException, KafkaConsumerException


class KafkaProducerContext(object):
    """
    REF:
    https://docs.confluent.io/4.1.2/clients/confluent-kafka-python/#producer
    https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    """

    def __init__(self,
                 seeds,
                 batch_num_messages=100,
                 linger_ms=5,
                 acks=-1,
                 max_in_flight=1,
                 retries=3,
                 message_max_bytes=16777216,
                 retry_backoff_ms=5000):

        try:
            config = {
                'bootstrap.servers': ",".join(seeds),
                'batch.num.messages': batch_num_messages,
                'linger.ms': linger_ms,
                'acks': acks,
                'max.in.flight': max_in_flight,
                'retries': retries,
                'message.max.bytes': message_max_bytes,
                'retry.backoff.ms': retry_backoff_ms
            }
            self.__producer = Producer(config)
        except Exception as e:
            raise KafkaConnectionException('Failed to open Kafka connection: {}'.format(str(e)))

    def __del__(self):
        try:
            self.close()
        except Exception as e:
            logging.warning('Unclean closure of KafkaProducerContext: {}'.format(str(e)))

    """
    API
    """

    def produce(self, topic, msg_payload, msg_key=None):

        def ack(err, msg):
            """
            Called once for each message produced to indicate delivery result. Triggered by poll() or flush().
            """
            if err is not None:
                error_message = "Error= " + str(msg.error()) + "|" + \
                                "Record= " + str(msg.value()) + "|" + \
                                "Topic= " + str(msg.topic())

                error_message = "ERROR" + ":" + "KafkaProducerException" + ":" + error_message
                raise KafkaProducerException(error_message)

        try:
            if isinstance(msg_payload, dict):
                message = json.dumps(msg_payload)
                while True:
                    try:
                        self.__producer.produce(topic=topic, value=message.encode('utf-8'), key=msg_key, callback=ack)
                        self.__producer.poll(0)
                        break
                    except BufferError as be:
                        self.__producer.poll(1)
            else:
                raise KafkaInvalidRequestException('Invalid produce request')
        except KafkaInvalidRequestException as ire:
            raise ire
        except KafkaProducerException as pe:
            raise pe
        except Exception as e:
            if not self.__producer:
                raise KafkaProducerContextNotInitializedException('KafkaProducerContext is not initialized')
            else:
                raise KafkaConnectionException('Failed to produce: {}'.format(str(e)))

    def flush(self):
        try:
            self.__producer.flush()
        except Exception as e:
            if not self.__producer:
                raise KafkaProducerContextNotInitializedException('KafkaProducerContext is not initialized')
            else:
                raise KafkaConnectionException('Failed to flush: {}'.format(str(e)))

    def close(self):
        try:
            if self.__producer:
                self.flush()
        except Exception as e:
            raise KafkaConnectionException('Failed to close Kafka connection: {}'.format(str(e)))
        finally:
            self.__producer = None


class KafkaConsumerContext(object):
    """
    REF:
    https://docs.confluent.io/4.1.2/clients/confluent-kafka-python/#consumer
    https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    """

    def __init__(self,
                 seeds,
                 topic,
                 group_id,
                 num_messages=25,
                 timeout=1,
                 auto_offset_reset='smallest',
                 is_enable_auto_commit=False,
                 max_poll_interval_ms=300000):
        try:
            config = {
                'bootstrap.servers': ",".join(seeds),
                'group.id': group_id,
                'default.topic.config': {'auto.offset.reset': auto_offset_reset},
                'enable.auto.commit': is_enable_auto_commit,

                # Should be set slightly higher than message.max.bytes setting in Kafka Broker
                'fetch.message.max.bytes': 16842752,

                # Increase this to facilitate very high latency message processing
                'max.poll.interval.ms': max_poll_interval_ms
            }

            self.__consumer = Consumer(config)
            self.__consumer.subscribe([topic])
            self.__topic = topic
            self.__num_messages = num_messages
            self.__timeout = timeout
        except Exception as e:
            raise KafkaConnectionException('Failed to open Kafka connection: {}'.format(str(e)))

    def __del__(self):
        try:
            self.close()
        except Exception as e:
            logging.warning('Unclean closure of KafkaConsumerContext: {}'.format(str(e)))

    """
    API
    """

    def get_consumer(self):
        """
        :return: Tuple (consumer, topic, num_messages, timeout)
        """
        return self.__consumer, self.__topic, self.__num_messages, self.__timeout

    def consume(self):
        try:
            dcts = []

            messages = self.__consumer.consume(self.__num_messages, self.__timeout)
            for message in messages:
                if message is None:
                    continue

                if message.error():
                    if message.error().code() != KafkaError._PARTITION_EOF:
                        raise KafkaConsumerException('Kafka Consumer exception: {}'.format(str(message.error())))
                else:
                    decoded_message = message.value().decode('utf-8')
                    dct = json.loads(decoded_message)
                    dcts.append(dct)

            return dcts
        except Exception as e:
            if not self.__consumer:
                raise KafkaConsumerContextNotInitializedException('KafkaConsumerContext is not initialized')
            else:
                raise KafkaConnectionException('Kafka consumption exception: {}'.format(str(e)))

    def commit(self, is_asynchronous_commit=False):
        try:
            self.__consumer.commit(asynchronous=is_asynchronous_commit)
        except Exception as e:
            if 'KafkaError{code=_NO_OFFSET,val=-168,str="Commit failed: Local: No offset stored"}' in str(e):
                return
            if not self.__consumer:
                raise KafkaConsumerContextNotInitializedException('KafkaConsumerContext is not initialized')
            else:
                raise KafkaConnectionException('Kafka consumer commit exception: {}'.format(str(e)))

    def close(self):
        try:
            if self.__consumer:
                self.__consumer.close()
        except Exception as e:
            raise KafkaConnectionException('Failed to close Kafka connection: {}'.format(str(e)))
        finally:
            self.__consumer = None
            self.__topic = None
            self.__num_messages = None
            self.__timeout = None


