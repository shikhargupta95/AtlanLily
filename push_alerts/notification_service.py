import json
import requests


class AlertsService:
    '''
    Config Structure for alerts (embedded in the document from Alerts Processor Service):
    [{
        "headers": <headers to be sent in the request>,
        "webhook_endpoint": <endpoint where the alerts are to be sent>,
        "retries": {
            "n_retries": <number of retries after a failed delivery>,
            "max_backoff": <the maximum back-off time in seconds>
        }
    }]

    Alert Structure


    '''


    def __init__(self, user_id, connector_id, consumer_ctx, producer_ctx, mode, topic, retry_topic=None):
        self.__user_id = user_id
        self.__connector_id = connector_id
        self.__consumer_ctx = consumer_ctx
        self.__producer_ctx = producer_ctx
        self.__mode = mode
        self.__read_topic = topic
        self.__retry_flag = None
        if mode.upper == "RETRY":
            self.__write_topic = topic
            self.__retry_flag = True
        elif retry_topic:
            self.__retry_flag = True
            self.__write_topic = retry_topic
        else:
            self.__retry_flag = False
            self.__write_topic = None


    def execute(self):
        if self.__mode != "RETRY":
            while True:
                dcts = self.__consumer_ctx.consume()
                if len(dcts) > 0:
                    for dct in dcts:
                        confs = dct["alert_conf"]
                        payload = dct["payload"]
                        for conf in confs:
                            self.__send_to_webhook(conf, payload)

    def __send_to_webhook(self, config, payload):
        pass

    def __send_to_retry_topic(self, payload):
        pass


