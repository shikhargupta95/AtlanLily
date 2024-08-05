import json
import requests

from dateutil.parser import parse
from datetime import datetime, timedelta

class AlertsService:
    '''
    Config Structure for alerts (embedded in the document from Alerts Processor Service):
    {
        "headers": <headers to be sent in the request>,
        "webhook_endpoint": <endpoint where the alerts are to be sent>,
        "retry_conf": {
            "n_retries": <number of retries after a failed delivery>,
            "max_backoff": <the maximum back-off time in seconds>
        }
    }

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
        self.__success_codes = [200, 201, 203, 204, 205, 206, 207, 208, 226]
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
        while True:
            dcts = self.__consumer_ctx.consume()
            if len(dcts) > 0:
                for dct in dcts:
                    self.__send_to_webhook(dct)



    def __send_to_webhook(self, dct):
        if self.__mode != "RETRY":
            config = dct["alert_conf"]
            payload = dct["payload"]
            headers = config["headers"]
            headers["Content-Type"] = "application/json"
            url = config["webhook_endpoint"]
            resp = requests.post(url, headers=headers, json=payload)
            if resp.status_code not in self.__success_codes:
                retry_conf = config.get("retry_conf", {})
                if retry_conf and self.__retry_flag:
                    n_retries = retry_conf.get("n_retries", 3)
                    max_backoff = retry_conf.get("max_backoff", 300)
                    exponential_backoff_factor = int(max_backoff ** (1/n_retries))
                    dct["retry_meta"] = {
                        "n_retries": n_retries,
                        "remaining_retries": n_retries,
                        "backoff_factor": exponential_backoff_factor,
                        "next_retry_time": datetime.isoformat(
                            datetime.now() + timedelta(seconds=exponential_backoff_factor))
                    }
                    self.__send_to_retry_topic(payload=dct)
        else:
            config = dct["alert_conf"]
            payload = dct["payload"]
            retry_meta = dct["retry_meta"]
            next_retry_time = retry_meta["next_retry_time"]
            if parse(next_retry_time) > datetime.now():
                self.__send_to_retry_topic(payload=dct)
                return
            else:
                headers = config["headers"]
                headers["Content-Type"] = "application/json"
                url = config["webhook_endpoint"]
                resp = requests.post(url, headers=headers, json=payload)
                if resp.status_code not in self.__success_codes:
                    remaining_retries = retry_meta["remaining_retries"] - 1
                    if remaining_retries >= 0:
                        backoff_time = retry_meta["backoff_factor"] ** ((retry_meta["n_retries"] + 1) - remaining_retries)
                        next_retry_time = datetime.isoformat(datetime.now() + timedelta(seconds=backoff_time))
                        dct["retry_meta"] = {
                            "n_retries": retry_meta["n_retries"],
                            "remaining_retries": remaining_retries,
                            "backoff_factor": retry_meta["backoff_factor"],
                            "next_retry_time": next_retry_time
                        }
                        self.__send_to_retry_topic(payload=dct)


    def __send_to_retry_topic(self, payload):
        self.__producer_ctx.produce(topic=self.__write_topic, msg_payload=payload)


