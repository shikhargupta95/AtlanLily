import json
import time

from datetime import datetime
from pycarlo.core import Client, Query, Session

class MonteCarloConnectorException(Exception):
    pass

class MonteCarloConnector:
    '''
    Plugin to pull data from Monte Carlo and dump to a kafka topic.
    As of now monitor ingestion and usage of service conf is not implemented.


    Config Structure:

    {
        "auth_conf: {
            "mcd_id": <API KEY ID>,
            "mcd_token": <API Secret>
        },
        "service_conf": {
            "include_statuses": <In case only selected incidents are to be fetched>,
            "exclude_statuses": <In case a subset of incidents are not to be fetched>
    }
    '''

    def __init__(self, user_id, connector_id, cassandra_ctx, producer_ctx, config_table, topic):
        self.__connector_type = "monte_carlo_crawler"
        self.__connector_id = connector_id
        self.__user_id = user_id
        self.__cassandra_ctx = cassandra_ctx
        self.__producer_ctx = producer_ctx
        self.__topic = topic
        self.__config = self.__get_config_from_cassandra(
            config_table=config_table
        )
        self.__mc_client = self.__get_client()


    def __get_config_from_cassandra(self, config_table):
        query = f"SELECT JSON blobAsText(auth_conf), blobAsText(service_conf) from {config_table} where connector_id='{self.__connector_id}' and user_id='{self.__user_id} LIMIT 1"
        conf_row = self.__cassandra_ctx.exec_read(query)
        if conf_row:
            config = json.loads(conf_row[0][0])
            return config
        else:
            raise MonteCarloConnectorException("Configuration not found for the service. Please recheck input parameters")

    def execute(self):
        # Get a list of all warehouses associated with a user
        warehouses_query = '''
            query getUser {
              getUser {
                account {
                  warehouses {
                    uuid
                    id
                    createdOn
                    connectionType
                    name
                  }
                }
              }
            }
        '''
        warehouse_response = self.__mc_client(warehouses_query)
        warehouses = warehouse_response['get_user']['account']['warehouses']

        dw_incident_query = '''
            query getIncidents{
              getIncidents(
                dwId: {}
              ) {
                edges {
                  node {
                    id
                    uuid
                    title
                    tables
                    createdTime
                    type
                    subTypes
                    priority
                    status
                    project
                    dataset
                    incidentType
                  }
                },
                pageInfo {
                    endCursor
                    hasNextPage
                }
              }
            }
        '''

        dw_incident_pagination_query = '''
            query getIncidents{
              getIncidents(
                dwId: {}
                after: {}
              ) {
                edges {
                  node {
                    id
                    uuid
                    title
                    tables
                    createdTime
                    type
                    subTypes
                    priority
                    status
                    project
                    dataset
                    incidentType
                  }
                },
                pageInfo {
                    endCursor
                    hasNextPage
                }
              }
            }
        '''

        # Get incidents
        for warehouse in warehouses:
            warehouse_id = warehouse["id"]
            warehouse_uuid = warehouse["uuid"]
            # Need to check if
            incident_response = self.__mc_client(dw_incident_query.format(warehouse_uuid))
            pages = incident_response['get_incidents']['page_info']
            incidents = incident_response['get_incidents']['edges']
            while True:
                for incident in incidents:
                    incident["mc_dw_id"] = warehouse_id
                    incident["user_id"] = self.__user_id
                    incident["warehouse_info"] = warehouse
                    incident["timestamp"] = datetime.isoformat(datetime.now())
                    self.__push_to_kafka(payload = incident, doc_type="mc_incident")
                if not pages["has_next_page"]:
                    break
                incident_response = self.__mc_client(dw_incident_pagination_query.format(warehouse_uuid,
                                                                                         pages["end_cursor"]))
                pages = incident_response['get_incidents']['page_info']
                incidents = incident_response['get_incidents']['edges']


    def __get_client(self):
        auth_conf = self.__config["auth_conf"]
        mcd_id = auth_conf.get("mcd_id")
        mcd_token = auth_conf.get("mcd_token")
        if not (mcd_id and mcd_token):
            raise MonteCarloConnectorException("Bad configuration. Cannot find API keys.")
        client = Client(session=Session(mcd_id=mcd_id, mcd_token=mcd_token))
        return client

    def __push_to_kafka(self, payload, doc_type):
        message = {
            "doc_type": doc_type,
            "user_id": self.__user_id,
            "__key": f"{doc_type}:{self.__user_id}",
            "meta": {
                "producer_process_type": self.__connector_type,
                "producer_process_id": self.__connector_id,
                "timestamp": time.time()*1000
            },
            "payload": payload
        }
        self.__producer_ctx.produce(topic=self.__topic,
                                    msg_payload=message,
                                    msg_key=message["__key"])
