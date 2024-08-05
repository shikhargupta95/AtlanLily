import json


'''
This is a simple cassandra loader which just pushes through the data to cassandra blindly.
This can be better optimized by using an abstract class that defines a wireframe structure of cassandra loaders and
does the important transformations that might be needed.
'''

class MonteCarloLoaderException(Exception):
    pass


class MonteCarloLoader:

    def __init__(self, cassandra_ctx, consumer_ctx, target_namespace, doc_type, config_table):
        self.__cassandra_ctx = cassandra_ctx
        self.__consumer_ctx = consumer_ctx
        self.__namespace = target_namespace
        self.__doc_type = doc_type
        self.__namespace_conf = self.__get_config_from_cassandra(config_table)

    def execute(self):
        while True:
            dcts = self.__consumer_ctx.consume()
            if len(dcts) > 0:
                for dct in dcts:
                    if dct["doc_type"] == self.__doc_type:
                        self.__push_to_cassandra(dct)

    def __get_config_from_cassandra(self, config_table):
        query = f"SELECT JSON keyspace, table_name, partition_keys, clustering_keys, version_keys from {config_table} where source_id = '{self.__namespace}' LIMIT 1"
        conf_row = self.__cassandra_ctx.exec_read(query)
        if conf_row:
            config = json.loads(conf_row[0][0])
            return config
        else:
            raise MonteCarloLoaderException(
                "Configuration not found for the service. Please recheck input parameters")

    def __push_to_cassandra(self, doc):
        keyspace = self.__namespace_conf["keyspace"]
        table = self.__namespace_conf["table_name"]
        full_table_name = f"{keyspace}.{table}"
        insert_query = "INSERT INTO {} JSON {}".format(full_table_name, json.dumps(doc))
