#!/usr/bin/env python

import logging
import time
from elasticsearch import Elasticsearch

from core.exceptions.exceptions import ElasticsearchContextNotInitializedException, ElasticsearchConnectionException


class ElasticsearchContext(object):

    def __init__(self, seeds, **options):
        auth = options.pop("auth", None)
        self.username = auth.get('username') if auth else None
        self.password = auth.get('password') if auth else None
        if auth:
            options.update({"http_auth": (self.username, self.password)})
        try:
            try:
                self.__es = Elasticsearch(hosts=seeds, **options)                   # <= python3.6.x
            except Exception as e:
                self.__es = Elasticsearch(hosts='http://' + seeds[0], **options)    # >= python3.7.x
        except Exception as e:
            raise ElasticsearchConnectionException('Failed to open Elasticsearch connection: {}'.format(str(e)))

    def __del__(self):
        try:
            self.close()
        except Exception as e:
            logging.warning('Unclean closure of ElasticsearchContext: {}'.format(str(e)))

    """
    API
    """

    def get_session(self):
        return self.__es

    def create_index(self, index_name, no_of_shards, no_of_replicas, mapping_dct=None):
        try:
            settings = {
                'settings': {
                    'number_of_shards': no_of_shards,
                    'number_of_replicas': no_of_replicas
                }
            }
            if mapping_dct:
                settings['mappings'] = mapping_dct
            self.__es.indices.create(index=index_name, body=settings)
            self.__es.indices.put_mapping(index=index_name, body=settings)

            time.sleep(30)
        except Exception as e:
            if not self.__es:
                raise ElasticsearchContextNotInitializedException('ElasticsearchContext is not initialized')
            else:
                raise ElasticsearchConnectionException('Failed to create index: {}'.format(str(e)))

    def close(self):
        # Elasticsearch has no corresponding close method
        pass
