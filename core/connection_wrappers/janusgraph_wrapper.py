#!/usr/bin/env python

import json
import logging
import random
import requests
import socket

from core.exceptions.exceptions import JanusGraphNoHostAvailableException, JanusGraphConnectionException
from core.exceptions.exceptions import JanusGraphRequestTimeoutException, JanusGraphResponseException



class JanusGraphContext(object):
    """
    This class is not to be used in a Request-Response pattern (Open connection, Execute query, Close connection).
    Open one connection per application (container). Close the connection when application terminates.
    """

    def __init__(self, seeds, **options):
        try:
            # Creds
            auth = options.pop("auth", None)
            self.username = auth.get('username') if auth else None
            self.password = auth.get('password') if auth else None
            self.auth_header = None
            if auth:
                self.auth_header = requests.auth.HTTPBasicAuth(self.username, self.password)
            # Available servers
            self.__servers = set(seeds)

            # Not Available servers
            self.__na_servers = set()

            self.__servers, self.__na_servers, self.__curr_server = \
                self.__select_server(servers=self.__servers, na_servers=self.__na_servers)

            self.__url = 'http://{}:8182'.format(self.__curr_server)
        except JanusGraphNoHostAvailableException as hue:
            raise hue
        except Exception as e:
            raise JanusGraphConnectionException('Failed to open JanusGraph Connection: {}'.format(str(e)))

    def __del__(self):
        try:
            self.close()
        except Exception as e:
            logging.warning('Unclean closure of JanusGraphContext: {}'.format(str(e)))

    """
    API
    """

    def execute(self, query):
        """
        :param query: Type dict which contains 'gremlin' query string, and 'bindings' dict
        :return:
        """
        try:
            if self.auth_header:
                response = requests.post(self.__url, data=json.dumps(query),
                                         headers={'content-type': 'application/json'}, auth=self.auth_header)
            else:
                response = requests.post(self.__url, data=json.dumps(query), headers={'content-type': 'application/json'})
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as he:
            # TODO: what if above fails and response line did not execute properly and we do not have response initialized there
            response_content = json.loads(response.content)
            raise JanusGraphResponseException('Response exception: {}. Exception-class: {}. JG-Message: {}'.format(
                str(he), str(response_content['Exception-Class']), str(response_content['message'])))
        except requests.exceptions.ConnectionError as ce:
            self.__servers.remove(self.__curr_server)
            self.__na_servers.add(self.__curr_server)
            self.__servers, self.__na_servers, self.__curr_server = \
                self.__select_server(servers=self.__servers, na_servers=self.__na_servers)
            raise JanusGraphConnectionException('Failed to execute query: {}'.format(str(ce)))
        except requests.exceptions.Timeout as te:
            raise JanusGraphRequestTimeoutException('JanusGraph request timed out: {}'.format(str(te)))

    def close(self):
        self.__servers = None
        self.__na_servers = None
        self.__curr_server = None
        self.__url = None

    """
    Abstraction
    """

    def __select_server(self, servers, na_servers):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        while servers:
            server = random.sample(servers, 1)[0]
            result = sock.connect_ex((server, 8182))
            if result == 0:
                return servers, na_servers, server
            else:
                servers.remove(server)
                na_servers.add(server)

        raise JanusGraphNoHostAvailableException('No JanusGraph hosts available')
