#!/usr/bin/env python

import logging
import re
from cassandra import ConsistencyLevel, InvalidRequest, ReadTimeout, WriteTimeout
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
from cassandra.policies import RoundRobinPolicy
from core.exceptions.exceptions import CassandraContextNotInitializedException, CassandraConnectionException
from core.exceptions.exceptions import CassandraInvalidRequestException
from core.exceptions.exceptions import CassandraReadTimeoutException, CassandraWriteTimeoutException
from core.exceptions.exceptions import CassandraInvalidReadCallException, CassandraInvalidWriteCallException


class CassandraContext(object):
    """
    This class is not to be used in a Request-Response pattern (Open connection, Execute query, Close connection).
    Open one connection per application (container). Close the connection when application terminates.
    """

    def __init__(self, seeds, **options):
        try:
            auth = options.pop("auth", None)
            self.username = auth.get('username') if auth else None
            self.password = auth.get('password') if auth else None
            if auth:
                auth_provider = PlainTextAuthProvider(username=self.username,
                                                      password=self.password)
                self.__cluster = Cluster(seeds, auth_provider=auth_provider, load_balancing_policy=RoundRobinPolicy())
            else:
                self.__cluster = Cluster(seeds)
            self.__session = self.__cluster.connect()
        except Exception as e:
            raise CassandraConnectionException('Failed to open Cassandra connection: {}'.format(str(e)))

    def __del__(self):
        try:
            self.close()
        except Exception as e:
            logging.warning('Unclean closure of CassandraContext: {}'.format(str(e)))

    """
    API
    """

    def get_session(self):
        """
        Allows client to perform operations beyond exec_read and exec_write
        """
        return self.__session

    def exec_read(self, query, params=()):
        # upper is only to check
        if query[:6].upper() == 'SELECT':
            try:
                q = SimpleStatement(query, consistency_level=ConsistencyLevel.ONE)
                return self.__session.execute(q, params)
            except InvalidRequest as ire:
                raise CassandraInvalidRequestException('Invalid read request: {}'.format(str(ire)))
            except ReadTimeout as rte:
                raise CassandraReadTimeoutException('Read timeout: {}'.format(str(rte)))
            except Exception as e:
                if not self.__cluster:
                    raise CassandraContextNotInitializedException('CassandraContext is not initialized')
                else:
                    raise CassandraConnectionException('Failed to execute read request: {}'.format(str(e)))
        else:
            raise CassandraInvalidReadCallException('Please invoke exec_write method for INSERT/UPDATE/DELETE instead of exec_read.')

    def exec_write(self, query, params=(), sanitize_query=True):
        # upper is only to check
        if query[:6].upper() in {'INSERT', 'UPDATE', 'DELETE'}:
            try:
                # Added due to kprofile, litigation documents where \\ was causing issue
                # whilst using INSERT INTO {} JSON '{}'; syntax
                if sanitize_query:
                    s_query = self.__sanitize_query_string(query=query)
                else:
                    s_query = query

                q = SimpleStatement(s_query, consistency_level=ConsistencyLevel.ALL)
                return self.__session.execute(q, params)
            except InvalidRequest as ire:
                raise CassandraInvalidRequestException('Invalid write request: {}'.format(str(ire)))
            except WriteTimeout as wte:
                raise CassandraWriteTimeoutException('Write timeout: {}'.format(str(wte)))
            except Exception as e:
                if not self.__cluster:
                    raise CassandraContextNotInitializedException('CassandraContext is not initialized')
                else:
                    raise CassandraConnectionException('Failed to execute write request: {}'.format(str(e)))
        else:
            raise CassandraInvalidWriteCallException('Please invoke exec_read method for SELECT instead of exec_write.')

    def close(self):
        try:
            if self.__cluster:
                self.__cluster.shutdown()
        except Exception as e:
            raise CassandraConnectionException('Failed to close Cassandra connection: {}'.format(str(e)))
        finally:
            self.__cluster = None
            self.__session = None

    """
    ABSTRACTION
    """

    def __sanitize_query_string(self, query):
        # Replace more than one backslash with one backslash
        s_query = re.sub(r'\\+', '\\\\', query)
        return s_query
