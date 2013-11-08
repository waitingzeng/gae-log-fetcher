# -*- coding: utf-8 -*-
import datetime
import json
import redis
import traceback
import time
import urlparse
import random


class TransportException(Exception):
    pass

class BaseTransport(object):

    def __init__(self, hostname, format=None, logger=None):
        """Generic transport configuration
        Will attach the file_config object, setup the
        current hostname, and ensure we have a proper
        formatter for the current transport
        """
        self._current_host = hostname
        self._default_formatter = format
        self._formatters = {}
        self._is_valid = True
        self._logger = logger

        def raw_formatter(data):
            return data['@message']

        def rawjson_formatter(data):
            json_data = json.loads(data['@message'])
            del data['@message']

            for field in json_data:
                data[field] = json_data[field]

            for field in ['@message', '@source', '@source_host', '@source_path', '@tags', '@timestamp', '@type']:
                if field not in data:
                    data[field] = ''

            return json.dumps(data)

        def string_formatter(data):
            return '[{0}] [{1}] {2}'.format(data['@source_host'], data['@timestamp'], data['@message'])

        self._formatters['json'] = json.dumps
        self._formatters['raw'] = raw_formatter
        self._formatters['rawjson'] = rawjson_formatter
        self._formatters['string'] = string_formatter

  
    def callback(self, filename, lines):
        """Processes a set of lines for a filename"""
        return True

    def format(self, filename, line, timestamp, **kwargs):
        """Returns a formatted log line"""
        formatter = kwargs.pop('format', self._default_formatter)
        if formatter not in self._formatters:
            formatter = self._default_formatter

        return self._formatters[formatter]({
            '@source': 'file://{0}{1}'.format(self._current_host, filename),
            '@type': kwargs.get('type'),
            '@tags': kwargs.get('tags'),
            '@fields': kwargs.get('fields'),
            '@timestamp': timestamp,
            '@source_host': self._current_host,
            '@source_path': filename,
            '@message': line,
        })

    def get_timestamp(self, **kwargs):
        """Retrieves the timestamp for a given set of data"""
        timestamp = kwargs.get('timestamp')
        if not timestamp:
            timestamp = datetime.datetime.utcnow().isoformat() + 'Z'

        return timestamp

    def interrupt(self):
        """Allows keyboard interrupts to be
        handled properly by the transport
        """
        return True

    def invalidate(self):
        """Invalidates the current transport"""
        self._is_valid = False

    def reconnect(self):
        """Allows reconnection from when a handled
        TransportException is thrown"""
        return True

    def unhandled(self):
        """Allows unhandled exceptions to be
        handled properly by the transport
        """
        return True

    def valid(self):
        """Returns whether or not the transport can send data"""
        return self._is_valid


class RedisTransport(BaseTransport):

    def __init__(self, redis_namespace, redis_url, hostname, format=None, logger=None):
        super(RedisTransport, self).__init__(hostname, format, logger=logger)

        _url = urlparse.urlparse(redis_url, scheme='redis')
        self._redis = redis.StrictRedis(host=_url.hostname, port=_url.port, socket_timeout=10)
        self._redis_namespace = redis_namespace
        self._is_valid = False

        self._connect()

    def _connect(self):
        wait = -1
        while True:
            wait += 1
            time.sleep(wait)
            if wait == 20:
                return False

            if wait > 0:
                self._logger.info("Retrying connection, attempt {0}".format(wait + 1))

            try:
                self._redis.ping()
                break
            except UserWarning:
                traceback.print_exc()
            except Exception:
                traceback.print_exc()

        self._is_valid = True
        self._pipeline = self._redis.pipeline(transaction=False)

    def reconnect(self):
        self._connect()

    def invalidate(self):
        """Invalidates the current transport"""
        super(RedisTransport, self).invalidate()
        self._redis.connection_pool.disconnect()
        return False

    def callback(self, filename, lines, **kwargs):
        timestamp = self.get_timestamp(**kwargs)
        if kwargs.get('timestamp', False):
            del kwargs['timestamp']

        for line in lines:
            self._pipeline.rpush(
                self._redis_namespace,
                self.format(filename, line, timestamp, **kwargs)
            )

        try:
            self._pipeline.execute()
        except redis.exceptions.ConnectionError, e:
            traceback.print_exc()
            raise TransportException(str(e))


class RedisTransports(object):
    def __init__(self, redis_namespace, redis_urls, hostname, format=None, logger=None):
        self._trans = []
        for redis_url in redis_urls:
            self._trans.append(RedisTransport(redis_namespace, redis_url, hostname, format, logger))

    def callback(self, *args, **kwargs):
        trans = random.choice(self._trans)

        return trans.callback(*args, **kwargs)
