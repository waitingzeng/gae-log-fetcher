#!/usr/bin/env python
#coding=utf8

import time

import logging

import simplejson as json


from elasticsearch import Elasticsearch
from elasticsearch import helpers

import socket
socket.setdefaulttimeout(30)
import redis


logger = logging.getLogger()


class FetchLog(object):

    def __init__(self):
        self.es = Elasticsearch()

    def send_to_es(self, index_name, lines, **kwargs):
        actions = []
        for msg in lines:
            action = {
                "_index": "logstash-%s" % index_name,
                "_type": msg['@type'],
                #"_version": "1",
                "_source": msg
            }

            actions.append(action)
        logging.info("save to es[index_name: %s, type: %s, actions: %s]", action['_index'], action['_type'], len(actions))

        helpers.bulk(self.es, actions, chunk_size=100, params={'request_timeout': 90})

    def run(self):
        lines = []
        self._redis = redis.StrictRedis(host='127.0.0.1', socket_timeout=10)
        while True:
            try:
                log_data = self._redis.rpop('ec2_easilydo_log')
                if not log_data:
                    logging.info("not log in queue, sleep 1")
                    time.sleep(1)
                    continue

                log_data = json.loads(log_data)

                lines.append(log_data)

                if lines and len(lines) >= 100:
                    index_name = log_data['@fields']['date'].replace('-', '.')
                    self.send_to_es(index_name, lines)
                    lines = []

            except Exception as e:
                logging.error("pop log fail %s", e, exc_info=True)
        return ""


if __name__ == '__main__':
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # other run time options

    fetch_app = FetchLog()
    fetch_app.run()
