import signal
import sys
import time

import logging
import ConfigParser

from dateutil import tz
from datetime import datetime
from datetime import timedelta

from google.appengine.ext.remote_api import remote_api_stub
from google.appengine.api.logservice import logservice
from google.appengine.ext.remote_api.remote_api_stub import ConfigurationError

import getpass
import simplejson as json
import base64

import argparse
from redis_transport import RedisTransports


RECOVERY_LOG = '/tmp/recovery.log'

# end_time is 3 mins before now
PERIOD_END_NOW = 60 * 3

# period length
PERIOD_LENGTH = 60

GAE_TZ = tz.gettz('US/Pacific')

logger = logging.getLogger()

last_offset = None
last_time_period = None

ENCODING = "ISO-8859-1"


def _get_level(level):
    # TODO - better?
    if logservice.LOG_LEVEL_DEBUG == level:
        return "DEBUG"
    if logservice.LOG_LEVEL_INFO == level:
        return "INFO"
    if logservice.LOG_LEVEL_WARNING == level:
        return "WARNING"
    if logservice.LOG_LEVEL_ERROR == level:
        return "ERROR"
    if logservice.LOG_LEVEL_CRITICAL == level:
        return "CRITICAL"

    return "UNKNOWN"


def get_time_period(start_timestamp):
    end = start_timestamp - PERIOD_END_NOW
    start = end - PERIOD_LENGTH

    start_human = datetime.fromtimestamp(start, tz=GAE_TZ)
    end_human = datetime.fromtimestamp(end, tz=GAE_TZ)

    return {'start': start, 'end': end, 'start_human': start_human, 'end_human': end_human}


def _split_time_period(start, end, interval_s=10):
    """
        Splits given time_period in segments based on interval
        and returns a list of tuples [(start,end),...]

        Uses seconds since epoch
    """
    r = range(start, end, interval_s)
    segments = []
    for s in r:
        e = s + interval_s
        if e > end:
            e = end
        segment = (s, e)
        segments.append(segment)

    logger.debug("Splitted %s:%s into %d segments - %s" %
                 (start, end, len(segments), segments))

    return segments


class GAEFetchLog(object):

    def __init__(self, username, password, app_name, redis_namespace, redis_urls):
        self.username = username
        self.password = password
        self.app_name = app_name
        self.redis_urls = redis_urls
        self.redis_namespace = redis_namespace
        self.version_ids = ['1']
        self.redis_transports = RedisTransports(redis_namespace,  self.redis_urls, hostname='%s.appspot.com' % app_name, format='raw', logger=logger)

    def _prepare_json(self, req_log):
        """Prepare JSON in logstash json_event format"""
        data = {'fields': {}}
        data['type'] = '%s-gae-test' % self.app_name
        data['tags'] = ['gae']
        data['fields']['response'] = req_log.status
        data['fields']['latency_ms'] = req_log.latency
        
        # Timestamp - this helps if events are not coming in chronological
        # order
        t = datetime.fromtimestamp(req_log.end_time)
        t = t.replace(tzinfo=GAE_TZ)
        data['timestamp'] = t.isoformat()

        # processing APP Logs
        msg = req_log.combined
        if len(req_log.app_logs) > 0:
            app_log_msgs = []
            for app_log in req_log.app_logs:
                t = datetime.fromtimestamp(app_log.time)
                t = t.replace(tzinfo=GAE_TZ)
                l = _get_level(app_log.level)
                app_log_msgs.append("%s %s %s"
                                    % (t.isoformat(), l, app_log.message))

            # The new lines give it more readability in Kibana
            msg = msg + "\n\n" + "\n".join(app_log_msgs)

        data['line'] = msg

        return data

    def fetch_logs(self, time_period):
        f = lambda: (self.username, self.password)

        try:
            remote_api_stub.ConfigureRemoteApi(
                None, '/remote_api', f, self.app_name + '.appspot.com')
        except ConfigurationError:
            # Token expired?
            logger.exception(
                "Token validation failed. Probably expired. Will retry")
            remote_api_stub.ConfigureRemoteApi(
                None, '/remote_api', f, app_name)

        logger.info("Successfully authenticated")

        logger.info("Fetching logs from %s to %s (GAE TZ)"
                    % (time_period['start_human'], time_period['end_human']))

        end = time_period['end']
        start = time_period['start']
        start_human = time_period['start_human']

        intervals = _split_time_period(start, end)

        i = 0
        dest = '%s-%s.log' % (app_name, start_human.strftime('%Y-%m-%d'))

        try:
            for interval in intervals:
                start, end = interval
                logger.info("Interval : %s - %s" % (start, end))

                lines = []
                offset = None
                for req_log in logservice.fetch(end_time=end,
                                                start_time=start,
                                                minimum_log_level=logservice.LOG_LEVEL_INFO,
                                                version_ids=self.version_ids,
                                                include_app_logs=True, include_incomplete=True,
                                                offset=offset):

                    logger.debug("Retrieved - %s" % req_log.combined)

                    i = i + 1
                    if i % 100 == 0:
                        logger.info("Fetched %d req logs so far" % i)

                    lines.append(self._prepare_json(req_log))

                    offset = req_log.offset
                    # end fetch
                if lines:
                    logger.info("Save to redis %s", len(lines))
                    self.redis_transports.callback(dest, lines)
                # end interval
        except:
            logger.exception("Something went wrong")

        logger.info("Retrieved %d logs. Done." % i)

        return ""


if __name__ == '__main__':
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # other run time options
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_timestamp",
                        help="default is now")

    parser.add_argument("--gae_config",
                        help="Config file for GAE user, pass, app. If not specified, it looks for fetcher.conf")

    args = parser.parse_args()

    #logger.setLevel(logging.DEBUG)

    # getting app name & credentials from a file
    conf = args.gae_config or 'fetcher.conf'

    config = ConfigParser.SafeConfigParser()
    config.read(conf)

    username = config.get('GAE', 'username')
    password = config.get('GAE', 'password')
    app_name = config.get('GAE', 'app_name')
    redis_urls = config.get('REDIS', 'redis_urls')
    redis_namespace = config.get('REDIS', 'namespace')

    redis_urls = redis_urls.split(',')
    start_timestamp = args.start_timestamp and int(args.start_timestamp) or int(time.time())

    gae_fetch_app = GAEFetchLog(username, password, app_name, redis_namespace, redis_urls)
    gae_fetch_app.fetch_logs(get_time_period(start_timestamp))
