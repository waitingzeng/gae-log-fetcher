#!/usr/bin/env python
#coding=utf8
import sys
from dateutil import tz
from datetime import datetime

start = int(sys.argv[1])
num = int(sys.argv[2])
GAE_TZ = tz.gettz('US/Pacific')


for i in range(num):
	real_start = (start + i * 3600) / 3600 * 3600
	start_human = datetime.fromtimestamp(real_start, tz=GAE_TZ)

	cmd = """export PYTHONPATH="/home/gae_log_fetcher/google_appengine:/home/gae_log_fetcher/google_appengine/lib/fancy_urllib";cd /home/gae_log_fetcher/current;  nohup /usr/bin/python fetcher.py --gae_config /home/gae_log_fetcher/current/fetcher.conf.agent8-backend --save_to_file /mnt/gae_logs --start_timestamp %s --end_timestamp %s > /mnt/gae_log_fetcher/%s &""" % (real_start, start + 3600, start_human.strftime('%Y-%m-%d-%H'))
	print cmd