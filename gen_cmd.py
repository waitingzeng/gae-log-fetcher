#!/usr/bin/env python
#coding=utf8
import sys
from dateutil import tz
from datetime import datetime

start = int(sys.argv[1])
num = int(sys.argv[2])
GAE_TZ = tz.gettz('US/Pacific')


print """export PYTHONPATH="/home/gae_log_fetcher/google_appengine:/home/gae_log_fetcher/google_appengine/lib/fancy_urllib";cd /home/gae_log_fetcher/current;  """
for i in range(num):
	real_start = (start + i * 3600) / 3600 * 3600
	start_human = datetime.fromtimestamp(real_start, tz=GAE_TZ)

	cmd = """/usr/bin/python fetcher.py --gae_config fetcher.conf.agent8-backend --save_to_file /mnt/gae_logs --start_timestamp %s --end_timestamp %s --send_to_es > /mnt/gae_log_fetcher/%s""" % (real_start, real_start + 3600, start_human.strftime('%Y-%m-%d-%H'))
	print cmd