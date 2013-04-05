#!/usr/bin/env python
"""Usage:
        latency_test_sub.py [options]

Options:
  -h --help              show this help message and exit
  -s SECONDS             number of seconds (approx) between prints [default: 1]
  -t NAME                thread name to watch for events [default: t00]
"""
import time
import logging

import docopt

def main(args):
    import pymongo
    from mongotools.pubsub import Channel
    cli = pymongo.MongoClient()
    chan = Channel(cli.test, 'mychannel')
    chan.ensure_channel()
    chan.sub(args['-t'], Benchmark(float(args['-s'])).handle)
    print 'Begin sub benchmark'

    while True:
        chan.handle_ready(raise_errors=True, await=True)
        time.sleep(0.1)

class Benchmark(object):

    def __init__(self, interval):
        self._interval = interval
        self._n_messages = 0
        self._l_total = 0.0
        self._ts_last = time.time()

    def handle(self, chan, msg):
        now = time.time()
        i_latency = now - msg['data']['t']
        self._l_total += i_latency
        self._n_messages += 1
        elapsed = float(now - self._ts_last)
        if elapsed > self._interval:
            self._ts_last = now
            print '%s: %.1fk mps, %.1fms lat' % (
                time.strftime('%Y-%m-%d %H:%M:%S'),
                self._n_messages / elapsed / 1000,
                self._l_total * 1000 / self._n_messages)
            self._n_messages = 0
            self._l_total = 0

if __name__ == '__main__':
    logging.basicConfig()
    main(docopt.docopt(__doc__))
