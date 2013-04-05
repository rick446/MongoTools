#!/usr/bin/env python
"""Usage:
        latency_test_pub.py [options]

Options:
  -h --help              show this help message and exit
  -n COUNT               number of messages per multipub [default: 100]
  -c CONCURRENCY         number of threads to spawn [default: 1]
  -s SIZE                size of message data payload [default: 0]
  --capacity CAPACITY    size of the capped collection [default: 32768]
"""

import time
import logging
import threading

import docopt

sent=0

def main(args):
    global sent
    import pymongo
    from mongotools.pubsub import Channel
    cli = pymongo.MongoClient(w=0)
    chan = Channel(cli.test, 'mychannel')
    cli.test.mychannel.drop()
    chan.ensure_channel(capacity=int(args['--capacity']))
    names = [ 't%.2d' % i for i in range(int(args['-c'])) ]
    threads = [
        threading.Thread(
            target=target,
            args=(chan, name, int(args['-n']), int(args['-s'])))
        for name in names ]
    for t in threads:
        t.setDaemon(True)
        t.start()
    last = time.time()
    while True:
        time.sleep(10)
        now = time.time()
        elapsed = now-last
        print '%.1f mps' % (
            1.0 * sent / elapsed)
        sent=0
        last = now
                               
def target(chan, name, n, size):
    global sent
    payload = ' ' * size
    while True:
        chan.multipub(
            [ dict(k=name, data=dict(t=time.time(), p=payload)) for x in range(n) ])
        sent += n
        
if __name__ == '__main__':
    logging.basicConfig()
    main(docopt.docopt(__doc__))
