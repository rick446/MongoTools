#!/usr/bin/env python
"""Usage:
        latency-test [options]

Options:
  -h --help              show this help message and exit
  -n COUNT               number of messages per multipub [default: 100]
  -c CONCURRENCY         number of threads to spawn [default: 1]
  --capacity CAPACITY    size of the capped collection [default: 32768]
"""

import time
import logging
import threading

import docopt

def main(args):
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
            args=(chan, name, int(args['-n'])))
        for name in names ]
    for t in threads:
        t.setDaemon(True)
        t.start()
    while True:
        time.sleep(60)
        print '---mark---'
                               
def target(chan, name, n):
    while True:
        chan.multipub(
            [ dict(k=name, data=time.time()) for x in range(n) ])
        
if __name__ == '__main__':
    logging.basicConfig()
    main(docopt.docopt(__doc__))
