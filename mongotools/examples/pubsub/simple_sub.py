# Subscriber
import time
import pymongo
from mongotools.pubsub import Channel

cli = pymongo.MongoClient()

chan = Channel(cli.test, 'mychannel')
chan.ensure_channel()

def printer(chan, msg):
    print chan, msg
    
chan.sub('foo', printer)
chan.sub('bar', printer)

while True:
    chan.handle_ready(await=True)
    time.sleep(0.1)
