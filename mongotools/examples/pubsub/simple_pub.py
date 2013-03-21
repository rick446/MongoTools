# Publisher
import pymongo
from mongotools.pubsub import Channel

cli = pymongo.MongoClient()

chan = Channel(cli.test, 'mychannel')
chan.ensure_channel()

chan.pub('foo')
chan.pub('bar', {'a':1})
chan.multipub([
    { 'k': 'bar', 'data': None },
    { 'k': 'baz', 'data':{'a':1}} ])
