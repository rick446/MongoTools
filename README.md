# MongoTools

MongoTools is a collection of utilities that I have found helpful in using MongoDB. 

## MIM: MongoDB-in-Memory

This module allows you to have an in-memory lightweight replacement for the
pymongo Connection/MongoClient class for use in unit testing (it's cheaper to
teardown and rebuild a MIM database than it is to do a "real" MongoDB database)

## Sequence

This module gives you the ability to create the 'auto-increment integer' you've
been missing from MySQL.

## PubSub

This module provides a `Channel` class that you can publish messages to, and from
which you can subscribe to messages. 
It uses capped collections.
It seems to be very fast, with (on my laptop) latencies of 2-4ms with a
throughput of around 40k messages per second.
(You should be able to increase the overall throughput to over 90-100k messages per
second, but individual clients can't consume messages that fast.)

Here's a simple example of subscriber code:

~~~~python
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
~~~~

... and the publisher:

~~~~python
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
~~~~

