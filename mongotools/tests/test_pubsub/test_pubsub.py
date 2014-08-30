from unittest import TestCase

import pymongo
from mongotools import pubsub

class TestPubSub(TestCase):

    def setUp(self):
        self.bind = pymongo.MongoClient()
        self.db = self.bind['mongotools-test']
        for cname in self.db.collection_names():
            if not cname.startswith('system.'):
                self.db[cname].drop()
        self.chan = pubsub.Channel(self.db, 'channel')
        self.chan.db[self.chan.name].drop()
        self.chan.ensure_channel()

    def test_basic(self):
        messages = []
        def callback(channel, message):
            messages.append(message)
        self.chan.sub('', callback)
        self.chan.pub('foo')
        self.chan.pub('bar')
        self.chan.pub('baz')
        self.assertEqual(messages, [])
        self.chan.handle_ready()
        self.assertEqual(
            messages,
            [ dict(k='foo', data=None),
              dict(k='bar', data=None),
              dict(k='baz', data=None),
              ])

    def test_multipub(self):
        messages = []
        def callback(channel, message):
            messages.append(message)
        self.chan.sub('', callback)
        self.chan.multipub([
            dict(k='foo', data=None) for x in range(3)])
        self.assertEqual(messages, [])
        self.chan.handle_ready()
        self.assertEqual(
            messages,
            [ dict(k='foo', data=None),
              dict(k='foo', data=None),
              dict(k='foo', data=None),
              ])

    def test_swallow_exc(self):
        messages = []
        def callback(channel, message):
            messages.append(message)
            raise ValueError,'test error'
        self.chan.sub('', callback)
        self.chan.pub('foo')
        self.assertEqual(messages, [])
        self.chan.handle_ready()
        self.assertEqual(messages, [
                dict(k='foo', data=None),
                ])

    def test_raise_exc(self):
        messages = []
        def callback(channel, message):
            messages.append(message)
            raise ValueError,'test error'
        self.chan.sub('', callback)
        self.chan.pub('foo')
        self.assertEqual(messages, [])
        with self.assertRaises(ValueError):
            self.chan.handle_ready(raise_errors=True)
        self.assertEqual(messages, [
                dict(k='foo', data=None),
                ])
