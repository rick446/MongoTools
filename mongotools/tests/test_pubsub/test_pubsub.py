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

    def test_no_index(self):
        self.assertEqual(
            self.chan.db[self.chan.name].index_information(),
            {})

    def test_cursor_explain(self):
        cur = self.chan.cursor()
        plan = cur.explain()
        self.assertEqual(plan['cursor'], 'ForwardCappedCursor')

    def test_cursor_await_explain(self):
        cur = self.chan.cursor(await=True)
        plan = cur.explain()
        self.assertEqual(plan['cursor'], 'ForwardCappedCursor')

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
            [ dict(ts=1, k='foo', data=None),
              dict(ts=2, k='bar', data=None),
              dict(ts=3, k='baz', data=None),
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
            [ dict(ts=1, k='foo', data=None),
              dict(ts=2, k='foo', data=None),
              dict(ts=3, k='foo', data=None),
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
                dict(ts=1, k='foo', data=None),
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
                dict(ts=1, k='foo', data=None),
                ])
        
