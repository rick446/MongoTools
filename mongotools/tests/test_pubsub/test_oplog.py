from unittest import TestCase

import pymongo
from mongotools import pubsub

class TestOplog(TestCase):

    def setUp(self):
        self.bind = pymongo.MongoClient()
        self.tail = pubsub.OplogTail(self.bind)

    def test_cursor_explain(self):
        cur = self.tail.cursor(await=False)
        plan = cur.explain()
        self.assertEqual(plan['cursor'], 'ForwardCappedCursor')

    def test_cursor_await_explain(self):
        cur = self.tail.cursor(await=True)
        plan = cur.explain()
        self.assertEqual(plan['cursor'], 'ForwardCappedCursor')

