from unittest import TestCase

from mongotools import sequence, mim

class TestSequence(TestCase):

    def setUp(self):
        self.bind = mim.Connection.get()
        self.bind.drop_all()
        self.seq = sequence.Sequence(self.bind.db)

    def test_basic_seqence(self):
        values = [ self.seq.next('foo') for x in range(5) ]
        self.assertEqual(values, range(1, 6))

    def test_inc_2(self):
        values = [ self.seq.next('foo', 2) for x in range(5) ]
        self.assertEqual(values, range(2, 11, 2))

    def test_dual_sequence(self):
        v0 = self.seq.next('foo', 2)
        v1 = self.seq.next('bar', 2)
        self.assertEqual(v0, 2)
        self.assertEqual(v1, 2)

class TestSequenceBasic(TestCase):

    def setUp(self):
        self.bind = mim.Connection.get()
        self.bind.drop_all()

    def test_seq_custom_name(self):
        self.seq = sequence.Sequence(self.bind.db, 'seq')
        self.seq.next('foo')
        self.assertEqual(
            self.bind.db.collection_names(),
            ['seq'])

    def test_seq_name(self):
        self.seq = sequence.Sequence(self.bind.db)
        self.seq.next('foo')
        self.assertEqual(
            self.bind.db.collection_names(),
            ['mongotools.sequence'])
