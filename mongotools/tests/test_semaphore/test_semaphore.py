from unittest import TestCase

from mongotools import mim
from mongotools.semaphore import Semaphore


class SemaphoreTestCase(TestCase):
    
    def setUp(self):
        self.bind = mim.Connection.get()
        self.bind.drop_all()
        self.semaphore = Semaphore(self.bind.db, 1, 'counter', 1)

    def test_acquire(self):
        self.assertTrue(self.semaphore.acquire(), msg='Failed to acquire intial semaphore')
        self.assertEqual(self.semaphore._counter, 0)
        self.assertFalse(self.semaphore.acquire(), msg='Failed to block after first acquire')

    def test_release(self):
        self.semaphore.acquire()
        self.semaphore.release()
        self.assertEqual(self.semaphore._counter, 1)

class BasicSemaphoreTestCase(TestCase):
    
    def setUp(self):
        self.bind = mim.Connection.get()
        self.bind.drop_all()
        self.semaphore = Semaphore(self.bind.db, 1, 'counter', 1)

    def test_semaphore_name(self):
        pass
