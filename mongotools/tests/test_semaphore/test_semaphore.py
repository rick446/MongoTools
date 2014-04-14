from unittest import TestCase

from mongotools import mim
from mongotools.semaphore import Semaphore


class SemaphoreBinaryTestCase(TestCase):
    
    def setUp(self):
        self.bind = mim.Connection.get()
        self.bind.drop_all()
        self.semaphore = Semaphore(self.bind.db, 1, 'counter', 1)

    def test_acquire(self):
        self.assertTrue(self.semaphore.acquire(), msg='Failed to acquire intial semaphore')

    def test_release(self):
        self.semaphore.acquire()
        self.semaphore.release()

    def test_sequence(self):
        self.assertTrue(self.semaphore.acquire(), msg='Failed to acquire intial semaphore')
        self.assertFalse(self.semaphore.acquire(), msg='Failed to block on second acquire')
        self.semaphore.release()
        self.assertTrue(self.semaphore.acquire(), msg='Failed to reacquire after release')
        for i in xrange(10):
            self.semaphore.release()
            self.assertTrue(self.semaphore.acquire())
            self.semaphore.release()

class SemaphoreBinaryTestCase(TestCase):
    
    _MAX = 5

    def setUp(self):
        self.bind = mim.Connection.get()
        self.bind.drop_all()
        self.semaphore = Semaphore(self.bind.db, 1, 'counter', self._MAX)

    def test_acquire(self):
        for i in xrange(self._MAX):
            self.assertTrue(self.semaphore.acquire(), msg='Failed to acquire intial semaphore')
        self.assertFalse(self.semaphore.acquire())


class BasicSemaphoreTestCase(TestCase):
    
    def setUp(self):
        self.bind = mim.Connection.get()
        self.bind.drop_all()
        self.semaphore = Semaphore(self.bind.db, 1, 'counter', 1)

    def test_counter_name(self):
        self.assertEquals(self.semaphore._counter, 'counter')
