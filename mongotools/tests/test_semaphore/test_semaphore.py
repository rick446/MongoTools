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

    def test_peek(self):
        self.assertEquals(self.semaphore.peek(), 1)
        self.semaphore.acquire()
        self.assertEquals(self.semaphore.peek(), 0)

    def test_force_acquire(self):
        val = self.semaphore.peek()
        self.semaphore.force_acquire()
        self.assertEquals(val - 1, self.semaphore.peek())
        self.semaphore.force_acquire()
        self.assertEquals(val - 2, self.semaphore.peek())

    def test_force_release(self):
        val = self.semaphore.peek()
        self.semaphore.force_release()
        self.assertEquals(val + 1, self.semaphore.peek())
        self.semaphore.force_release()
        self.assertEquals(val + 2, self.semaphore.peek())

    def test_status(self):
        self.assertTrue(self.semaphore.status(), msg='Initial status is false')
        self.assertTrue(self.semaphore.acquire())
        self.assertFalse(self.semaphore.status(), msg='Status after acquire is true')
        self.assertFalse(self.semaphore.acquire())
        self.semaphore.release()
        self.assertTrue(self.semaphore.status())

        
class SemaphoreMultipleTestCase(TestCase):
    
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

    def test_id(self):
        self.assertEquals(self.semaphore._id, 1)

    def test_db_holds_sem(self):
        sem = self.semaphore
        q = sem._db[sem._name].find_one({'_id':sem._id})
        self.assertIsNotNone(q, msg='semaphore not in database')
