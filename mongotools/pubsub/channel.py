import re
import logging
from collections import defaultdict

from .oplog import OplogTail

log = logging.getLogger(__name__)

class Channel(object):

    def __init__(self, db, name):
        self.db = db
        self.name = name
        self._collection_ns = '{}.{}'.format(self.db.name, self.name)
        self._callbacks = defaultdict(list)
        self._tail = OplogTail(db.connection)

    def __repr__(self): # pragma no cover
        return '<Channel %s.%s>' % (
            self.db.name,
            self.name)

    def _spec(self):
        spec = {'ns': self._collection_ns, 'op': 'i'}
        if self._callbacks:
            regex = '|'.join(cb.pattern for cb in self._callbacks)
            spec['o.k'] = re.compile(regex)
        return spec

    def ensure_channel(self, capacity=2**15, message_size=1024):
        if self.name not in self.db.collection_names():
            self.db.create_collection(
                self.name,
                size=capacity * message_size,
                capped=True,
                max=capacity,
                autoIndexId=False)

    def sub(self, pattern, callback=None):
        re_pattern = re.compile('^' + re.escape(pattern))
        self._callbacks[re_pattern] # ensure key exists
        def decorator(func):
            self._callbacks[re_pattern].append(func)
            return func
        if callback is None: return decorator
        return decorator(callback)

    def pub(self, key, data=None):
        doc = dict(k=key, data=data)
        self.db[self.name].insert(doc, manipulate=False)
        return doc

    def multipub(self, messages):
        self.db[self.name].insert(messages, manipulate=False)
        return messages

    def handle_ready(self, raise_errors=False, await=False):
        if not self._callbacks:
            return
        spec = self._spec()
        for msg in self._tail.tail(spec, raise_errors=raise_errors, await=await):
            msg_obj = msg['o']
            to_call = []
            for pattern, callbacks in self._callbacks.items():
                if pattern.match(msg_obj['k']):
                    to_call += callbacks
            for cb in to_call:
                try:
                    cb(self, msg_obj)
                except:
                    if raise_errors:
                        raise
                    log.exception('Error in callback handling %r(%r)',
                                  cb, msg)

    def await(self):
        return self._tail.await(self._spec())

