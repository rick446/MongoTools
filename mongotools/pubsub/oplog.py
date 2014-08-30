import logging

import bson
from pymongo.errors import OperationFailure
from pymongo.cursor import _QUERY_OPTIONS

log = logging.getLogger(__name__)

class OplogTail(object):

    def __init__(self, cli):
        self._cli = cli
        self._coll = cli.local['oplog.rs']
        last_msg = self.last()
        if last_msg:
            self._position = last_msg['ts']
        else:
            self._position = bson.Timestamp(0, 1)

    def last(self, spec=None):
        if spec is None:
            spec = {}
        return self._coll.find_one(spec, sort=[('$natural', -1)], limit=1)

    def cursor(self, spec=None, await=True):
        '''Cursor over all events, starting right now, that satisfy the spec'''
        if spec is None:
            spec = {}
        spec['ts'] = {'$gt': self._position}
        if await:
            options = dict(tailable=True, await_data=True)
        else:
            options = {}
        q = self._coll.find(spec, **options)
        q = q.hint([('$natural', 1)])
        if await:
            q = q.add_option(_QUERY_OPTIONS['oplog_replay'])
        return q

    def tail(self, spec=None, raise_errors=False, await=True):
        if spec is None:
            spec = {}
        cursor = self.cursor(spec, await)
        while True:
            try:
                msg = cursor.next()
            except StopIteration:
                break
            except OperationFailure as err:
                if raise_errors:
                    raise
                else:
                    log.warning(
                        'Error getting messages, may have dropped some: %r',
                        err)
                    break
            self._position = msg['ts']
            yield msg

    def await(self, spec=None):
        '''Await the very next message on the oplog satisfying the spec'''
        if spec is None:
            spec = {}
        last = self.last(spec)
        if last is None:
            return  # Can't await unless there is an existing message satisfying spec
        await_spec = dict(spec)
        last_ts = last['ts']
        await_spec['ts'] = {'$gt': bson.Timestamp(last_ts.time, last_ts.inc - 1)}
        curs = self._coll.find(await_spec, tailable=True, await_data=True)
        curs = curs.hint([('$natural', 1)])
        curs = curs.add_option(_QUERY_OPTIONS['oplog_replay'])
        curs.next()  # should always find 1 element
        try:
            return curs.next()
        except StopIteration:
            return None

