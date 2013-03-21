'''mim.py - Mongo In Memory - stripped-down version of mongo that is
non-persistent and hopefully much, much faster
'''
import re
import sys
import time
import itertools
import collections
import logging
from datetime import datetime
from hashlib import md5

try:
    import spidermonkey
    from spidermonkey import Runtime
except ImportError:
    Runtime = None

from mongotools.util import LazyProperty

import bson
from pymongo.errors import InvalidOperation, OperationFailure, DuplicateKeyError
from pymongo import database, collection, ASCENDING

log = logging.getLogger(__name__)

class Connection(object):
    _singleton = None

    @classmethod
    def get(cls):
        if cls._singleton is None:
            cls._singleton = cls()
        return cls._singleton

    def __init__(self):
        self._databases = {}

    def drop_all(self):
        self._databases = {}

    def clear_all(self):
        '''Remove all data, but keep the indexes'''
        for db in self._databases.values():
            db.clear()

    def start_request(self):
        return _DummyRequest()

    def end_request(self):
        pass

    def _make_database(self):
        return Database(self)

    def __getattr__(self, name):
        return self[name]

    def __getitem__(self, name):
        return self._get(name)

    def _get(self, name):
        try:
            return self._databases[name]
        except KeyError:
            db = self._databases[name] = Database(self, name)
            return db

    def database_names(self):
        return self._databases.keys()

    def drop_database(self, name):
        del self._databases[name]

    def __repr__(self):
        return 'mim.Connection()'

class Database(database.Database):

    def __init__(self, connection, name):
        self._name = name
        self._connection = connection
        self._collections = {}
        if Runtime is not None:
            self._jsruntime = Runtime()
        else:
            self._jsruntime = None

    @property
    def name(self):
        return self._name

    @property
    def connection(self):
        return self._connection

    def _make_collection(self):
        return Collection(self)

    def command(self, command,
                value=1, check=True, allowable_errors=None, **kwargs):
        if isinstance(command, basestring):
            command = {command:value}
            command.update(**kwargs)
        if 'filemd5' in command:
            checksum = md5()
            for chunk in self.chef.file.chunks.find().sort('n'):
                checksum.update(chunk['data'])
            return dict(md5=checksum.hexdigest())
        elif 'findandmodify' in command:
            coll = self._collections[command['findandmodify']]
            before = coll.find_one(command['query'], sort=command.get('sort'))
            upsert = False
            if before is None:
                upsert = True
                if command.get('upsert'):
                    before = dict(command['query'])
                    coll.insert(before)
                else:
                    raise OperationFailure, 'No matching object found'
            coll.update(command['query'], command['update'])
            if command.get('new', False) or upsert:
                return dict(value=coll.find_one(dict(_id=before['_id'])))
            return dict(value=before)
        elif 'mapreduce' in command:
            collection = command.pop('mapreduce')
            return self._handle_mapreduce(collection, **command)
        elif 'distinct' in command:
            collection = self._collections[command['distinct']]
            key = command['key']
            return list(set(_lookup(d, key) for d in collection.find()))
        elif 'getlasterror' in command:
            return dict(connectionId=None, err=None, n=0, ok=1.0)
        else:
            raise NotImplementedError, repr(command)

    def _handle_mapreduce(self, collection,
                          query=None, map=None, reduce=None, out=None, finalize=None):
        if self._jsruntime is None:
            raise ImportError, 'Cannot import spidermonkey, required for MIM mapreduce'
        j = self._jsruntime.new_context()
        tmp_j = self._jsruntime.new_context()
        temp_coll = collections.defaultdict(list)
        def emit(k, v):
            k = topy(k)
            if isinstance(k, dict):
                k = bson.BSON.encode(k)
            temp_coll[k].append(v)
        def emit_reduced(k, v):
            print k,v 
        # Add some special MongoDB functions
        j.execute('var NumberInt = Number;')
        j.add_global('emit', emit)
        j.add_global('emit_reduced', emit_reduced)
        j.execute('var map=%s;' % map)
        j.execute('var reduce=%s;' % reduce)
        if finalize:
            j.execute('var finalize=%s;' % finalize)
        if query is None: query = {}
        # Run the map phase
        def topy(obj):
            if isinstance(obj, spidermonkey.Array):
                return [topy(x) for x in obj]
            if isinstance(obj, spidermonkey.Object):
                tmp_j.add_global('x', obj)
                js_source = tmp_j.execute('x.toSource()')
                if js_source.startswith('(new Date'):
                    # Date object by itself
                    obj = datetime.fromtimestamp(tmp_j.execute('x.valueOf()')/1000.)
                elif js_source.startswith('({'):
                    # Handle recursive conversion in case we got back a
                    # mapping with multiple values.
                    obj = dict((a, topy(obj[a])) for a in obj)
                else:
                    assert False, 'Cannot convert %s to Python' % (js_source)
            elif isinstance(obj, collections.Mapping):
                return dict((k, topy(v)) for k,v in obj.iteritems())
            elif isinstance(obj, basestring):
                return obj
            elif isinstance(obj, collections.Sequence):
                return [topy(x) for x in obj]
            return obj
        def tojs(obj):
            if isinstance(obj, basestring):
                return obj
            elif isinstance(obj, datetime):
                ts = 1000. * time.mktime(obj.timetuple())
                ts += (obj.microsecond / 1000.)
                return j.execute('new Date(%f)' % (ts))
            elif isinstance(obj, collections.Mapping):
                return dict((k,tojs(v)) for k,v in obj.iteritems())
            elif isinstance(obj, collections.Sequence):
                result = j.execute('new Array()')
                for v in obj:
                    result.push(tojs(v))
                return result
            else: return obj
        for obj in self._collections[collection].find(query):
            obj = tojs(obj)
            j.execute('map').apply(obj)
        # Run the reduce phase
        reduced = topy(dict(
            (k, j.execute('reduce')(k, tojs(values)))
            for k, values in temp_coll.iteritems()))
        # Run the finalize phase
        if finalize:
            reduced = topy(dict(
                (k, j.execute('finalize')(k, tojs(value)))
                for k, value in reduced.iteritems()))
        # Handle the output phase
        result = dict()
        assert len(out) == 1
        if out.keys() == ['reduce']:
            result['result'] = out.values()[0]
            out_coll = self[out.values()[0]]
            for k, v in reduced.iteritems():
                doc = out_coll.find_one(dict(_id=k))
                if doc is None:
                    out_coll.insert(dict(_id=k, value=v))
                else:
                    doc['value'] = topy(j.execute('reduce')(k, tojs([v, doc['value']])))
                    out_coll.save(doc)
        elif out.keys() == ['merge']:
            result['result'] = out.values()[0]
            out_coll = self[out.values()[0]]
            for k, v in reduced.iteritems():
                out_coll.save(dict(_id=k, value=v))
        elif out.keys() == ['replace']:
            result['result'] = out.values()[0]
            self._collections.pop(out.values()[0], None)
            out_coll = self[out.values()[0]]
            for k, v in reduced.iteritems():
                out_coll.save(dict(_id=k, value=v))
        elif out.keys() == ['inline']:
            result['results'] = [
                dict(_id=k, value=v)
                for k,v in reduced.iteritems() ]
        else:
            raise TypeError, 'Unsupported out type: %s' % out.keys()
        return result
                

    def __getattr__(self, name):
        return self[name]

    def __getitem__(self, name):
        return self._get(name)

    def _get(self, name):
        try:
            return self._collections[name]
        except KeyError:
            db = self._collections[name] = Collection(self, name)
            return db

    def __repr__(self):
        return 'mim.Database(%s)' % self.name

    def collection_names(self):
        return self._collections.keys()

    def drop_collection(self, name):
        del self._collections[name]

    def clear(self):
        for coll in self._collections.values():
            coll.clear()

class Collection(collection.Collection):

    def __init__(self, database, name):
        self._name = self.__name = name
        self._database = database
        self._data = {}
        self._unique_indexes = {}
        self._indexes = {}

    def clear(self):
        self._data = {}
        for ui in self._unique_indexes.values():
            ui.clear()

    @property
    def name(self):
        return self._name

    @property
    def database(self):
        return self._database

    def drop(self):
        self._database.drop_collection(self._name)

    def __getattr__(self, name):
        return self._database['%s.%s' % (self.name, name)]

    def _find(self, spec, sort=None, **kwargs):
        bson_safe(spec)
        def _gen():
            for doc in self._data.itervalues():
                mspec = match(spec, doc)
                if mspec is not None: yield doc, mspec
        return _gen()

    def find(self, spec=None, fields=None, as_class=dict, **kwargs):
        if spec is None:
            spec = {}
        sort = kwargs.pop('sort', None)
        cur = Cursor(collection=self, fields=fields, as_class=as_class,
                     _iterator_gen=lambda: self._find(spec, **kwargs))
        if sort:
            cur = cur.sort(sort)
        return cur

    def find_one(self, spec_or_id=None, *args, **kwargs):
        if spec_or_id is not None and not isinstance(spec_or_id, dict):
            spec_or_id = {"_id": spec_or_id}
        for result in self.find(spec_or_id, *args, **kwargs):
            return result
        return None

    def find_and_modify(self, query=None, update=None, upsert=False, **kwargs):
        if query is None: query = {}
        before = self.find_one(query, sort=kwargs.get('sort'))
        upserted = False
        if before is None:
            upserted = True
            if upsert:
                before = dict(query)
                self.insert(before)
            else:
                return None
        before = self.find_one(query, sort=kwargs.get('sort'))
        self.update({'_id': before['_id']}, update)
        if kwargs.get('new', False) or upserted:
            return self.find_one(dict(_id=before['_id']))
        return before

    def insert(self, doc_or_docs, safe=False):
        if not isinstance(doc_or_docs, list):
            doc_or_docs = [ doc_or_docs ]
        for doc in doc_or_docs:
            doc = bcopy(doc)
            bson_safe(doc)
            _id = doc.get('_id', ())
            if _id == ():
                _id = doc['_id'] = bson.ObjectId()
            if _id in self._data:
                if safe: raise DuplicateKeyError('duplicate ID on insert')
                continue
            self._index(doc)
            self._data[_id] = bcopy(doc)
        return _id

    def save(self, doc, safe=False):
        _id = doc.get('_id', ())
        if _id == ():
            return self.insert(doc, safe=safe)
        else:
            self.update({'_id':_id}, doc, upsert=True, safe=safe)
            return _id

    def update(self, spec, updates, upsert=False, safe=False, multi=False):
        bson_safe(spec)
        bson_safe(updates)
        result = dict(
            connectionId=None,
            updatedExisting=False,
            err=None,
            ok=1.0,
            n=0)
        for doc, mspec in self._find(spec):
            self._deindex(doc) 
            mspec.update(updates)
            self._index(doc) 
            result['n'] += 1
            if not multi: break
        if result['n']:
            result['updatedExisting'] = True
            return result
        if upsert:
            doc = dict(spec)
            MatchDoc(doc).update(updates)
            _id = doc.get('_id', ())
            if _id == ():
                _id = doc['_id'] = bson.ObjectId()
            self._index(doc) 
            self._data[_id] = bcopy(doc)
            result['upserted'] = _id
            return result
        else:
            return result

    def remove(self, spec=None, **kwargs):
        if spec is None: spec = {}
        new_data = {}
        for id, doc in self._data.iteritems():
            if match(spec, doc):
                self._deindex(doc)
            else:
                new_data[id] = doc
        self._data = new_data

    def ensure_index(self, key_or_list, unique=False, ttl=300,
                     name=None, background=None, sparse=False):
        if isinstance(key_or_list, list):
            keys = tuple(k[0] for k in key_or_list)
        else:
            keys = (key_or_list,)
        index_name = '_'.join(keys)
        self._indexes[index_name] =[ (k, 0) for k in keys ]
        if not unique: return
        self._unique_indexes[keys] = index = {}
        for id, doc in self._data.iteritems():
            key_values = tuple(doc.get(key, None) for key in keys)
            index[key_values] =id
        return index_name

    def index_information(self):
        return dict(
            (index_name, dict(key=fields))
            for index_name, fields in self._indexes.iteritems())

    def drop_index(self, iname):
        index = self._indexes.pop(iname, None)
        if index is None: return
        keys = tuple(i[0] for i in index)
        self._unique_indexes.pop(keys, None)

    def _get_wc_override(self):
        '''For gridfs compatibility'''
        return {}

    def __repr__(self):
        return 'mim.Collection(%r, %s)' % (self._database, self.name)

    def _index(self, doc):
        if '_id' not in doc: return
        for keys, index in self._unique_indexes.iteritems():
            key_values = tuple(doc.get(key, None) for key in keys)
            old_id = index.get(key_values, ())
            if old_id == doc['_id']: continue
            if old_id in self._data:
                raise DuplicateKeyError, '%r: %s' % (self, keys)
            index[key_values] = doc['_id']

    def _deindex(self, doc):
        for keys, index in self._unique_indexes.iteritems():
            key_values = tuple(doc.get(key, None) for key in keys)
            index.pop(key_values, None)

    def map_reduce(self, map, reduce, out, full_response=False, **kwargs):
        if isinstance(out, basestring):
            out = { 'replace':out }
        cmd_args = {'mapreduce': self.name,
                    'map': map,
                    'reduce': reduce,
                    'out': out,
                    }
        cmd_args.update(kwargs)
        return self.database.command(cmd_args)

    def distinct(self, key):
        return self.database.command({'distinct': self.name,
                                      'key': key,
                                      })


class Cursor(object):

    def __init__(self, collection, _iterator_gen,
                 sort=None, skip=None, limit=None, fields=None, as_class=dict):
        if isinstance(fields, list):
            fields = dict((f, 1) for f in fields)
        
        if fields is not None and '_id' not in fields:
            f = { '_id': 1 }
            f.update(fields)
            fields = f

        self._collection = collection
        self._iterator_gen = _iterator_gen
        self._sort = sort
        self._skip = skip
        self._limit = limit
        self._fields = fields
        self._as_class = as_class
        self._safe_to_chain = True

    @LazyProperty
    def iterator(self):
        self._safe_to_chain = False
        result = (doc for doc,match in self._iterator_gen())
        if self._sort is not None:
            result = sorted(result, cmp=cursor_comparator(self._sort))
        if self._skip is not None:
            result = itertools.islice(result, self._skip, sys.maxint)
        if self._limit is not None:
            result = itertools.islice(result, abs(self._limit))
        return iter(result)

    def clone(self, **overrides):
        result = Cursor(
            collection=self._collection,
            _iterator_gen=self._iterator_gen,
            sort=self._sort,
            skip=self._skip,
            limit=self._limit,
            fields=self._fields,
            as_class=self._as_class)
        for k,v in overrides.items():
            setattr(result, k, v)
        return result

    def rewind(self):
        if not self._safe_to_chain:
            del self.iterator
            self._safe_to_chain = True

    def count(self):
        return sum(1 for x in self._iterator_gen())

    def __getitem__(self, key):
        # Le *sigh* -- this is the only place apparently where pymongo *does*
        # clone
        clone = self.clone()
        return clone.skip(key).next()

    def __iter__(self):
        return self

    def next(self):
        value = self.iterator.next()
        value = bcopy(value)
        if self._fields:
            value = _project(value, self._fields)
        return wrap_as_class(value, self._as_class)

    def sort(self, key_or_list, direction=ASCENDING):
        if not self._safe_to_chain:
            raise InvalidOperation('cannot set options after executing query')
        if not isinstance(key_or_list, list):
            key_or_list = [ (key_or_list, direction) ]
        keys = []
        for t in key_or_list:
            if isinstance(t, tuple):
                keys.append(t)
            else:
                keys.append(t, ASCENDING)
        self._sort = keys
        return self # I'd rather clone, but that's not what pymongo does here

    def all(self):
        return list(self._iterator_gen())

    def skip(self, skip):
        if not self._safe_to_chain:
            raise InvalidOperation('cannot set options after executing query')
        self._skip = skip
        return self # I'd rather clone, but that's not what pymongo does here

    def limit(self, limit):
        if not self._safe_to_chain:
            raise InvalidOperation('cannot set options after executing query')
        self._skip = limit
        return self # I'd rather clone, but that's not what pymongo does here

    def distinct(self, key):
        return list(set(_lookup(d, key) for d in self.all()))

    def hint(self, index):
        # checks indexes, but doesn't actually use hinting
        if type(index) == list:
            # ignoring direction, since mim's ensure_index doesn't preserve it (set to 0)
            test_idx = [(i, 0) for i, direction in index if i != '$natural']
            if test_idx and test_idx not in self._collection._indexes.values():
                raise OperationFailure('database error: bad hint. Valid values: %s' %
                        self._collection._indexes.values())
        elif isinstance(index, basestring):
            if index not in self._collection._indexes.keys():
                raise OperationFailure('database error: bad hint. Valid values: %s'
                        % self._collection._indexes.keys())
        elif index == None:
            pass
        else:
            raise TypeError('hint index should be string, list of tuples, or None, but was %s' % type(index))
        return self

def cursor_comparator(keys):
    def comparator(a, b):
        for k,d in keys:
            x = _lookup(a, k, None)
            y = _lookup(b, k, None)
            part = BsonArith.cmp(x, y)
            if part: return part * d
        return 0
    return comparator

class BsonArith(object):
    _types = None
    _index = None

    @classmethod
    def cmp(cls, x, y):
        return cmp(cls.to_bson(x), cls.to_bson(y))

    @classmethod
    def to_bson(cls, val):
        if val is (): return val
        tp = cls.bson_type(val)
        return (tp, cls._types[tp][0](val))

    @classmethod
    def bson_type(cls, value):
        if cls._index is None:
            cls._build_index()
        tp = cls._index.get(type(value), None)
        if tp is not None: return tp
        for tp, (conv, types) in enumerate(cls._types):
            if isinstance(value, tuple(types)):
                cls._index[type(value)] = tp
                return tp
        raise KeyError, type(value)

    @classmethod
    def _build_index(cls):
        cls._build_types()
        cls._index = {}
        for tp, (conv, types) in enumerate(cls._types):
            for t in types:
                cls._index[t] = tp

    @classmethod
    def _build_types(cls):
        cls._types = [
            (lambda x:x, [ type(None) ]),
            (lambda x:x, [ int, long, float ]),
            (lambda x:x, [ str, unicode ]),
            (lambda x:dict(x), [ dict, MatchDoc ]),
            (lambda x:list(x), [ list, MatchList ]),
            (lambda x:x, [ bson.Binary ]),
            (lambda x:x, [ bson.ObjectId ]),
            (lambda x:x, [ bool ]),
            (lambda x:x, [ datetime ]),
            (lambda x:x, [ type(re.compile('foo')) ] )
            ]        

def match(spec, doc):
    if '$or' in spec:
        assert len(spec) == 1
        if any(match(branch, doc) for branch in spec['$or']):
            return True
        return None
    mspec = MatchDoc(doc)
    try:
        for k,v in spec.iteritems():
            subdoc, subdoc_key = mspec.traverse(*k.split('.'))
            for op, value in _parse_query(v):
                if not subdoc.match(subdoc_key, op, value): return None
    except KeyError:
        raise
        return None
    return mspec

class Match(object): 
    def match(self, key, op, value):
        log.debug('match(%r, %r, %r, %r)',
                  self, key, op, value)
        val = self.get(key, ())
        if isinstance(val, MatchList):
            if val.match('$', op, value): return True
        if op == '$eq': return BsonArith.cmp(val, value) == 0
        if op == '$ne': return BsonArith.cmp(val, value) != 0
        if op == '$gt': return BsonArith.cmp(val, value) > 0
        if op == '$gte': return BsonArith.cmp(val, value) >= 0
        if op == '$lt': return BsonArith.cmp(val, value) < 0
        if op == '$lte': return BsonArith.cmp(val, value) <= 0
        if op == '$in':
            for ele in value:
                if self.match(key, '$eq', ele):
                    return True
            return False
        if op == '$nin':
            for ele in value:
                if self.match(key, '$eq', ele):
                    return False
            return True
        if op == '$exists':
            if value: return val != ()
            else: return val == ()
        if op == '$all':
            for ele in value:
                if not self.match(key, '$eq', ele):
                    return False
            return True
        if op == '$elemMatch':
            if not isinstance(val, MatchList): return False
            for ele in val:
                m = match(value, ele)
                if m: return True
            return False
        raise NotImplementedError, op
    def getvalue(self, path):
        parts = path.split('.')
        subdoc, key = self.traverse(*parts)
        return subdoc[key]
    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default
    def update(self, updates):
        newdoc = {}
        for k, v in updates.iteritems():
            if k.startswith('$'): break
            newdoc[k] = bcopy(v)
        if newdoc:
            self._orig.clear()
            self._orig.update(bcopy(newdoc))
            return
        for op, update_parts in updates.iteritems():
            func = getattr(self, '_op_' + op[1:], None)
            if func is None:
                raise NotImplementedError, op
            for k,arg in update_parts.items():
                subdoc, key = self.traverse(k)
                func(subdoc, key, arg)
        validate(self._orig)

    def _op_inc(self, subdoc, key, arg):
        subdoc.setdefault(key, 0)
        subdoc[key] += arg

    def _op_set(self, subdoc, key, arg):
        subdoc[key] = bcopy(arg)
        
    def _op_push(self, subdoc, key, arg):
        l = subdoc.setdefault(key, [])
        l.append(bcopy(arg))

    def _op_pop(self, subdoc, key, arg):
        l = subdoc.setdefault(key, [])
        if arg == 1:
            del l[-1]
        else:
            del l[1]

    def _op_pushAll(self, subdoc, key, arg):
        l = subdoc.setdefault(key, [])
        l.extend(bcopy(arg))

    def _op_addToSet(self, subdoc, key, arg):
        l = subdoc.setdefault(key, [])
        if arg not in l:
            l.append(bcopy(arg))

    def _op_pull(self, subdoc, key, arg):
        l = subdoc.setdefault(key, [])
        if isinstance(arg, dict):
            subdoc[key] = [
                vv for vv in l
                if not match(arg, vv) ]
        else:
            subdoc[key] = [
                vv for vv in l
                if not compare('$eq', arg, vv) ]
            


class MatchDoc(Match):
    def __init__(self, doc):
        self._orig = doc
        self._doc = {}
        for k,v in doc.iteritems():
            if isinstance(v, list):
                self._doc[k] = MatchList(v)
            elif isinstance(v, dict):
                self._doc[k] = MatchDoc(v)
            else:
                self._doc[k] = v
    def traverse(self, first, *rest):
        if not rest:
            if '.' in first:
                return self.traverse(*(first.split('.')))
            return self, first
        if first not in self._doc:
            self._doc[first] = MatchDoc({})
        return self[first].traverse(*rest)
    def iteritems(self):
        return self._doc.iteritems()
    def __eq__(self, o):
        return isinstance(o, MatchDoc) and self._doc == o._doc
    def __hash__(self):
        return hash(self._doc)
    def __repr__(self):
        return 'M%r' % (self._doc,)
    def __getitem__(self, key):
        return self._doc[key]
    def __setitem__(self, key, value):
        self._doc[key] = value
        self._orig[key] = value
    def setdefault(self, key, default):
        self._doc.setdefault(key, default)
        return self._orig.setdefault(key, default)

class MatchList(Match):
    def __init__(self, doc, pos=None):
        self._orig = doc
        self._doc = []
        for ele in doc:
            if isinstance(ele, list):
                self._doc.append(MatchList(ele))
            elif isinstance(ele, dict):
                self._doc.append(MatchDoc(ele))
            else:
                self._doc.append(ele)
        self._pos = pos
    def __iter__(self):
        return iter(self._doc)
    def traverse(self, first, *rest):
        if not rest:
            return self, first
        return self[first].traverse(*rest)
    def match(self, key, op, value):
        if key == '$':
            for i, item in enumerate(self._doc):
                if self.match(i, op, value):
                    if self._pos is None:
                        self._pos = i
                    return True
            return None
        try:
            m = super(MatchList, self).match(key, op, value)
            if m: return m
        except:
            pass
        for ele in self:
            if (isinstance(ele, Match)
                and ele.match(key, op, value)):
                return True

    def __eq__(self, o):
        return isinstance(o, MatchList) and self._doc == o._doc
    def __hash__(self):
        return hash(self._doc)
    def __repr__(self):
        return 'M<%r>%r' % (self._pos, self._doc)
    def __getitem__(self, key):
        try:
            if key == '$':
                if self._pos is None:
                    return self._doc[0]
                else:
                    return self._doc[self._pos]
            else:
                return self._doc[int(key)]
        except IndexError:
            raise KeyError, key
    def __setitem__(self, key, value):
        if key == '$':
            key = self._pos
        self._doc[key] = value
        self._orig[key] = value
    def __delitem__(self, key):
        del self._doc[key]
    def setdefault(self, key, default):
        if key == '$':
            key = self._pos
        if key <= len(self._orig):
            return self._orig[key]
        while key >= len(self._orig):
            self._doc.append(None)
            self._orig.append(None)
        self._doc[key] = default
        self._orig[key] = default


def _parse_query(v):
    if isinstance(v, dict) and all(k.startswith('$') for k in v.keys()):
        return v.items()
    else:
        return [('$eq', v)]

def _part_match(op, value, key_parts, doc, allow_list_compare=True):
    if not key_parts:
        return compare(op, doc, value)
    elif isinstance(doc, list) and allow_list_compare:
        for v in doc:
            if _part_match(op, value, key_parts, v, allow_list_compare=False):
                return True
        else:
            return False
    else:
        return _part_match(op, value, key_parts[1:], doc.get(key_parts[0], ()))

def _lookup(doc, k, default=()):
    try:
        for part in k.split('.'):
            doc = doc[part]
    except KeyError:
        if default != (): return default
        raise
    return doc

def compare(op, a, b):
    if op == '$gt': return BsonArith.cmp(a, b) > 0
    if op == '$gte': return BsonArith.cmp(a, b) >= 0
    if op == '$lt': return BsonArith.cmp(a, b) < 0
    if op == '$lte': return BsonArith.cmp(a, b) <= 0
    if op == '$eq':
        if hasattr(b, 'match'):
            return b.match(a)
        elif isinstance(a, list):
            if a == b: return True
            return b in a
        else:
            return a == b
    if op == '$ne': return a != b
    if op == '$in':
        if isinstance(a, list):
            for ele in a:
                if ele in b:
                    return True
            return False
        else:
            return a in b
    if op == '$nin': return a not in b
    if op == '$exists':
        return a != () if b else a == ()
    if op == '$all':
        return set(a).issuperset(b)
    if op == '$elemMatch':
        return match(b, a)
    raise NotImplementedError, op
        
def validate(doc):
    for k,v in doc.iteritems():
        assert '$' not in k
        assert '.' not in k
        if hasattr(v, 'iteritems'):
            validate(v)
            
def bson_safe(obj):
    bson.BSON.encode(obj)

def bcopy(obj):
    if isinstance(obj, dict):
        return bson.BSON.encode(obj).decode()
    elif isinstance(obj, list):
        return map(bcopy, obj)
    else:
        return obj
        
def wrap_as_class(value, as_class):
    if isinstance(value, dict):
        return as_class(dict(
                (k, wrap_as_class(v, as_class))
                for k,v in value.items()))
    elif isinstance(value, list):
        return [ wrap_as_class(v, as_class) for v in value ]
    else:
        return value

def _traverse_doc(doc, key):
    path = key.split('.')
    cur = doc
    for part in path[:-1]:
        cur = cur.setdefault(part, {})
    return cur, path[-1]

def _project(doc, fields):
    result = {}
    for name, value in fields.items():
        if not value: continue
        sub_doc, key = _traverse_doc(doc, name)
        sub_result, key = _traverse_doc(result, name)
        sub_result[key] = sub_doc[key]
    return result

class _DummyRequest(object):

    def __enter__(self):
        pass

    def __exit__(self, ex_type, ex_value, ex_tb):
        pass
    
    def end(self):
        pass

