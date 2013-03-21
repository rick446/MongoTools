from datetime import datetime
from unittest import TestCase

import bson
from mongotools import mim
from pymongo.errors import OperationFailure
from nose import SkipTest

class TestDatastore(TestCase):

    def setUp(self):
        self.bind = mim.Connection.get()
        self.bind.drop_all()
        self.bind.db.coll.insert({'_id':'foo', 'a':2, 'c':[1,2,3]})
        for r in range(4):
            self.bind.db.rcoll.insert({'_id':'r%s' % r, 'd':r})

    def test_eq(self):
        f = self.bind.db.rcoll.find
        assert 1 == f(dict(d={'$eq': 0})).count()

    def test_ne(self):
        f = self.bind.db.rcoll.find
        assert 3 == f(dict(d={'$ne': 0})).count()

    def test_gt(self):
        f = self.bind.db.rcoll.find
        assert 1 == f(dict(d={'$gt': 2})).count()
        assert 0 == f(dict(d={'$gt': 3})).count()

    def test_gte(self):
        f = self.bind.db.rcoll.find
        assert 2 == f(dict(d={'$gte': 2})).count()
        assert 1 == f(dict(d={'$gte': 3})).count()

    def test_lt(self):
        f = self.bind.db.rcoll.find
        assert 0 == f(dict(d={'$lt': 0})).count()
        assert 1 == f(dict(d={'$lt': 1})).count()
        assert 2 == f(dict(d={'$lt': 2})).count()

    def test_lte(self):
        f = self.bind.db.rcoll.find
        assert 1 == f(dict(d={'$lte': 0})).count()
        assert 2 == f(dict(d={'$lte': 1})).count()
        assert 3 == f(dict(d={'$lte': 2})).count()

    def test_range_equal(self):
        f = self.bind.db.rcoll.find
        assert 1 == f(dict(d={'$gte': 2, '$lte': 2})).count()
        assert 2 == f(dict(d={'$gte': 1, '$lte': 2})).count()
        assert 0 == f(dict(d={'$gte': 4, '$lte': -1})).count()

    def test_range_inequal(self):
        f = self.bind.db.rcoll.find
        assert 0 == f(dict(d={'$gt': 2, '$lt': 2})).count()
        assert 1 == f(dict(d={'$gt': 2, '$lt': 4})).count()
        assert 0 == f(dict(d={'$gt': 1, '$lt': 2})).count()
        assert 1 == f(dict(d={'$gt': 1, '$lt': 3})).count()
        assert 0 == f(dict(d={'$gt': 4, '$lt': -1})).count()

    def test_exists(self):
        f = self.bind.db.coll.find
        assert 1 == f(dict(a={'$exists':True})).count()
        assert 0 == f(dict(a={'$exists':False})).count()
        assert 0 == f(dict(b={'$exists':True})).count()
        assert 1 == f(dict(b={'$exists':False})).count()

    def test_all(self):
        f = self.bind.db.coll.find
        assert 1 == f(dict(c={'$all':[1,2]})).count()
        assert 1 == f(dict(c={'$all':[1,2,3]})).count()
        assert 0 == f(dict(c={'$all':[2,3,4]})).count()
        assert 1 == f(dict(c={'$all':[]})).count()

    def test_or(self):
        f = self.bind.db.coll.find
        assert 1 == f(dict({'$or': [{'c':{'$all':[1,2,3]}}]})).count()
        assert 0 == f(dict({'$or': [{'c':{'$all':[4,2,3]}}]})).count()
        assert 1 == f(dict({'$or': [{'a': 2}, {'c':{'$all':[1,2,3]}}]})).count()

    def test_find_with_fields(self):
        o = self.bind.db.coll.find_one({'a':2}, fields=['a'])
        assert o['a'] == 2
        assert o['_id'] == 'foo'
        assert 'c' not in o

    def test_rewind(self):
        collection = self.bind.db.coll
        collection.insert({'a':'b'}, safe=True)

        cursor = collection.find()
        doc = cursor[0]
        cursor.next()
        cursor.rewind()
        assert cursor.next() == doc


class TestDottedOperators(TestCase):

    def setUp(self):
        self.bind = mim.Connection.get()
        self.bind.drop_all()
        self.bind.db.coll.insert(
            {'_id':'foo', 'a':2,
             'b': { 'c': 1, 'd': 2, 'e': [1,2,3],
                    'f': [ { 'g': 1 }, { 'g': 2 } ] } })
        self.coll = self.bind.db.coll

    def test_inc_dotted_dollar(self):
        self.coll.update({'b.e': 2}, { '$inc': { 'b.e.$': 1 } })
        obj = self.coll.find_one({}, { '_id': 0, 'b.e': 1 })
        self.assertEqual(obj, { 'b': { 'e': [ 1,3,3 ] } })

    def test_find_dotted(self):
        self.assertEqual(self.coll.find({'b.c': 1}).count(), 1)
        self.assertEqual(self.coll.find({'b.c': 2}).count(), 0)

    def test_inc_dotted(self):
        self.coll.update({}, { '$inc': { 'b.c': 4 } })
        obj = self.coll.find_one({}, { '_id': 0, 'b.c': 1 })
        self.assertEqual(obj, { 'b': { 'c': 5 } })

    def test_set_dotted(self):
        self.coll.update({}, { '$set': { 'b.c': 4 } })
        obj = self.coll.find_one({}, { '_id': 0, 'b.c': 1 })
        self.assertEqual(obj, { 'b': { 'c': 4 } })

    def test_push_dotted(self):
        self.coll.update({}, { '$push': { 'b.e': 4 } })
        obj = self.coll.find_one({}, { '_id': 0, 'b.e': 1 })
        self.assertEqual(obj, { 'b': { 'e': [1,2,3,4] } })

    def test_addToSet_dotted(self):
        self.coll.update({}, { '$addToSet': { 'b.e': 4 } })
        obj = self.coll.find_one({}, { '_id': 0, 'b.e': 1 })
        self.assertEqual(obj, { 'b': { 'e': [1,2,3,4] } })
        self.coll.update({}, { '$addToSet': { 'b.e': 4 } })
        obj = self.coll.find_one({}, { '_id': 0, 'b.e': 1 })
        self.assertEqual(obj, { 'b': { 'e': [1,2,3,4] } })

    def test_project_dotted(self):
        obj = self.coll.find_one({}, { 'b.e': 1 })
        self.assertEqual(obj, { '_id': 'foo', 'b': { 'e': [ 1,2,3] } })
        obj = self.coll.find_one({}, { '_id': 0, 'b.e': 1 })
        self.assertEqual(obj, { 'b': { 'e': [ 1,2,3] } })

    def test_lt_dotted(self):
        obj = self.coll.find_one({'b.c': { '$lt': 1 } })
        self.assertEqual(obj, None)
        obj = self.coll.find_one({'b.c': { '$lt': 2 } })
        self.assertNotEqual(obj, None)

    def test_pull_dotted(self):
        self.coll.update(
            {},
            { '$pull': { 'b.f': { 'g': { '$gt': 1 } } } })
        obj = self.coll.find_one({}, { '_id': 0, 'b.f': 1 } )
        self.assertEqual(obj, { 'b': { 'f': [ {'g': 1 } ] } } )


class TestCommands(TestCase):
        
    sum_js = '''function(key,values) {
        var total = 0;
        for(var i = 0; i < values.length; i++) {
            total += values[i]; }
        return total; }'''

    first_js = 'function(key,values) { return values[0]; }'
    concat_js = 'function(key,vs) { return [].concat.apply([], vs);}'

    def setUp(self):
        self.bind = mim.Connection.get()
        self.bind.drop_all()
        self.doc = {'_id':'foo', 'a':2, 'c':[1,2,3]}
        self.bind.db.coll.insert(self.doc)

    def test_filemd5(self):
        self.assertEqual(
            dict(md5='d41d8cd98f00b204e9800998ecf8427e'),
            self.bind.db.command('filemd5'))

    def test_findandmodify_old(self):
        result = self.bind.db.command(
            'findandmodify', 'coll',
            query=dict(_id='foo'),
            update={'$inc': dict(a=1)},
            new=False)
        self.assertEqual(result['value'], self.doc)
        newdoc = self.bind.db.coll.find().next()
        self.assertEqual(newdoc['a'], 3, newdoc)
        
    def test_findandmodify_new(self):
        result = self.bind.db.command(
            'findandmodify', 'coll',
            query=dict(_id='foo'),
            update={'$inc': dict(a=1)},
            new=True)
        self.assertEqual(result['value']['a'], 3)
        newdoc = self.bind.db.coll.find().next()
        self.assertEqual(newdoc['a'], 3, newdoc)


class TestMRCommands(TestCommands):

    def setUp(self):
        super(TestMRCommands, self).setUp()
        if not self.bind.db._jsruntime:
            raise SkipTest

    def test_mr_inline(self):
        result = self.bind.db.command(
            'mapreduce', 'coll',
            map='function(){ emit(1, this.a); }',
            reduce=self.sum_js,
            out=dict(inline=1))
        self.assertEqual(result['results'], [ dict(_id=1, value=2) ])

    def test_mr_inline_date_key(self):
        dt = datetime.utcnow()
        dt = dt.replace(microsecond=123000)
        self.bind.db.date_coll.insert({'a': dt })
        result = self.bind.db.command(
            'mapreduce', 'date_coll',
            map='function(){ emit(1, this.a); }',
            reduce=self.first_js,
            out=dict(inline=1))
        self.assertEqual(result['results'][0]['value'], dt)

    def test_mr_inline_date_value(self):
        result = self.bind.db.command(
            'mapreduce', 'coll',
            map='function(){ emit(1, new Date()); }',
            reduce=self.first_js,
            out=dict(inline=1))
        self.assertEqual(result['results'][0]['_id'], 1)
        self.assert_(isinstance(result['results'][0]['value'], datetime))

    # MAP_TIMESTAMP and REDUCE_MIN_MAX are based on the recipe
    # http://cookbook.mongodb.org/patterns/finding_max_and_min_values_for_a_key
    MAP_TIMESTAMP = bson.code.Code("""
    function () {
        emit('timestamp', { min : this.timestamp,
                            max : this.timestamp } )
    }
    """)

    REDUCE_MIN_MAX = bson.code.Code("""
    function (key, values) {
        var res = values[0];
        for ( var i=1; i<values.length; i++ ) {
            if ( values[i].min < res.min )
               res.min = values[i].min;
            if ( values[i].max > res.max )
               res.max = values[i].max;
        }
        return res;
    }
    """)

    def test_mr_inline_multi_date_response(self):
        # Calculate the min and max timestamp with one mapreduce call,
        # and return a mapping containing both values.
        self.bind.db.coll.remove()
        docs = [{'timestamp': datetime(2013, 1, 1, 14, 0)},
                {'timestamp': datetime(2013, 1, 9, 14, 0)},
                {'timestamp': datetime(2013, 1, 19, 14, 0)},
                ]
        for d in docs:
            self.bind.db.date_coll.insert(d)
        result = self.bind.db.date_coll.map_reduce(
            map=self.MAP_TIMESTAMP,
            reduce=self.REDUCE_MIN_MAX,
            out={'inline': 1})
        expected = [{'value': {'min': docs[0]['timestamp'],
                               'max': docs[-1]['timestamp']},
                     '_id': 'timestamp'}]
        print 'RESULTS:', result['results']
        print 'EXPECTED:', expected
        self.assertEqual(result['results'], expected)

    def test_mr_inline_collection(self):
        result = self.bind.db.coll.map_reduce(
            map='function(){ emit(1, this.a); }',
            reduce=self.sum_js,
            out=dict(inline=1))
        self.assertEqual(result['results'], [ dict(_id=1, value=2) ])

    def test_mr_finalize(self):
        result = self.bind.db.coll.map_reduce(
            map='function(){ emit(1, this.a); }',
            reduce=self.sum_js,
            out=dict(inline=1),
            finalize='function(k, v){ return v + 42; }')
        self.assertEqual(result['results'], [ dict(_id=1, value=44) ])

    def test_mr_merge(self):
        result = self.bind.db.command(
            'mapreduce', 'coll',
            map='function(){ emit(1, this.a+1); }',
            reduce=self.sum_js,
            out=dict(merge='coll'))
        self.assertEqual(result['result'], 'coll')
        self.assertEqual(
            sorted(list(self.bind.db.coll.find())),
            sorted([ self.doc, dict(_id=1, value=3) ]))

    def test_mr_merge_collection(self):
        result = self.bind.db.coll.map_reduce(
            map='function(){ emit(1, this.a+1); }',
            reduce=self.sum_js,
            out=dict(merge='coll'))
        self.assertEqual(result['result'], 'coll')
        self.assertEqual(
            sorted(list(self.bind.db.coll.find())),
            sorted([ self.doc, dict(_id=1, value=3) ]))

    def test_mr_replace(self):
        result = self.bind.db.command(
            'mapreduce', 'coll',
            map='function(){ emit(1, this.a+1); }',
            reduce=self.sum_js,
            out=dict(replace='coll'))
        self.assertEqual(result['result'], 'coll')
        self.assertEqual(
            list(self.bind.db.coll.find()),
            [ dict(_id=1, value=3) ])

    def test_mr_replace_collection(self):
        result = self.bind.db.coll.map_reduce(
            map='function(){ emit(1, this.a+1); }',
            reduce=self.sum_js,
            out=dict(replace='coll'))
        self.assertEqual(result['result'], 'coll')
        self.assertEqual(
            list(self.bind.db.coll.find()),
            [ dict(_id=1, value=3) ])

    def test_mr_reduce(self):
        self.bind.db.reduce.insert(dict(
                _id=1, value=42))
        result = self.bind.db.command(
            'mapreduce', 'coll',
            map='function(){ emit(1, this.a+1); }',
            reduce=self.sum_js,
            out=dict(reduce='reduce'))
        self.assertEqual(result['result'], 'reduce')
        self.assertEqual(
            list(self.bind.db.reduce.find()),
            [ dict(_id=1, value=45) ])

    def test_mr_reduce_list(self):
        self.bind.db.reduce.insert(dict(
                _id=1, value=[42]))
        result = self.bind.db.command(
            'mapreduce', 'coll',
            map='function(){ emit(1, [1]); }',
            reduce=self.concat_js,
            out=dict(reduce='reduce'))
        self.assertEqual(result['result'], 'reduce')
        self.assertEqual(
            list(self.bind.db.reduce.find()),
            [ dict(_id=1, value=[1, 42]) ])

    def test_mr_reduce_collection(self):
        self.bind.db.reduce.insert(dict(
                _id=1, value=42))
        result = self.bind.db.coll.map_reduce(
            map='function(){ emit(1, this.a+1); }',
            reduce=self.sum_js,
            out=dict(reduce='reduce'))
        self.assertEqual(result['result'], 'reduce')
        self.assertEqual(
            list(self.bind.db.reduce.find()),
            [ dict(_id=1, value=45) ])

class TestCollection(TestCase):
    
    def setUp(self):
        self.bind = mim.Connection.get()
        self.bind.drop_all()

    def test_getitem_clones(self):
        test = self.bind.db.test
        test.insert({'a':'b'})
        cursor = test.find()
        doc = cursor[0]
        self.assertEqual(cursor.next(), doc)


    def test_upsert_simple(self):
        test = self.bind.db.test
        test.update(
            dict(_id=0, a=5),
            {'$set': dict(b=6) },
            upsert=True)
        doc = test.find_one()
        self.assertEqual(doc, dict(_id=0, a=5, b=6))
        
    def test_upsert_inc(self):
        test = self.bind.db.test
        test.update(
            dict(_id=0, a=5),
            {'$inc': dict(a=2, b=3) },
            upsert=True)
        doc = test.find_one()
        self.assertEqual(doc, dict(_id=0, a=7, b=3))
        
    def test_upsert_push(self):
        test = self.bind.db.test
        test.update(
            dict(_id=0, a=5),
            {'$push': dict(c=1) },
            upsert=True)
        doc = test.find_one()
        self.assertEqual(doc, dict(_id=0, a=5, c=[1]))

    def test_distinct(self):
        for i in range(5):
            self.bind.db.coll.insert({'_id':str(i), 'a':'A'})
        result = self.bind.db.coll.distinct('a')
        self.assertEqual(result, ['A'])

    def test_find_and_modify_returns_none_on_no_entries(self):
        self.assertEqual(None, self.bind.db.foo.find_and_modify({'i': 1}, {'$set': {'i': 2}}))

    def test_hint_simple(self):
        self.bind.db.coll.ensure_index([('myindex', 1)])

        cursor = self.bind.db.coll.find().hint([('$natural', 1)])
        self.assertEqual(type(cursor), type(self.bind.db.coll.find()))
        cursor = self.bind.db.coll.find().hint([('myindex', 1)])
        self.assertEqual(type(cursor), type(self.bind.db.coll.find()))
        cursor = self.bind.db.coll.find().hint('myindex')
        self.assertEqual(type(cursor), type(self.bind.db.coll.find()))
        cursor = self.bind.db.coll.find().hint(None)
        self.assertEqual(type(cursor), type(self.bind.db.coll.find()))
    
    def test_hint_invalid(self):
        self.assertRaises(OperationFailure, self.bind.db.coll.find().hint, [('foobar', 1)])
        self.assertRaises(OperationFailure, self.bind.db.coll.find().hint, 'foobar')
        self.assertRaises(TypeError, self.bind.db.coll.find().hint, 123)

class TestBsonCompare(TestCase):

    def test_boolean_bson_type(self):
        assert mim.BsonArith.cmp(True, True) == 0
        assert mim.BsonArith.cmp(True, False) == 1
        assert mim.BsonArith.cmp(False, True) == -1
        assert mim.BsonArith.cmp(False, False) == 0
        assert mim.BsonArith.cmp(False, bson.ObjectId()) == 1
        assert mim.BsonArith.cmp(True, datetime.fromordinal(1)) == -1

class TestMatch(TestCase):

    def test_simple_match(self):
        mspec = mim.match({'foo': 4}, { 'foo': 4 })
        self.assertEqual(mspec, mim.MatchDoc({'foo': 4}))

    def test_dotted_match(self):
        mspec = mim.match({'foo.bar': 4}, { 'foo': { 'bar': 4 } })
        self.assertEqual(mspec, mim.MatchDoc({'foo': mim.MatchDoc({'bar': 4}) } ))

    def test_list_match(self):
        mspec = mim.match({'foo.bar': 4}, { 'foo': { 'bar': [1,2,3,4,5] } })
        self.assertEqual(mspec, mim.MatchDoc({
                    'foo': mim.MatchDoc({'bar': mim.MatchList([1,2,3,4,5], pos=3) } ) }))
        self.assertEqual(mspec.getvalue('foo.bar.$'), 4)

    def test_elem_match(self):
        mspec = mim.match({'foo': { '$elemMatch': { 'bar': 1, 'baz': 2 } } },
                          {'foo': [ { 'bar': 1, 'baz': 2 } ] })
        self.assertIsNotNone(mspec)
        mspec = mim.match({'foo': { '$elemMatch': { 'bar': 1, 'baz': 2 } } },
                          {'foo': [ { 'bar': 1, 'baz': 1 }, { 'bar': 2, 'baz': 2 } ] })
        self.assertIsNone(mspec)

    def test_gt(self):
        spec = { 'd': { '$gt': 2 } }
        self.assertIsNone(mim.match(spec, { 'd': 1 } ))
        self.assertIsNone(mim.match(spec, { 'd': 2 } ))
        self.assertIsNotNone(mim.match(spec, { 'd': 3 } ))

    def test_gte(self):
        spec = { 'd': { '$gte': 2 } }
        self.assertIsNone(mim.match(spec, { 'd': 1} ))
        self.assertIsNotNone(mim.match(spec, { 'd': 2 } ))
        self.assertIsNotNone(mim.match(spec, { 'd': 3} ))

    def test_lt(self):
        spec = { 'd': { '$lt': 2 } }
        self.assertIsNotNone(mim.match(spec, { 'd': 1 } ))
        self.assertIsNone(mim.match(spec, { 'd': 2 } ))
        self.assertIsNone(mim.match(spec, { 'd': 3 } ))

    def test_lte(self):
        spec = { 'd': { '$lte': 2 } }
        self.assertIsNotNone(mim.match(spec, { 'd': 1 } ))
        self.assertIsNotNone(mim.match(spec, { 'd': 2 } ))
        self.assertIsNone(mim.match(spec, { 'd': 3 } ))

    def test_range(self):
        doc = { 'd': 2 }
        self.assertIsNotNone(mim.match({'d': { '$gt': 1, '$lt': 3 } }, doc))
        self.assertIsNone(mim.match({'d': { '$gt': 1, '$lt': 2 } }, doc))
        self.assertIsNotNone(mim.match({'d': { '$gt': 1, '$lte': 2 } }, doc))

    def test_exists(self):
        doc = { 'd': 2 }
        self.assertIsNotNone(mim.match({'d': { '$exists': 1 } }, doc))
        self.assertIsNone(mim.match({'d': { '$exists': 0 } }, doc))
        self.assertIsNone(mim.match({'e': { '$exists': 1 } }, doc))
        self.assertIsNotNone(mim.match({'e': { '$exists': 0 } }, doc))

    def test_all(self):
        doc = { 'c': [ 1, 2 ] }
        self.assertIsNotNone(mim.match({'c': {'$all': [] } }, doc))
        self.assertIsNotNone(mim.match({'c': {'$all': [1] } }, doc))
        self.assertIsNotNone(mim.match({'c': {'$all': [1, 2] } }, doc))
        self.assertIsNone(mim.match({'c': {'$all': [1, 2, 3] } }, doc))

    def test_or(self):
        doc = { 'd': 2 }
        self.assertIsNotNone(mim.match(
                {'$or': [ { 'd': 1 }, { 'd': 2 } ] },
                doc))
        self.assertIsNone(mim.match(
                {'$or': [ { 'd': 1 }, { 'd': 3 } ] },
                doc))

    def test_traverse_list(self):
        doc = { 'a': [ { 'b': 1 }, { 'b': 2 } ] }
        self.assertIsNotNone(mim.match( {'a.b': 1 }, doc))
