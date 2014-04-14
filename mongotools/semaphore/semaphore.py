
class Semaphore(object):

    """docstring for Semaphore"""
    def __init__(self, db, id, counter, value, collection_name='mongotools.semaphore'):
        self._db = db
        self._name = collection_name
        self._id = id
        self._counter = counter
        self._db[self._name].update(dict(_id=id), {'$setOnInsert':{counter:value}}, upsert=True)

    def acquire(self):
        doc = self._db[self._name].update({'_id':self._id, self._counter:{'$gt':0}}, {'$inc': {self._counter:-1} }, w=1)
        return doc['updatedExisting']

    def release(self):
        self._db[self._name].update({'_id':self._id}, {'$inc': {self._counter:1}})

