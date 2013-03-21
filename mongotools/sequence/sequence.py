class Sequence(object):

    def __init__(self, db, name='mongotools.sequence'):
        self._db = db
        self._name = name

    def cur(self, name):
        doc = self._db[self._name].find_one({'_id': name})
        if doc is None: return 0
        return doc['value']

    def next(self, sname, inc=1):
        doc = self._db[self._name].find_and_modify(
            query={'_id': sname},
            update={'$inc': { 'value': inc } },
            upsert=True,
            new=True)
        return doc['value']
