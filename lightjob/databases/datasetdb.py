import dataset
import json

from .base import  GenericDB

class Dataset(GenericDB):

    def load_from_dir(self, dirname):
        filename = 'sqlite:///{}/db'.format(dirname)
        self.db = dataset.connect(filename)
        self.table = self.db['table']

    def insert(self, d):
        self.table.insert(self._preprocess(d))

    def _preprocess(self, d):
        return {k: self._preprocess_element(v) for k, v in d.items()}

    def _preprocess_element(self, d):
        if isinstance(d, dict) or type(d) == list:
            return json.dumps(d, default=date_handler)
        else:
            return d

    def _deprocess(self, d):
        return {k: self._deprocess_element(v) for k, v in d.items()}

    def _deprocess_element(self, d):
        try:
            return json.loads(d)
        except Exception:
            return d

    def insert_list(self, l):
        self.db.begin()
        for d in l:
            self.table.insert(self._preprocess(d))
        self.db.commit()

    def get_by_id(self, id_):
        j = self.table.find_one(summary=id_)
        if j is None:
            return None
        else:
            return self._deprocess(j)

    def delete(self, d):
        d = self._preprocess(d)
        self.table.delete(**d)

    def get(self, d):
        d = self._preprocess(d)
        return map(self._deprocess, self.table.find(**d))

    def update(self, d, id_):
        d = self._preprocess(d)
        d[self.idkey] = id_
        self.table.update(d, [self.idkey])

    def close(self):
        pass

def date_handler(obj):
    if hasattr(obj, 'isoformat'):
        return obj.isoformat()
    else:
        raise TypeError
