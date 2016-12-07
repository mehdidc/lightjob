import os
import json

import h5py

from .base import  GenericDB

from ..utils import recur_update
from ..utils import match

class H5py(GenericDB):

    def load_from_dir(self, dirname):
        self.db = h5py.File(os.path.join(dirname, 'db.hdf5'))

    def insert(self, d):
        self.db.attrs[d[self.idkey]] = json.dumps(d, default=date_handler)

    def insert_list(self, l):
        for j in l:
            self.insert(j)

    def get_by_id(self, id_):
        d = self.db.attrs.get(id_, None)
        return json.loads(d) if d else None

    def delete(self, d):
        del self.db.attrs[d[self.idkey]]

    def get(self, d):
        o = map(json.loads, self.db.attrs.values())
        o = filter(lambda v: match(v, d), o)
        return o

    def update(self, d, id_):
        obj = self.get_by_id(id_)
        recur_update(obj, d)
        if obj is not None:
            self.db.attrs[id_] = json.dumps(obj, default=date_handler)
            return True
        else:
            return False

    def close(self):
        self.db.close()

def date_handler(obj):
    if hasattr(obj, 'isoformat'):
        return obj.isoformat()
    else:
        raise TypeError
