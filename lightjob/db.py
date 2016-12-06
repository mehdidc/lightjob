import sys
import os
from datetime import datetime
import json
import collections

from blitzdb import Document, FileBackend
import dataset
import h5py

from .utils import summarize, recur_update, dict_format, match, flatten_dict

DBFILENAME = 'db.json'
STATES = AVAILABLE, RUNNING, SUCCESS, ERROR, PENDING, DELETED = 'available', 'running', 'success', 'error', 'pending', 'deleted'
IDKEY = 'summary'
CONTENTKEY = 'content'
STATEKEY = 'state'
LIFEKEY = 'life'

class GenericDB(object):

    def __init__(self, summarize=summarize, idkey=IDKEY, contentkey=CONTENTKEY, statekey=STATEKEY, lifekey=LIFEKEY, dict_format=dict_format):
        self.summarize = summarize
        self.idkey = idkey
        self.contentkey = contentkey
        self.statekey = statekey
        self.lifekey = lifekey
        self.dict_format = dict_format

    def load(self, dirname):
        self.load_from_dir(dirname)

    def load_from_dir(self, dirname):
        raise NotImplementedError()

    def insert_list(self, l):
        raise NotImplementedError()

    def insert(self, d):
        raise NotImplementedError()

    def get(self, d):
        raise NotImplementedError()

    def get_by_id(self, id_):
        raise NotImplementedError()

    def update(self, d, id):
        raise NotImplementedError()

    def close(self):
        raise NotImplementedError()

    def delete(self, d):
        raise NotImplementedError()

    def safe_add_job(self, d, **kw):
        if self.job_exists(d):
            return 0
        self.add_job(d, **kw)
        return 1

    def safe_add_or_update_job(self, d, **kw):
        if self.job_exists(d):
            u = {}
            u.update(kw)
            u[self.contentkey] = d
            s = self.summarize(d)
            self.job_update(s, u)
            return 0
        self.add_job(d, **kw)
        return 1

    def add_job(self, d, state=AVAILABLE, **meta):
        s = self.summarize(d)
        D = {self.statekey: state, self.contentkey: d, self.idkey: s, self.lifekey: []}
        D.update(meta)
        self.insert(D)
        self.modify_state_of(s, state)
        return s

    def all_jobs(self):
        return self.get({})

    def jobs_with(self, **kw):
        return self.get(kw)

    def jobs_filter(self, fn, **kw):
        return filter(fn, self.get(kw))

    def jobs_with_state(self, state):
        return self.get({self.statekey: state})

    def get_state_of(self, summary):
        return self.get_job_by_summary(summary)[self.statekey]

    def modify_state_of(self, summary, state, dt=None):
        self.update({self.statekey: state}, summary)
        if dt is None:
            dt = datetime.now()
        j = self.get_job_by_summary(summary)
        if self.lifekey in j:
            life = j[self.lifekey]
        else:
            life = []
        life.append({self.statekey: state, 'dt': dt})
        self.update({self.lifekey: life}, summary)

    def job_exists(self, d):
        return self.job_exists_by_summary(self.summarize(d))

    def job_update(self, s, values):
        self.update(values, s)

    def get_values(self, field, **kw):
        jobs = self.jobs_with(**kw)
        for j in jobs:
            s = j[self.idkey]
            try:
                value = self.get_value(j, field)
            except ValueError:
                continue
            else:
                yield {field: value, 'job': j}

    def get_value(self, job, field, dict_format=dict_format, **kw):
        return dict_format(job, field, db=self, **kw)

    def job_exists_by_summary(self, s):
        return True if self.get_by_id(s) is not None else False

    def get_job_by_summary(self, s):
        return self.get_by_id(s)

    def close(self):
        self.db.close()

class Job(Document):
    class Meta(Document.Meta):
        primary_key = IDKEY # TODO should depend on self.idkey

class Blitz(GenericDB):

    def load_from_dir(self, dirname):
        self.db = FileBackend(os.path.join(dirname, DBFILENAME))

    def insert(self, d):
        self.insert_list([d])

    def insert_list(self, l):
        for j in l:
            Job(j).save(self.db)
        self.db.commit()

    def get_by_id(self, id_):
        try:
            return self.db.get(Job, {self.idkey: id_})
        except Job.DoesNotExist:
            return None

    def delete(self, d):
        for el in self.get(d):
            self.db.delete(el)

    def get(self, d):
        return self.db.filter(Job, d)

    def update(self, d, id_):
        obj = self.get_by_id(id_)
        recur_update(obj, d)
        if obj is not None:
            obj.save(self.db)
            self.db.commit()
            return True
        else:
            return False

    def close(self):
        pass

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

def DB(backend=Blitz, **kw):
    if type(backend) in (str, unicode):
        backend = {'Blitz': Blitz, 'Dataset': Dataset, 'H5py': H5py}.get(backend, Blitz)
    return backend(**kw)
