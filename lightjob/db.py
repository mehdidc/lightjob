import sys
import os
from blitzdb import Document, FileBackend
import dataset
from utils import summarize, recur_update
import logging
from datetime import datetime
import json

logger = logging.getLogger(__name__)
handler = logging.StreamHandler(stream=sys.stdout)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


DBFILENAME = "db.json"
STATES = AVAILABLE, RUNNING, SUCCESS, ERROR, PENDING, DELETED = "available", "running", "success", "error", "pending", "deleted"
IDKEY = 'summary'


class GenericDB(object):

    def __init__(self):
        self.loaded = False

    def load(self, filename, idkey=IDKEY):
        assert self.loaded is False, "Already loaded"
        self.load_from_dir(filename)
        self.idkey = idkey

    def load_from_dir(self, filename):
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
            logger.error("Error during adding Job {} : it already exists, canceling.".format(summarize(d)))
            return 0
        self.add_job(d, **kw)
        return 1

    def safe_add_or_update_job(self, d, **kw):
        if self.job_exists(d):
            u = {}
            u.update(kw)
            u['content'] = d
            s = summarize(d)
            self.job_update(s, u)
            return 0
        self.add_job(d, **kw)
        return 1

    def add_job(self, d, state=AVAILABLE, **meta):
        s = summarize(d)
        self.insert(dict(state=state, content=d, summary=s, life=[], **meta))
        self.modify_state_of(s, state)

    def all_jobs(self):
        return self.get(dict())

    def jobs_with(self, **kw):
        return self.get(kw)

    def jobs_with_state(self, state):
        return self.get(dict(state=state))

    def get_state_of(self, summary):
        return self.get_job_by_summary(summary)["state"]

    def modify_state_of(self, summary, state, dt=None):
        self.update(dict(state=state), summary)
        if dt is None:
            dt = datetime.now()
        j = self.get_job_by_summary(summary)
        if "life" in j:
            life = j["life"]
        else:
            life = []
        life.append(dict(state=state, dt=dt))
        self.update(dict(life=life), summary)

    def job_exists(self, d):
        return self.job_exists_by_summary(summarize(d))

    def job_update(self, s, values):
        self.update(values, s)

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

    def load_from_dir(self, filename):
        self.db = FileBackend(os.path.join(filename, DBFILENAME))

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
        self.loaded = False


class Dataset(GenericDB):

    def load_from_dir(self, filename):
        filename = 'sqlite:///{}/db'.format(filename)
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
        self.loaded = False

def date_handler(obj):
    if hasattr(obj, 'isoformat'):
        return obj.isoformat()
    else:
        raise TypeError

def DB(backend=Blitz, **kw):
    if type(backend) in (str, unicode):
        backend = {'Blitz': Blitz, 'Dataset': Dataset}.get(backend, Blitz)
    return backend(**kw)
