from datetime import datetime

#sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from ..db import IDKEY, CONTENTKEY, STATEKEY, LIFEKEY, AVAILABLE
from ..utils import summarize
from ..utils import dict_format

class GenericDB(object):

    def __init__(self, summarize=summarize,
                 idkey=IDKEY, contentkey=CONTENTKEY, statekey=STATEKEY, lifekey=LIFEKEY,
                 dict_format=dict_format):
        self.summarize = summarize
        self.idkey = idkey
        self.contentkey = contentkey
        self.statekey = statekey
        self.lifekey = lifekey
        self.dict_format = dict_format #TODO remove dict_format from __init__

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
