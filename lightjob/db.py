import sys
import os
from tinydb import TinyDB, where, Query
from blitzdb import Document, FileBackend
from utils import summarize, recur_update
from tinyrecord import transaction
import logging
from datetime import datetime

logger = logging.getLogger(__name__)
handler = logging.StreamHandler(stream=sys.stdout)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


DBFILENAME = "db.json"
STATES = AVAILABLE, RUNNING, SUCCESS, ERROR, PENDING = "available", "running", "success", "error", "pending"
IDKEY = 'summary'


class GenericDB(object):

    def __init__(self):
        self.loaded = False

    def load(self, filename, db_filename=DBFILENAME, idkey=IDKEY):
        assert self.loaded is False, "Already loaded"
        if os.path.isdir(filename):
            filename = os.path.join(filename, db_filename)
        self.load_from_file(filename)
        self.idkey = idkey

    def load_from_file(self, filename):
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
        self.insert(dict(state=state, content=d, summary=s, **meta))
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


class Job(Document):
    class Meta(Document.Meta):
        primary_key = IDKEY # TODO should depend on self.idkey


class BlitzDBWrapper(GenericDB):

    def load_from_file(self, filename):
        self.db = FileBackend(filename)

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


class TinyDBWrapper(object):

    def __init__(self):
        self.db = None
        self.job_table = None

    def load(self, filename, db_filename=DBFILENAME):
        assert self.db is None, "Already loaded"
        if os.path.isdir(filename):
            filename = os.path.join(filename, db_filename)
        self.db = TinyDB(filename)
        self.jobs = self.db.table('Job')

    def close(self):
        self.db.close()

    def purge(self):
        self.db.purge()

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
        with transaction(self.jobs) as tr:
            out = tr.insert(dict(state=state, content=d, summary=summarize(d), **meta))
        return out

    def all_jobs(self):
        return self.jobs.all()

    def jobs_with(self, **kw):
        Job = Query()
        q = [getattr(Job, k) == v for k, v in kw.items()]
        if len(q) > 0:
            q = reduce(lambda a, b: a & b, q)
            return self.jobs.search(q)
        else:
            return self.jobs.all()

    def jobs_with_state(self, state):
        Job = Query()
        return self.jobs.search(Job.state == state)

    def modify_state_by_query(self, q, state):
        with transaction(self.jobs) as tr:
            tr.update({"state": state}, q)

    def get_state_of(self, summary):
        return self.get_job_by_summary(summary)["state"]

    def modify_state_of(self, summary, state):
        Job = Query()
        with transaction(self.jobs) as tr:
            tr.update({"state": state}, Job.summary == summary)

    def job_exists(self, d):
        return self.job_exists_by_summary(summarize(d))

    def job_update(self, s, values):
        Job = Query()
        with transaction(self.jobs) as tr:
            tr.update(values, Job.summary == s)

    def job_exists_by_summary(self, s):
        Job = Query()
        return True if len(self.jobs.search(Job.summary == s)) else False

    def get_job_by_summary(self, s):
        Job = Query()
        jobs = self.jobs.search(Job.summary == s)
        if len(jobs) == 1:
            return jobs[0]
        else:
            return None


DB = BlitzDBWrapper

def migrate(source_db, dest_db):
    for o in source_db.all_jobs():
        dest_db.insert(o)
