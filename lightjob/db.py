import sys
import os
from tinydb import TinyDB, where, Query
from utils import summarize
import tinyrecord
import logging

logger = logging.getLogger(__name__)
handler = logging.StreamHandler(stream=sys.stdout)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


DBFILENAME = "db.json"
STATES = AVAILABLE, RUNNING, SUCCESS, ERROR = "available", "running", "success", "error"


class DB(object):

    def __init__(self):
        self.db = None
        self.job_table = None

    def load(self, filename, db_filename=DBFILENAME):
        assert self.db is None, "Already loaded"
        if os.path.isdir(filename):
            filename = os.path.join(filename, db_filename)
        self.db = TinyDB(filename)
        self.jobs = self.db.table('Job')

    def safe_add_job(self, d, **kw):
        if self.job_exists(d):
            logger.error("Error during adding Job {} : it already exists, canceling.".format(summarize(d)))
            return 0
        self.add_job(d, **kw)
        return 1

    def add_job(self, d, state=AVAILABLE, **meta):
        return self.jobs.insert(dict(state=state, content=d, summary=summarize(d), **meta))

    def all_jobs(self):
        return self.jobs.all()

    def jobs_with_state(self, state):
        Job = Query()
        return self.jobs.search(Job.state == state)

    def modify_state_by_query(self, q, state):
        self.jobs.update({"state": state}, q)

    def get_state_of(self, summary):
        return self.get_job_by_summary(summary)["state"]

    def modify_state_of(self, summary, state):
        Job = Query()
        self.jobs.update({"state": state}, Job.summary == summary)

    def job_exists(self, d):
        return self.job_exists_by_summary(summarize(d))

    def job_update(self, s, values):
        Job = Query()
        self.jobs.update(values, Job.summary == s)

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
