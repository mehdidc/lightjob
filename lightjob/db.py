import sys
import os
from tinydb import TinyDB, where, Query
from utils import summarize

import logging

logger = logging.getLogger(__name__)
handler = logging.StreamHandler(stream=sys.stdout)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


DBFILENAME = "db.json"
STATES = AVAILABLE, RUNNING, FINISHED, ERROR = "available", "running", "success", "error"

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

    def safe_add_job(self, d):
        if self.job_exists(d):
            logger.error("Error during adding Job {} : it already exists, canceling.".format(summarize(d)))
            return
        self.add_job(d)

    def add_job(self, d):
        self.jobs.insert(dict(state=AVAILABLE, content=d, summary=summarize(d)))
    
    def jobs_with_state(self, state):
        Job = Query()
        return self.jobs.search(Job.state == state)
    
    def modify_state(self, q, state):
        self.jobs.update({"state": state}, q)

    def job_exists(self, d):
        s = summarize(d)
        Job = Query()
        return True if len(self.jobs.search(Job.summary == s)) else False
