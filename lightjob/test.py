import os, shutil
from lightjob.db import DB, AVAILABLE, SUCCESS, RUNNING, ERROR
from lightjob.utils import summarize

testdir = ".db"

class Test(object):

    def setUp(self):
        if os.path.exists(testdir):
            shutil.rmtree(testdir)
        os.mkdir(testdir)
        db = DB()
        db.load(testdir)
        self.db = db

    def tearDown(self):
        shutil.rmtree(testdir)

    def test_add(self):
        d = {'a': 1, 'b': 2}
        self.db.add_job(d)
        jobs = self.db.all_jobs()
        jobs = list(jobs)
        assert len(jobs) == 1
        j = jobs[0]
        assert j['content'] == d
        assert len(j['life']) == 1

    def test_life(self):
        d = {'a': 1, 'b': 2}
        self.db.add_job(d)
        self.db.modify_state_of(summarize(d), RUNNING)
        self.db.modify_state_of(summarize(d), ERROR)
        self.db.modify_state_of(summarize(d), RUNNING)
        self.db.modify_state_of(summarize(d), SUCCESS)
        j = self.db.get_job_by_summary(summarize(d))
        life = j['life']
        assert len(life) == 5
        assert [l['state'] for l in life] == [AVAILABLE, RUNNING, ERROR, RUNNING, SUCCESS]
