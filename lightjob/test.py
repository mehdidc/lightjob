import os, shutil
from lightjob.db import DB, AVAILABLE, SUCCESS, RUNNING, ERROR
from lightjob.utils import summarize
from db import Blitz, Dataset
from tempfile import mkdtemp


class BaseTest(object):

    def setUp(self):
        self.testdir = mkdtemp(suffix='lightjob')
        db = DB(backend=self.backend)
        db.load(self.testdir)
        self.db = db

    def tearDown(self):
        shutil.rmtree(self.testdir)

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

    def test_exists(self):
        d = {'a': 1, 'b': 2}
        s = summarize(d)
        self.db.add_job(d)
        assert self.db.job_exists_by_summary(s)

    def test_with(self):
        d = {'a': 1, 'b': 2}
        self.db.add_job(d, x=1, y=2)
        d = {'a': 1, 'b': 3}
        self.db.add_job(d, x=1, y=2)
        d = {'a': 2, 'b': 4}
        self.db.add_job(d, x=3, y=2)
        jobs = self.db.jobs_with(x=1)
        assert len(jobs) == 2


def with_backend(cls, backend):
    class C(cls):
        pass
    C.__name__ = 'Test' + str(backend)
    C.backend = backend
    return C

TestBlitz = with_backend(BaseTest, backend=Blitz)
TestDataset = with_backend(BaseTest, backend=Dataset)


if __name__ == '__main__':
    pass
