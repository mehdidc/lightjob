import shutil
from tempfile import mkdtemp

from lightjob.db import DB
from lightjob.db import AVAILABLE, SUCCESS, RUNNING, ERROR
from lightjob.databases import Blitz, Dataset, H5py
from lightjob.utils import summarize


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
        assert self.db.job_exists(d)

    def test_with(self):
        d = {'a': 1, 'b': 2}
        self.db.add_job(d, x=1, y=2)
        d = {'a': 1, 'b': 3}
        self.db.add_job(d, x=1, y=2)
        d = {'a': 2, 'b': 4}
        self.db.add_job(d, x=3, y=2)
        jobs = list(self.db.jobs_with(x=1))
        assert len(jobs) == 2

    def test_safe_add(self):
        d = {'a': 1, 'b': 2}
        assert self.db.safe_add_job(d) == 1
        assert self.db.safe_add_job(d) == 0


def with_backend(cls, backend):
    class C(cls):
        pass
    C.__name__ = 'Test' + str(backend)
    C.backend = backend
    return C


TestBlitz = with_backend(BaseTest, backend=Blitz)
TestDataset = with_backend(BaseTest, backend=Dataset)
TestH5py = with_backend(BaseTest, backend=H5py)

if __name__ == '__main__':
    pass
