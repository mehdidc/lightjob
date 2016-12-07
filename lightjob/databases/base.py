from datetime import datetime

from ..db import IDKEY, CONTENTKEY, STATEKEY, LIFEKEY, AVAILABLE
from ..utils import summarize
from ..utils import dict_format

class GenericDB(object):
    """
    base class for databases.

    Parameters
    ----------

    summarize: callable, optional
        hash function to use in order to set the value of `idkey` based
        on the content defined by the value of `contentkey`
    idkey: str, optional[default=IDKEY]
        key to use for the id of the jobs
    contentkey: str, optional[default=CONTENTKEY]
        key to use for the jobs
    statekey: str, optional[default=STATEKEY]
        key to use for the state of the jobs
    lifekey: str, optional[default=LIFEKEY]
        key to use for the life of the jobs.
        the life of the jobs is a list of states
        (labeled by their datetime) that a job passed
        through.
    dict_format : callable, optional[default=utils.dict_format]
        SHOULD REMOVE THIS
    """
    def __init__(self,
                 summarize=summarize,
                 idkey=IDKEY,
                 contentkey=CONTENTKEY,
                 statekey=STATEKEY,
                 lifekey=LIFEKEY,
                 dict_format=dict_format):
        self.summarize = summarize
        self.idkey = idkey
        self.contentkey = contentkey
        self.statekey = statekey
        self.lifekey = lifekey
        self.dict_format = dict_format #TODO remove dict_format from __init__

    def load(self, dirname):
        """
        load a db from a `dirname`.
        example of a dirname to use : /path/.lightjob
        """
        self.load_from_dir(dirname)

    def load_from_dir(self, dirname):
        """load a job from a dirname"""
        raise NotImplementedError()

    def insert_list(self, l):
        """
        insert a list of job contents into the db

        Parameters
        ----------

        l : list of dicts
        """
        raise NotImplementedError()

    def insert(self, d):
        """
        insert a job content into the db

        Parameters
        ----------

        d : dict
        """
        raise NotImplementedError()

    def get(self, d):
        """
        get a job corresponding to fields defined in d.

        Parameters
        ----------

        d : dict
            the dictionary that we want to match with
            the jobs in the db

        Returns
        -------

        iterator of dicts

        """
        raise NotImplementedError()

    def get_by_id(self, id_):
        """
        get a job based on its id

        Parameters
        ----------

        id_ : str

        Returns
        -------

        dict
        """
        raise NotImplementedError()

    def update(self, d, id):
        """
        update a job

        Parameters
        ----------

        d : dict
            fields to update
        id: str
            id of job to update
        """
        raise NotImplementedError()

    def close(self):
        """close a db"""
        raise NotImplementedError()

    def delete(self, d):
        """
        delete all jobs corresponding to the fields
        defined in d

        Parameters
        ----------

        d: dict
            fields to match the jobs you want to delete
        """
        raise NotImplementedError()

    def safe_add_job(self, d, **meta):
        """
        insert a job into the db safely.
        safely means it makes sure that it does not
        insert duplicates : jobs with the same content.
        `meta` are used to insert meta fields.
        `meta` fields are all fields that do not define
        the content of the job (don't affect the hash of the
        content (computed by `summarize`) of the job which defines
        its identity).

        Parameters
        ----------

        d : dict
            job content

        meta: kwargs
            meta fields

        Returns
        -------

        int : number of newly inserted jobs (either 0 or 1).
            - 0 if the content has been detected to be duplicate
            - 1 if the content is new
        """
        if self.job_exists(d):
            return 0
        self.add_job(d, **meta)
        return 1

    def safe_add_or_update_job(self, d, **meta):
        """
        safely add or update job.
        if the job does not exist, insert it.
        if it does exist, update it.
        `meta` are used to insert meta fields.
        `meta` fields are all fields that do not define
        the content of the job (don't affect the hash of the
        content (computed by `summarize`) of the job which defines
        its identity).

        Parameters
        ----------

        d : dict
            job content to insert/update
        meta : kwargs
            meta fields

        Returns
        -------

        int : number of newly inserted jobs (either 0 or 1).
            - 0 if the content has been detected to be duplicate
            - 1 if the content is new
        """
        if self.job_exists(d):
            u = {}
            u.update(meta)
            u[self.contentkey] = d
            s = self.summarize(d)
            self.job_update(s, u)
            return 0
        self.add_job(d, **meta)
        return 1

    def add_job(self, d, state=AVAILABLE, **meta):
        """
        add a job with the RISK of inserting a duplicate job.
        use `safe_add_job` to avoid this behaviour.
        `meta` are used to insert meta fields.
        `meta` fields are all fields that do not define
        the content of the job (don't affect the hash of the
        content (computed by `summarize`) of the job which defines
        its identity).

        Parameters
        ----------

        d : dict
            job content to insert

        state: str[default=AVAILABLE]
            starting state of the job
        meta : kwargs
            meta fields
        """
        s = self.summarize(d)
        D = {self.statekey: state, self.contentkey: d, self.idkey: s, self.lifekey: []}
        D.update(meta)
        self.insert(D)
        self.modify_state_of(s, state)
        return s

    def all_jobs(self):
        """
        roundeturn all jobs

        Returns
        -------

        iterator of dicts

        """
        return self.get({})

    def jobs_with(self, **kw):
        """
        Return all jobs that match the fields defined in
        the kwargs
        """
        return self.get(kw)

    def jobs_filter(self, fn, **kw):
        """
        Return all jobs that match the fields defined in
        the kwargs and filtered by `fn` using python `filter`
        function.

        Parameters
        ----------
        fn : callable
            callable to use for filtering
        kw : kwargs
            fields to match before filtering

        Returns
        -------

        iterator of dicts
        """
        return filter(fn, self.get(kw))

    def jobs_with_state(self, state):
        """
        Return all jobs with self.statekey==state

        Parameters
        ----------
        state : str
            state to match

        Returns
        -------

        iterator of dicts
        """
        return self.get({self.statekey: state})

    def get_state_of(self, summary):
        """ get the state of a job for which the `id` is `summary`. """
        return self.get_job_by_summary(summary)[self.statekey]

    def modify_state_of(self, summary, state, dt=None):
        """
        Modify the state of the job for which the `id` is `summary`.

        Parameters
        ----------

        summary: str
            id of the job
        state : str
            new state of the job
        dt : datetime, optional[default=datetime.now()]
            datetime to associate with the new state of the job.
            if it is not provided, it uses datetime.now().
        """
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

    def job_update(self, s, values):
        """
        update a job

        Parameters
        ----------

        s : str
            id of the job to update
        values : dict
            fields to update
        """
        self.update(values, s)

    def get_values(self, field, **meta):
        """
        get the values of a field for all the jobs matching
        the meta fields (if provided, otherwies use all
        the jobs)

        Parameters
        ----------

        field : str
            field in the form of field1.field2.field3...etc
            the '.' means going deep in the dict hierarchy.

        """
        jobs = self.jobs_with(**meta)
        for j in jobs:
            try:
                value = self.get_value(j, field)
            except ValueError:
                continue
            else:
                yield {field: value, 'job': j}

    def get_value(self, job, field, dict_format=dict_format, **kw):
        """
        get the value of a field in a job content

        Parameters
        ----------

        job : dict
            content to use
        field : str
            field in the form of field1.field2.field3...etc
            the '.' means going deep in the dict hierarchy.
        dict_format : callable
            dict_format callable to get the field from a dictionary
        """
        return dict_format(job, field, db=self, **kw)

    def job_exists(self, d):
        """return True if the content in `d` matches exactly the content of an existing job """
        return self.job_exists_by_summary(self.summarize(d))

    def job_exists_by_summary(self, s):
        """ return True if the job with the `id` defined by `s` exists"""
        return True if self.get_by_id(s) is not None else False

    def get_job_by_summary(self, s):
        """
        returns the content of a job based on its `id` defined by `s`

        Parameters
        ----------

        s : str
            id of the job

        Returns
        -------
        
        dict : content of the job
        """
        return self.get_by_id(s)
