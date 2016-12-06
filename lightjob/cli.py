import sys
import os
import click
from utils import mkdir_path, backward_search
from utils import dict_format as default_dict_format
from db import DB
import logging
import json
import pandas as pd
from dateutil import parser
import math

import importlib

logger = logging.getLogger(__name__)
handler = logging.StreamHandler(stream=sys.stdout)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

DOTDIR = ".lightjob"


@click.group()
def main():
    pass


@click.command()
@click.option('--force', default=False, help='Force init if exists', required=False)
@click.option('--purge', default=False, help='Force purge database (WARNING : dangerous!)', required=False)
@click.option('--backend', default='Blitz', help='Blitz/Dataset/H5py', required=False)
def init(force, purge, backend):
    folder = './{}'.format(DOTDIR)
    if os.path.exists(folder):
        if not force:
            logger.error("{} Already exists, cancel init".format(folder))
            return
    mkdir_path(folder)
    logger.info("Init on : {}".format(folder))
    rcfilename = open(os.path.join(folder, '.lightjobrc'), 'w')
    rcfilename.write('{"backend": "%s"}' % (backend,))
    rcfilename.close()
    db = DB(backend=backend)
    db.load(folder)
    if purge:
        db.purge()

@click.command()
@click.option('--filename', default='db.json', help='filename', required=False)
def dump(filename):
    db = load_db()
    kw = {}
    jobs = db.jobs_with(**kw)
    jobs = list(jobs)
    with open(filename, 'w') as fd:
        json.dump(jobs, fd, indent=2)

@click.command()
@click.option('--state', default=None, help='state', required=False)
@click.option('--type', default=None, help='type', required=False)
@click.option('--where', default=None, help='where', required=False)
@click.option('--details/--no-details', default=False, help='show with details', required=False)
@click.option('--fields', default='', help='show values of fields separated by comma', required=False)
@click.option('--summary', default='', help='show a specific job', required=False)
@click.option('--sort', default='', help='sort by some field or time', required=False)
@click.option('--export/--no-export', default=False, help='export to json', required=False)
@click.option('--ascending/--descending', default=True, help='orde of showing the sorted events', required=False)
@click.option('--show-fields/--no-show-fields', default=True, help='orde of showing the sorted events', required=False)
def show(state, type, where, details, fields, summary, sort, export, ascending, show_fields): 
    try:
        from tabulate import tabulate
    except ImportError:
        tabulate = lambda x:x
    import pprint
    from joblib import Memory
    db = load_db()
    params = get_db_params()
    if 'dict_format' in params:
        dict_format = params['dict_format']
        sys.path.append(os.getcwd())
        s = dict_format.split('.')
        module = '.'.join(s[0:-1])
        name = s[-1]
        dict_format = getattr(importlib.import_module(module), name)
    else:
        dict_format = default_dict_format

    if fields:
        def format_job(j):
            vals = []
            for field in fields.split(','):
                try:
                    val = dict_format(j, field, db=db)
                except ValueError:
                    val = 'not_found'
                vals.append(val)
                j[field] = val
            if show_fields:
                return map(str, vals)
            else:
                return pprint.pprint(j, indent=4)
            
    else: 
        if details:
            format_job = lambda j:pprint.pprint(j, indent=4)
        else:
            format_job = lambda j:j['summary']
    kw = {}
    if summary:
        kw['summary'] = summary
    if state is not None:
        kw["state"] = state
    if type is not None:
        kw["type"] = type
    if where is not None:
        kw['where'] = where
    jobs = db.jobs_with(**kw)

    def get_last(filter_func, L, default='none'):
        for el in L[::-1]:
            if filter_func(el):
                return el
        return default

    def parse_time(j, tag='start', default='2000-01-01 00:00:00.00000'):
        if 'life' in j and j['life']:
            life = j['life']
            if tag == 'end':
                moment = get_last(lambda l:l['state']=='success', life, default={'dt':default})
                dt = moment['dt']
            elif tag == 'start':
                moment = get_last(lambda l:l['state']=='running', life, default={'dt': default})
                dt = moment['dt']
            else:
                raise Exception('invalid tag : {}'.format(tag))
            dt = parser.parse(dt)
            return dt
        else:
            return parser.parse(default)
    jobs = list(jobs)
    for j in jobs:
        j['start_time'] = parse_time(j, tag='start')
        j['end_time'] = parse_time(j, tag='end')
        j['readable_start_time'] = str(j['start_time'])
        j['readable_end_time'] = str(j['end_time'])
        try:
            j['duration'] = j['end_time'] - j['start_time']
        except Exception:
            j['duration'] = 'none'
    if sort:
        infty = float('inf') if ascending else -float('inf')
        def key(j):
            try:
                val = dict_format(j, sort, db=db)
            except Exception:
                return infty
            else:
                if val and isinstance(val, float) and math.isnan(val):
                    return infty
                elif val and isinstance(val, str) or isinstance(val, unicode):
                    return infty
                else:
                    return val
        if not ascending:
            key_ = key
            key = lambda j:-key_(j)
        jobs = sorted(jobs, key=key)
    if details:
        logger.info("Number of jobs : {}".format(len(jobs)))
    
    if fields != '':
        header = [fields.split(',')]
    else:
        header = []

    if export:
        for j in jobs:
            fd = open(j['summary'] + '.json', 'w')
            fd.write(json.dumps(j['content'], indent=2))
            fd.close()

    jobs = map(format_job, jobs)
    if fields != '' and show_fields:
        print(tabulate(header + jobs))
    else:
        for j in jobs:
            print(j)


@click.command()
@click.option('--state', help='state', required=True)
@click.option('--details', help='details', required=False, type=bool, default=True)
@click.option('--dryrun', help='dry run', required=True, type=bool)
@click.argument('jobs', nargs=-1, required=True)
def update(state, details, dryrun, jobs):
    db = load_db()
    for job in jobs:
        print(job)
        j = db.get_job_by_summary(job)
        if details:
            print(j)
            print('')
        print("Previous state of {} : {}".format(job, j["state"]))
        if dryrun is False:
            db.modify_state_of(job, state)
            print("{} updated".format(job))
            print("Previous state of {} : {}".format(job, state))


@click.command()
@click.option('--dryrun', help='dry run', required=True, type=bool)
@click.argument('jobs', nargs=-1, required=True)
def delete(dryrun, jobs):
    db = load_db()
    for job in jobs:
        print(job)
        if dryrun is False:
            db.delete({'summary': job})

@click.command()
def ipython():
    from IPython import embed
    db = load_db() #NOQA
    embed()

def load_db(folder=None):
    if folder is None:
        folder = get_dotfolder()
    params = get_db_params(folder=folder)
    db = DB(**params)
    db.load(folder)
    return db

def get_db_params(folder=None):
    if folder is None:
        folder = get_dotfolder()
    rcfilename = os.path.join(folder, '.lightjobrc')
    if os.path.exists(rcfilename):
        params = json.load(open(rcfilename))
    else:
        params = {}
    return params
 

def get_dotfolder():
    folder = backward_search(os.getcwd(), DOTDIR)
    if folder is None:
        folder = os.path.join(os.getcwd(), DOTDIR)
    return folder

main.add_command(show)
main.add_command(init)
main.add_command(ipython)
main.add_command(update)
main.add_command(delete)
main.add_command(dump)
