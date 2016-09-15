import sys
import os
import click
from utils import mkdir_path, backward_search
from db import DB
import logging
import json

from dateutil import parser
import math

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
@click.option('--state', default=None, help='state', required=False)
@click.option('--type', default=None, help='type', required=False)
@click.option('--where', default=None, help='where', required=False)
@click.option('--details/--no-details', default=False, help='show with details', required=False)
@click.option('--fields', default='', help='show values of fields separated by comma', required=False)
@click.option('--summary', default='', help='show a specific job', required=False)
@click.option('--sort', default='', help='sort by some field or time', required=False)
@click.option('--export/--no-export', default=False, help='export to json', required=False)
def show(state, type, where, details, fields, summary, sort, export):
    import pandas as pd
    if details:
        import pprint
        def show(j):
            if fields:
                vals = []
                for field in fields.split(','):
                    try:
                        val = db.get_value(j, field)
                    except ValueError:
                        val = None
                    vals.append(val)
                print(' '.join(map(str, vals)))
            else:
                pprint.pprint(j, indent=4)
    else:
        def show(j):
            logger.info(j['summary'])
    db = load_db()
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
        try:
            j['duration'] = j['end_time'] - j['start_time']
        except Exception:
            j['duration'] = 'none'
    if sort:
        def key(j):
            val = db.get_value(j, sort)
            if val and isinstance(val, float) and math.isnan(val):
                return float('inf')
            return val
        jobs = sorted(jobs, key=key)
    if details:
        logger.info("Number of jobs : {}".format(len(jobs)))

    for j in jobs:
        show(j)
    if export:
        for j in jobs:
            fd = open(j['summary'] + '.json', 'w')
            fd.write(json.dumps(j['content'], indent=4))
            fd.close()


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
    rcfilename = os.path.join(folder, '.lightjobrc')
    if os.path.exists(rcfilename):
        params = json.load(open(rcfilename))
    else:
        params = {}
    db = DB(**params)
    db.load(folder)
    return db


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
