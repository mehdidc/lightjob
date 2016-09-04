import sys
import os
import click
from utils import mkdir_path, backward_search
from db import DB
import logging
import json

from dateutil import parser


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
def init(force, purge):
    folder = './{}'.format(DOTDIR)
    if os.path.exists(folder):
        if not force:
            logger.error("{} Already exists, cancel init".format(folder))
            return
    mkdir_path(folder)
    logger.info("Init on : {}".format(folder))
    db = DB()
    db.load(folder)
    if purge:
        db.purge()


@click.command()
@click.option('--state', default=None, help='state', required=False)
@click.option('--type', default=None, help='type', required=False)
@click.option('--where', default=None, help='where', required=False)
@click.option('--details', default=False, help='show with details', required=False)
@click.option('--fields', default='', help='show values of fields separated by comma', required=False)
@click.option('--summary', default='', help='show a specific job', required=False)
@click.option('--sort', default='', help='sort by some field or time', required=False)
def show(state, type, where, details, fields, summary, sort):
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
    if sort:
        jobs = list(jobs)
        if sort == 'time':
            key = lambda j:parser.parse(j['life'][-1]['dt'])
        else:
            key = lambda j:db.get_value(j, sort)
        jobs = sorted(jobs, key=key)
    if details:
        logger.info("Number of jobs : {}".format(len(jobs)))

    for j in jobs:
        show(j)


@click.command()
@click.option('--state', help='state', required=True)
@click.option('--details', help='details', required=False, type=bool)
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
