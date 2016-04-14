import sys
import os
import click
from utils import mkdir_path, backward_search
from db import DB
import logging

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
    folder = get_dotfolder()
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
@click.option('--details', default=False, help='show with details', required=False)
def show(state, type, details):
    if details:
        import pprint
        def show(j):
            pprint.pprint(j, indent=4)
    else:
        def show(j):
            logger.info(j['summary'])
    db = load_db()
    kw = {}
    if state is not None:
        kw["state"] = state
    if type is not None:
        kw["type"] = type
    jobs = db.jobs_with(**kw)
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
def ipython():
    from IPython import embed
    from tinydb import Query
    db = load_db()
    embed()


def load_db():
    folder = get_dotfolder()
    db = DB()
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
