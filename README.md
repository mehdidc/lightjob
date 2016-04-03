# Lightjob

a lightweight job manager using a server-less json database (tinydb)

## install

```
git clone https://github.com/mehdidc/lightjob
cd lightjob
python setup.py install
```


## Pipeline example

### init a repo

```
lightjob init
```

This creates a json database in .lightjob of your current folder.

### Show jobs

```
lightjob show --state=available
```

```
lightjob show --state=available --details=True
```

```
lightjob show --state=running --details=True
```

### More options

```
lightjob --help
```
### Access through python

#### Inserting jobs:

```python
from lightjob.cli import load_db
db = load_db()

job_content = {"nb_estimators": 100, "max_depth": 20}
db.add_job(job_content)
job_content = {"nb_estimators": 10, "max_depth": 3}
db.add_job(job_content)
```

#### Managing jobs

```python
from lightjob.cli import load_db
frm lightjob.db import AVAILABLE, RUNNING, ERROR

def run_job(job):
    # Code to run a job

db = load_db()

for job in db.jobs_with_state(AVAILABLE):
    db.modify_state_of(job["summary"], RUNNING)
    if run_job(job["content"]) == 0:
        db.modify_state_of(job["summary"], SUCCESS)
    else:
        db.modify_state_of(job["summary"], ERROR)
```
