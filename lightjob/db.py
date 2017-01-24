import six
import importlib

DBFILENAME = 'db.json'
STATES = AVAILABLE, RUNNING, SUCCESS, ERROR, PENDING, DELETED = (
    'available', 'running', 'success', 'error', 'pending', 'deleted')
IDKEY = 'summary'
CONTENTKEY = 'content'
STATEKEY = 'state'
LIFEKEY = 'life'


def DB(backend='Blitz', **kw):
    if isinstance(backend, six.string_types):
        module = importlib.import_module('.databases', 'lightjob')
        backend = getattr(module, backend)
    return backend(**kw)
