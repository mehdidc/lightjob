import os
import json
import hashlib
import collections

def mkdir_path(path):
    if not os.access(path, os.F_OK):
        os.makedirs(path)

def backward_search(path, filename):
    if path == "/":
        return None
    if os.path.exists(os.path.join(path, filename)):
        return os.path.join(path, filename)
    else:
        return backward_search(get_parent_directory(path), filename)

def get_parent_directory(path):
    return os.path.dirname(os.path.normpath(path))

def summarize(d):
    s = json.dumps(d, sort_keys=True)
    m = hashlib.md5()
    m.update(s)
    return m.hexdigest()

#http://stackoverflow.com/a/3233356
def recur_update(d, u):
    for k, v in u.iteritems():
        if isinstance(v, collections.Mapping):
            r = recur_update(d.get(k, {}), v)
            d[k] = r
        else:
            d[k] = u[k]
    return d
