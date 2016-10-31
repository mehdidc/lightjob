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

AGG = {'min': min, 'max': max, 'last': lambda l:l[-1], 'first':lambda l:l[0], 'sum':lambda l:sum(l)}
def dict_format(dict, field, agg=AGG, if_not_found='raise_exception', **kw):
    val = dict
    field_comps = field.split('.')
    found = True
    for comp in field_comps:
        if ':' in comp:
            comp, agg_name = comp.split(':', 2)
            agg = agg_name[agg_name]
        else:
            agg = lambda x:x
        if not val or (val and comp not in val):
            found = False
            break
        else:
            val = val[comp]
    if found:
        return agg(val)
    else:
        if if_not_found == 'raise_exception':
            raise ValueError('field {} does not exist'.format(field))
        else:
            return if_not_found

def match(d, d_ref):
    d = flatten_dict(d)
    d_ref = flatten_dict(d_ref)
    for k, v in d.items():
        if k in d_ref and d_ref[k] != v:
            return False
    return True

def flatten_dict(l):
    d = {}
    for k, v in l.items():
        if isinstance(v, collections.Mapping):
            d.update(flatten_dict(v))
        else:
            d[k] = v
    return d
