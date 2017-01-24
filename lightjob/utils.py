import os
import json
import hashlib
import collections
import six


def mkdir_path(path):
    """
    silent mkdir of a path if the path does not exist.
    it creates allow the needed directories that goes
    into the path.
    """
    if not os.access(path, os.F_OK):
        os.makedirs(path)


def backward_search(path, filename):
    """
    start from path and search for filename.
    - start from path
    - if the current `path` contains filename, returns the
      location of the filename prefixed by path
     - if the current `path` does not contain `filename`, go
       to the parent of the current `path`

    Parameters
    ----------

    path : str
        folder to start from
    filename : str
        filename to search for

    Returns
    -------

    the location of the filename prefixed by `path` if found
    Otherwise, it returns None if it is not found.
    """
    if path == "/":
        return None
    if os.path.exists(os.path.join(path, filename)):
        return os.path.join(path, filename)
    else:
        return backward_search(get_parent_directory(path), filename)


def get_parent_directory(path):
    return os.path.dirname(os.path.normpath(path))


def summarize(d):
    """
    hash a dict making sure the ordering of the content of the dict
    does not affect the hash. it is implemented by ordering the dict keys.
    """
    s = json.dumps(d, sort_keys=True)
    m = hashlib.md5()
    m.update(six.b(s))
    return m.hexdigest()

# http://stackoverflow.com/a/3233356


def recur_update(d, u):
    """
    update a dictionary `d` with another dictionary `u`
    recursively

    Parameters
    ----------

    d : dict
        dictionary to modify
    u : dict
        dictionary to use for update

    Returns
    -------

    dict, the modified `d`
    """
    for k, v in u.items():
        if isinstance(v, collections.Mapping):
            r = recur_update(d.get(k, {}), v)
            d[k] = r
        else:
            d[k] = u[k]
    return d

AGG = {'min': min, 'max': max, 'last': lambda l: l[-1],
       'first': lambda l: l[0], 'sum': lambda l: sum(l)}


def dict_format(d, field, agg=AGG, if_not_found='raise_exception', **kw):
    """
    get a field from a dictionary.

    Parameters
    ----------

    d : dict
        a deep dict
    field : str
        the path of the field to get the value from.
        syntax example of field : 'key1.key2.key3'.
        this would correspond to d['key1']['key2']['key3'].
        the field value could also be aggregated if it is not a scalar.
        syntax example for aggregation 'key1.key2.key3:sum':
        this would correspond to sum(d['key1']['key2']['key3']).
    agg : dict, optional
        aggregation functions to use, default is AGG, which contains:
        min, max, last, first, sum
    if_not_found: str, optional
        if 'raise_exception', then raises exception when the field is not found
        otherwise use the value of if_not_found, e.g if_not_found can be `np.nan`.
    """
    val = d
    field_comps = field.split('.')
    found = True
    for comp in field_comps:
        if ':' in comp:
            comp, agg_name = comp.split(':', 2)
            agg = agg_name[agg_name]
        else:
            agg = lambda x: x
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
    """
    return True if d and d_ref are matching for keys that exist in both
    Parameters
    ----------

    d : dict
        source dict
    d_ref : dict
        reference dict that we want to see if it matches with d
    """
    d = flatten_dict(d)
    d_ref = flatten_dict(d_ref)
    for k, v in d.items():
        if k in d_ref and d_ref[k] != v:
            return False
    return True


def flatten_dict(l):
    """
    flattens a dictionary

    Parameters
    ----------

    l : dict
        a deep dictionary

    Returns
    -------

    a flat dictionary version of l
    """
    d = {}
    for k, v in l.items():
        if isinstance(v, collections.Mapping):
            d.update(flatten_dict(v))
        else:
            d[k] = v
    return d
