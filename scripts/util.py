import re

def deep_get(dictionary, key, default=None):
    keys = key.split('.')
    value = reduce(lambda d, key: d.get(key, None) if isinstance(d, dict) else None, keys, dictionary)
    if isinstance(value, basestring):
        value = value.strip()
    return value or default

def deep_set(dictionary, key, value):
    keys = key.split('.')
    last = keys.pop()
    d = dictionary
    for k in keys:
        d = d.setdefault(k, {})
    d[last] = value

def deep_try_get(dictionary, *keys):
    for key in keys:
        value = deep_get(dictionary, key)
        if value:
            return value
    return None

def clean_text(value):
    return re.sub('[\s\r\n\t]+', ' ', value.strip())

def clean_list(values):
    return [clean_text(x) for x in values if x.strip()]
