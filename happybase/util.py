"""
HappyBase utility module.

These functions are not part of the public API.
"""

import re

CAPITALS = re.compile('([A-Z])')


try:
    # Python 2.7
    from collections import OrderedDict
except ImportError:
    try:
        # External package for Python 2.6
        from ordereddict import OrderedDict
    except ImportError as exc:
        # Stub to throw errors at run-time (not import time)
        def OrderedDict(*args, **kwargs):
            raise RuntimeError(
                "No OrderedDict implementation available; please "
                "install the 'ordereddict' Package from PyPI.")


def camel_case_to_pep8(name):
    """Convert a camel cased name to PEP8 style."""
    converted = CAPITALS.sub(lambda m: '_' + m.groups()[0].lower(), name)
    if converted[0] == '_':
        return converted[1:]
    else:
        return converted


def pep8_to_camel_case(name, initial=False):
    """Convert a PEP8 style name to camel case."""
    chunks = name.split('_')
    converted = [s[0].upper() + s[1:].lower() for s in chunks]
    if initial:
        return ''.join(converted)
    else:
        return chunks[0].lower() + ''.join(converted[1:])


def thrift_attrs(obj_or_cls):
    """Obtain Thrift data type attribute names for an instance or class."""
    return [v[2] for v in obj_or_cls.thrift_spec[1:]]


def thrift_type_to_dict(obj):
    """Convert a Thrift data type to a regular dictionary."""
    return dict((camel_case_to_pep8(attr), getattr(obj, attr))
                for attr in thrift_attrs(obj))


def str_increment(s):
    """Increment and truncate a byte string (for sorting purposes)

    This functions returns the shortest string that sorts after the given
    string when compared using regular string comparison semantics.

    This function increments the last byte that is smaller than ``0xFF``, and
    drops everything after it. If the string only contains ``0xFF`` bytes,
    `None` is returned.
    """
    for i in xrange(len(s) - 1, -1, -1):
        if s[i] != '\xff':
            return s[:i] + chr(ord(s[i]) + 1)

    return None
