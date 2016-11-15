#!/usr/bin/env python

import confluent_kafka
import re
from types import ModuleType

def test_verify_docs():
    """ Make sure all exported functions, classes, etc, have proper docstrings
    """
    fails = 0

    for n in dir(confluent_kafka):
        if n.startswith('__'):
            # Skip internals
            continue

        err = None
        level = 'ERROR'
        o = confluent_kafka.__dict__.get(n)
        d = o.__doc__
        if not d:
            err = 'Missing __doc__ for: %s (type %s)' % (n, type(o))
            fails += 1
        elif not re.search(r':', d):
            err = 'Missing Doxygen tag for: %s (type %s)' % (n, type(o))
            # Ignore missing doc strings for the cimpl module itself and
            # integer constants (which can't have a doc string)
            if n == 'cimpl' or type(o) in [int]:
                level = 'IGNORE'

        if err is not None:
            print('%s: %s' % (level, err))
            if level == 'ERROR':
                fails += 1

    assert fails == 0
