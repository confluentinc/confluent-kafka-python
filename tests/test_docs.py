#!/usr/bin/env python

import re
import sys
from collections import defaultdict
from types import ModuleType

import confluent_kafka


def build_doctree(tree, prefix, parent):
    """ Build doctree dict with format:
          dict key = full class/type name (e.g, "confluent_kafka.Message.timestamp")
          value = object
    """
    for n in dir(parent):
        if n.startswith('__') or n == 'cimpl':
            # Skip internals and the C module (it is automatically imported
            # to other names in __init__.py)
            continue

        o = parent.__dict__.get(n)
        if o is None:
            # Skip inherited (not overloaded)
            continue

        if isinstance(o, ModuleType):
            # Skip imported modules
            continue

        full = prefix + n
        tree[full].append(o)

        if hasattr(o, '__dict__'):
            is_module = isinstance(o, ModuleType)
            is_ck_package = o.__dict__.get('__module__', '').startswith('confluent_kafka.')
            is_cimpl_package = o.__dict__.get('__module__', '').startswith('cimpl.')
            if not is_module or is_ck_package or is_cimpl_package:
                build_doctree(tree, full + '.', o)


def test_verify_docs():
    """ Make sure all exported functions, classes, etc, have proper docstrings
    """

    tree = defaultdict(list)
    build_doctree(tree, 'confluent_kafka.', confluent_kafka)

    fails = 0
    expect_refs = defaultdict(list)
    all_docs = ''

    int_types = [int]
    if sys.version_info < (3, 0):
        int_types.append(long)  # noqa - long not defined in python2

    for n, vs in tree.items():
        level = 'ERROR'
        err = None

        if len(vs) > 1:
            err = 'Multiple definitions of %s: %s' % (n, vs)
        else:
            o = vs[0]
            doc = o.__doc__
            shortname = n.split('.')[-1]
            if n.find('KafkaException') != -1:
                # Ignore doc-less BaseException inheritance
                err = None
            elif doc is None:
                err = 'Missing __doc__ for: %s (type %s)' % (n, type(o))
            elif not re.search(r':', doc):
                err = 'Missing Doxygen tag for: %s (type %s):\n---\n%s\n---' % (n, type(o), doc)
                if n == 'confluent_kafka.cimpl':
                    # Ignore missing doc strings for the cimpl module itself.
                    level = 'IGNORE'
                elif type(o) in int_types:
                    # Integer constants can't have a doc strings so we check later
                    # that they are referenced somehow in the overall docs.
                    expect_refs[shortname].append(err)
                    err = None
                else:
                    pass
            else:
                all_docs += doc

        if err is not None:
            print('%s: %s' % (level, err))
            if level == 'ERROR':
                fails += 1

    # Make sure constants without docstrings (they can have any)
    # are referenced in other docstrings somewhere.
    for n in expect_refs:
        if all_docs.find(n) == -1:
            print('ERROR: %s not referenced in documentation (%s)' % (n, expect_refs[n]))
            fails += 1

    assert fails == 0
