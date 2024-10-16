# Developer Notes

This document provides information useful to developers working on confluent-kafka-python.


## Build

    $ python -m build

If librdkafka is installed in a non-standard location provide the include and library directories with:

    $ C_INCLUDE_PATH=/path/to/include LIBRARY_PATH=/path/to/lib python -m build

**Note**: On Windows the variables for Visual Studio are named INCLUDE and LIB 

## Generate Documentation

Install docs dependencies:

    $ pip install .[docs]

Build HTML docs:

    $ make docs

Documentation will be generated in `docs/_build/`.


## Tests


See [tests/README.md](tests/README.md) for instructions on how to run tests.

