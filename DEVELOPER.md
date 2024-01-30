# Developer Notes

This document provides information useful to developers working on confluent-kafka-python.


## Build

    $ python setup.py build

If librdkafka is installed in a non-standard location provide the include and library directories with:

    $ C_INCLUDE_PATH=/path/to/include LIBRARY_PATH=/path/to/lib python setup.py ...

**Note**: On Windows the variables for Visual Studio are named INCLUDE and LIB 

### aarch64 Build

Confluent's Python Client for Apache Kafka is a wrapper around `librdkafka`. `libradkafka` does not come with prebuilt binary wheels for aarch64, so you will need to compile it yourself, which requires you to first build and install `librdkafka` from source. `libradkafka` build steps for Debian based systems and Alpine are listed below:

**Debian based:**

```
    $ sudo apt install -y libssl-dev zlib1g-dev gcc g++ make 
    $ git clone https://github.com/edenhill/librdkafka 
    $ cd librdkafa 
    $ ./configure --prefix=/usr 
    $ make 
    $ sudo make install
```

**Alpine:**
`libradkafka` build steps are the same for Alpine, but some of the required packages are not available for Alpine. Replace the first step of Debian build with below command to satisfy the requirements:

    $ sudo apk add libssl-dev zlib1g-dev gcc g++ make

The rest would be the same as above.

## Generate Documentation

Install sphinx and sphinx_rtd_theme packages:

    $ pip install sphinx sphinx_rtd_theme

Build HTML docs:

    $ make docs

Documentation will be generated in `docs/_build/`.

or:

    $ python setup.py build_sphinx

Documentation will be generated in  `build/sphinx/html`.


## Tests


See [tests/README.md](tests/README.md) for instructions on how to run tests.

