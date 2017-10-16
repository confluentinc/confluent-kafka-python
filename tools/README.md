# Tools


## download-s3.py

To download CI build artifacts from S3, set up your AWS credentials
and run `tools/download-s3.py <tag|sha1>`, the artifacts will be downloaded
into `dl-<tag|sha1>`.

To upload packages to PyPi (test.pypi.org in this example to be safe), do:

    $ twine upload -r test dl-<tag|sha1>/*


## manylinux build

To build the manylinux Python packages follow these steps:

**NOTE**: Docker is required.

Install cibuildwheel:

    $ pip install -r tools/requirements-manylinux.txt

Build using cibuildwheel:

    $ tools/cibuildwheel-build.sh wheelhouse

To skip Python platform configurations, use glob matching in CIBW_SKIP env.
This example only builds for Python 2.7 x64:

    $ CIBW_SKIP="cp3* cp*i686*" tools/cibuildwheel-build.sh wheelhouse

Packages will now be available in wheelhouse/
