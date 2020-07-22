# Tools


## download-s3.py

To download CI build artifacts from S3, set up your AWS credentials
and run `tools/download-s3.py <tag|sha1>`, the artifacts will be downloaded
into `dl-<tag|sha1>`.

To upload binary packages to PyPi (test.pypi.org in this example to be safe), do:

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



## How to test wheels

After wheels have been downloaded with `download-s3.py`, perform smoketests
by running `tools/test-wheels.sh <download-dir>`, e.g.,
`tools/test-wheels.sh tools/dl-v1.5.0rc1`.
This script preferably be run on OSX (with Docker installed) so that
both OSX and Linux wheels are tested.
