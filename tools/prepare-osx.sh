#!/bin/bash
#
# This script prepares the Travis OSX env with a particular interpreter
# https://docs.travis-ci.com/user/languages/python/
#
# Default OSX environment
# https://docs.travis-ci.com/user/reference/osx/#compilers-and-build-toolchain
#
PY_INTERPRETER=$1
VENV_HOME=$2

set -ev

brew upgrade libtool || brew install libtool

if [[ -z ${PY_INTERPRETER} ]] || [[  -z ${VENV_HOME} ]]; then
    echo "Usage: $0 <Python interpreter version> <destination>"
    exit 1
fi

# Update virtualenv and install requested interpreter
echo "# Updating basic dependencies"
pip install -U pip
pip install virtualenv
pyenv install -f ${PY_INTERPRETER}

# Create virtualenv
echo "# Constructing virtualenv for interpreter ${PY_INTERPRETER}"
virtualenv -p ~/.pyenv/versions/${PY_INTERPRETER}/bin/python ${VENV_HOME}
