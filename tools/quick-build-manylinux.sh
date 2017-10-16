#!/bin/bash
#

set -e

python setup.py clean -a

rm -f wheelhouse/* ../kafka/wheels/*

export CIBW_BEFORE_BUILD="tools/prepare-cibuildwheel-linux.sh master"

if [[ -z $CIBW_SKIP ]]; then
    export CIBW_SKIP='cp*-*i686 cp33-* cp34-* cp35-* cp36-*'
fi

cibuildwheel --output-dir wheelhouse --platform linux

cp wheelhouse/* ../kafka/wheels/
