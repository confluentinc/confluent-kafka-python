#!/bin/bash

set -e

# Check if an argument is present
if [ -z "$1" ]; then
    echo "Please provide a version of confluent_kafka to install."
    exit 1
fi

# List of Python versions to run the code with
python_versions=("3.8" "3.9" "3.10" "3.11" "3.12" "3.13" "3.14")

if command -v sem-version &> /dev/null; then
    use_sem_version=true
else
    use_sem_version=false
    if ! command -v pyenv &> /dev/null; then
        echo "Neither sem-version nor pyenv is available."
        exit 1
    fi
    eval "$(pyenv init -)"
fi


# Loop through each Python version and execute the code
for version in "${python_versions[@]}"; do

    echo -e "\e[1;34mRunning with Python version $version \e[0m"

    if [ "$use_sem_version" = true ]; then
        sem-version python $version
    else
        echo Installing python $version
        pyenv install -s $version > /dev/null 2>&1
        echo Using $version
        pyenv shell $version > /dev/null 2>&1
    fi

    echo Python version: `python -V`

    pip install uv

    venvdir=$(mktemp -d /tmp/_venvXXXXXX)
    uv venv -p python$version $venvdir
    source $venvdir/bin/activate

    echo "Uninstalling confluent_kafka if already installed"
    uv pip uninstall confluent_kafka > /dev/null 2>&1 || true

    if [ "$2" = "test" ]; then
        echo "Installing confluent_kafka from test PyPI"
        uv pip install --index-url https://test.pypi.org/simple/ confluent_kafka==$1
    else
        echo "Installing confluent_kafka"
        uv pip install confluent_kafka==$1
    fi

    echo "Testing confluent_kafka"
    output=$(python -c 'import confluent_kafka as ck ; print("py:", ck.version(), "c:", ck.libversion())')
    echo -e "\e[1;32m$output\e[0m"
    echo "Successfully tested confluent_kafka version $1 with Python version $version"

    deactivate
    rm -rf "$venvdir"
    echo
    echo
done