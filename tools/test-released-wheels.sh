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
        echo "DEBUG: UV_INDEX=${UV_INDEX:-<unset>}"
        echo "DEBUG: UV_INDEX_URL=${UV_INDEX_URL:-<unset>}"
        echo "DEBUG: UV_EXTRA_INDEX_URL=${UV_EXTRA_INDEX_URL:-<unset>}"
        echo "DEBUG: PIP_INDEX_URL=${PIP_INDEX_URL:-<unset>}"
        echo "DEBUG: PIP_EXTRA_INDEX_URL=${PIP_EXTRA_INDEX_URL:-<unset>}"
        echo "DEBUG: PIP_CONFIG_FILE=${PIP_CONFIG_FILE:-<unset>}"
        pip config list 2>/dev/null || echo "DEBUG: pip config not available"
        uv pip config list 2>/dev/null || echo "DEBUG: uv pip config not available"
        install_cmd="UV_INDEX= UV_INDEX_URL= UV_EXTRA_INDEX_URL= PIP_INDEX_URL= PIP_EXTRA_INDEX_URL= PIP_CONFIG_FILE=/dev/null uv pip install --no-config --no-deps --prerelease allow -v --index-url https://test.pypi.org/simple/ confluent_kafka==$1"
    else
        echo "Installing confluent_kafka"
        install_cmd="uv pip install confluent_kafka==$1"
    fi

    max_retries=20
    retry_interval=30
    for attempt in $(seq 1 $max_retries); do
        if eval $install_cmd; then
            break
        fi
        if [ $attempt -eq $max_retries ]; then
            echo "Failed to install confluent_kafka==$1 after $max_retries attempts"
            exit 1
        fi
        echo "Attempt $attempt/$max_retries failed, retrying in ${retry_interval}s..."
        sleep $retry_interval
    done

    echo "Testing confluent_kafka"
    output=$(python -c 'import confluent_kafka as ck ; print("py:", ck.version(), "c:", ck.libversion())')
    echo -e "\e[1;32m$output\e[0m"
    echo "Successfully tested confluent_kafka version $1 with Python version $version"

    deactivate
    rm -rf "$venvdir"
    echo
    echo
done