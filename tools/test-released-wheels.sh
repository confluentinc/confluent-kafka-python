#!/bin/bash

set -e

# Check if an argument is present
if [ -z "$1" ]; then
    echo "Please provide a version of confluent_kafka to install."
    exit 1
fi

# Check if pyenv is installed
if ! command -v pyenv &> /dev/null; then
    echo "pyenv is not installed. Please install pyenv before running this script."
    exit 1
fi

# List of Python versions to run the code with
python_versions=("3.6.15" "3.7.17" "3.8.18" "3.9.18" "3.10.11" "3.11.7" "3.12.1" "3.13.0")

eval "$(pyenv init -)"


# Loop through each Python version and execute the code
for version in "${python_versions[@]}"; do
 
    echo -e "\e[1;34mRunning with Python version $version in env $environment_name \e[0m"
    
    echo Installing python $version
    pyenv install -s $version > /dev/null 2>&1

    echo Using $version
    pyenv shell $version > /dev/null 2>&1

    echo Python version: `python -V`
    echo Pip version: `pip -V`

    pip install --upgrade pip

    echo "Uninstalling confluent_kafka if already installed"
    pip uninstall -y confluent_kafka > /dev/null 2>&1 || true

    if [ "$2" = "test" ]; then
        echo "Installing confluent_kafka from test PyPI"
        pip install --index-url https://test.pypi.org/simple/ confluent_kafka==$1 > /dev/null 2>&1
    else
        echo "Installing confluent_kafka"
        pip install confluent_kafka==$1
    fi

    echo "Testing confluent_kafka"    
    output=$(python -c 'import confluent_kafka as ck ; print("py:", ck.version(), "c:", ck.libversion())')
    echo -e "\e[1;32m$output\e[0m"
    echo "Successfully tested confluent_kafka version $1 with Python version $version"
    echo
    echo
done
