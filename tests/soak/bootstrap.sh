#!/bin/bash
#
#

# Bootstrap EC2 instance (Ubuntu) for soak client use
#
# Usage:
#  $0 <python-branch/tag> <librdkafka-branch/tag>

set -e

if [[ $# != 2 ]]; then
    echo "Usage: $0 <librdkafka-branch/tag> <python-client-branch/tag>"
    exit 1
fi

librdkafka_branch=$1
python_branch=$2
otel_collector_version=0.130.0
otel_collector_package_url="https://github.com/open-telemetry/"\
"opentelemetry-collector-releases/releases/download/"\
"v${otel_collector_version}/otelcol-contrib_${otel_collector_version}_linux_amd64.deb"

validate_config() {
    local errors=0

    if grep -q "FILL_IN_REGION_HERE" otel-config.yaml; then
        echo "ERROR: AWS region not configured in otel-config.yaml"
        echo "Please set AWS_REGION in the file"
        errors=$((errors + 1))
    fi

    if grep -q "FILL_IN_ROLE_ARN_HERE" otel-config.yaml; then
        echo "ERROR: AWS Role ARN not configured in otel-config.yaml"
        echo "Please set AWS_ROLE_ARN in the file"
        errors=$((errors + 1))
    fi
    
    if grep -q "FILL_IN_REMOTE_WRITE_ENDPOINT_HERE" otel-config.yaml; then
        echo "ERROR: Prometheus endpoint not configured"
        echo "Please set PROMETHEUS_ENDPOINT in the file"
        errors=$((errors + 1))
    fi
    
    if [[ $errors -gt 0 ]]; then
        echo "Configuration validation failed. Please fix the above errors."
        exit 1
    fi
    
    echo "Configuration validation passed."
}
validate_config

sudo apt update
sudo apt install -y git curl wget make gcc g++ zlib1g-dev libssl-dev \
    libzstd-dev python3-dev python3-pip python3-venv
wget -O otel_collector_package.deb $otel_collector_package_url
sudo dpkg -i otel_collector_package.deb
rm otel_collector_package.deb
sudo cp otel-config.yaml /etc/otelcol-contrib/config.yaml
sudo systemctl restart otelcol-contrib
cp setup_all_versions.py $HOME/setup_all_versions.py
chmod +x $HOME/setup_all_versions.py

./build.sh $librdkafka_branch $python_branch

venv=$PWD/venv
echo "All done, activate the virtualenv in $venv before running the client:"
echo "source $venv/bin/activate"

