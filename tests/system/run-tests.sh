#!/usr/bin/env bash

if [[ -z $WORKSPACE ]]; then
  echo "This should be run by Jenkins only"
  exit 1
fi

set -e

# Checkout supporting scripts
# Used by add_suffix_to_latest_results function above and jenkins/upload-tests.sh
rm -rf jenkins-common
git clone git@github.com:confluentinc/jenkins-common.git

cp $MUCKRAKE_PEM muckrake.pem

. jenkins-common/resources/scripts/extract-iam-credential.sh

set -x

# Immediately flush output when running python
export PYTHONUNBUFFERED=1

TEST_PATH=tests/kafkatest/tests/client

LIBRDKAFKA_BRANCH=master
KAFKA_BRANCH=2.6.0  # Tag
REPO=https://github.com/apache/kafka.git

CACHE=$WORKSPACE/cache  # Helps with reusing vagrant cluster
RESULTS=$WORKSPACE/results
KAFKA_DIR=$WORKSPACE/kafka

# Bringing up a Vagrant cluster is slow, so we may want to reuse a preexisting cluster
# These flags provide some control over caching behavior
DESTROY_BEFORE=true    # Destroy cluster (if applicable) *before* test run?

# VAGRANT_CLEANUP specifies what action to take after running the tests
NO_ACTION="no_action"
DESTROY="destroy"
SHUTDOWN="shutdown"
VAGRANT_CLEANUP=$DESTROY

# Build python client wheels and deploy on vagrant workers
# Note: a virtualenv must be active.
function build_python_client {
    local this_host=`curl http://169.254.169.254/latest/meta-data/local-ipv4`
    export DOCKER_HOST="tcp://$this_host:2375"

    tools/build-linux-selfcontained.sh $LIBRDKAFKA_BRANCH wheels

    # Deploy wheels on workers
    confluent_kafka/kafkatest/deploy.sh --prepare $KAFKA_DIR wheels

    # Synchronize workers
    pushd $KAFKA_DIR
    vagrant rsync
    popd # $KAFKA_DIR
}


function is_ducktape_session_id {
    local string="$1"

    if [[ -z "$(echo "$string" | egrep "^[0-9]{4}-[0-9]{2}-[0-9]{2}--[0-9]{3}$")" ]]; then
        echo "false"
    else
        echo "true"
    fi
}

# add a suffix which contains additional information such as
# github user, branch, commit id
function add_suffix_to_latest_results {
    if [[ -d "$RESULTS" ]]; then
        cd $RESULTS
    else
        return
    fi

    # easier to reason about state if we get rid of symlink
    rm -f latest || true

    # most recently modified
    latest_name="$(basename "$(ls -tr | tail -1)")"

    # We only want to rename latest_name if it is an unadulterated ducktape session id
    if [[ "$(is_ducktape_session_id "$latest_name")" == "false"  ]]; then
        return
    fi

    suffix="$($WORKSPACE/jenkins-common/scripts/system-tests/kafka-system-test/make-repo-identifier.sh --directory $KAFKA_DIR)"
    archive_name="${latest_name}.${suffix}"

    mv "$latest_name" "$archive_name"

    echo "$BUILD_URL" > "$archive_name/jenkins.txt"
}

trap cleanup EXIT
function cleanup() {
    add_suffix_to_latest_results
}

# Return false if at least one node is not running, else return true
function vagrant_alive() {
    vagrant status | egrep "(poweroff)|(not)|(stopped)" > /dev/null
    result=$?
    if [ "x$result" != "x0" ]; then
        echo true
    else
        echo false
    fi
}


# Clear results from the last run
# Do this before a test run rather than after so that jenkins can archive test output
rm -rf $RESULTS

# Get kafka and build
if [ ! -d $KAFKA_DIR ]; then
    echo "Downloading kafka..."
    git clone $REPO $KAFKA_DIR
fi
echo "Checking out $KAFKA_BRANCH ..."
cd $KAFKA_DIR
git pull
git checkout $KAFKA_BRANCH
./gradlew clean assemble systemTestLibs

# Cached vagrant data
if [ -d $CACHE ]; then
    cd $CACHE

    # Mark cluster for destruction if provisioning script has changed
    # TODO - vagrant doesn't seem to deal well with any changes, so
    # we might want to be more aggressive with overriding DESTROY_BEFORE
    if [ -f vagrant/base.sh ]; then
        if [ ! -z `diff vagrant/base.sh $KAFKA_DIR/vagrant/base.sh` ]; then
            echo "Vagrant provisioning has changed, so vagrant cluster will not be reused"
            DESTROY_BEFORE=true
        fi
    fi

    # Cached VM data
    if [ -d .vagrant ]; then
        if [ "x$DESTROY_BEFORE" != "xtrue" ]; then
            echo "Pulling in cached Vagrant data from previous test run..."
            cp -r .vagrant/ $KAFKA_DIR/.vagrant/
        fi
    fi
fi

echo "Grabbing Vagrantfile.local"
cp $WORKSPACE/jenkins-common/scripts/system-tests/kafka-system-test/Vagrantfile.local $KAFKA_DIR

# The client system tests only need about 12 workers, rather than
# the default 30 (or so). This speeds up test start up times.
sed -i=bak 's/^num_workers.*/num_workers = 12/g' $KAFKA_DIR/Vagrantfile.local

if [ "x$DESTROY_BEFORE" == "xtrue" ]; then
    echo "Destroying Vagrant cluster before running tests..."
    cd $KAFKA_DIR
    vagrant destroy -f || true
fi

# Bring up cluster if necessary
alive=`vagrant_alive`
if [ "x$alive" == "xtrue" ]; then
    echo "Vagrant cluster is already running"
    echo "Syncing contents of kafka directory to virtual machines..."
    vagrant rsync
else
    echo "Bringing up cluster..."
    if [[ -e vagrant/vagrant-up.sh ]]; then
        vagrant/vagrant-up.sh --aws
    else
        vagrant up --provider=aws --no-parallel --no-provision
        echo "Provisioning cluster..."
        vagrant provision
    fi
fi

# Set up python dependencies
cd $KAFKA_DIR
virtualenv venv
. venv/bin/activate
cd tests
python setup.py develop

# Build Python client
cd $WORKSPACE
build_python_client

# Downgrade bcrypt since 3.2.0 no longer works with Python 2.
# Remove this when ducktape runs on Python 3.
pip install bcrypt==3.1.7

# Run the tests
cd $KAFKA_DIR
python `which ducktape` --debug $TEST_PATH \
    --globals tests/confluent-kafka-python/globals.json \
    --results-root $RESULTS \
    --compress
