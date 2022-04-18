# confluent-kafka-python installation instructions

## Install pre-built wheels (recommended)

Confluent provides pre-built Python wheels of confluent-kafka-python with
all dependencies included.

To install, simply do:

```bash
python3 -m pip install confluent-kafka
```

If you get a build error or require Kerberos/GSSAPI support please read the next section: *Install from source*


## Install from source

It is sometimes necessary to install confluent-kafka from source, rather
than from prebuilt binary wheels, such as when:
 - You need GSSAPI/Kerberos authentication.
 - You're on a Python version we do not provide prebuilt wheels for.
 - You're on an architecture or platform we do not provide prebuilt wheels for.
 - You want to build confluent-kafka-python from the master branch.


### Install from source on RedHat, CentOS, Fedora, etc

```bash
#
# Perform these steps as the root user (e.g., in a 'sudo bash' shell)
#

# Install build tools and Kerberos support.

yum install -y python3 python3-pip python3-devel gcc make cyrus-sasl-gssapi krb5-workstation

# Install the latest version of librdkafka:

rpm --import https://packages.confluent.io/rpm/7.0/archive.key

echo '
[Confluent-Clients]
name=Confluent Clients repository
baseurl=https://packages.confluent.io/clients/rpm/centos/$releasever/$basearch
gpgcheck=1
gpgkey=https://packages.confluent.io/clients/rpm/archive.key
enabled=1' > /etc/yum.repos.d/confluent.repo

yum install -y librdkafka-devel


#
# Now build and install confluent-kafka-python as your standard user
# (e.g., exit the root shell first).
#

python3 -m pip install --no-binary confluent-kafka confluent-kafka


# Verify that confluent_kafka is installed:

python3 -c 'import confluent_kafka; print(confluent_kafka.version())'
```

### Install from source on Debian or Ubuntu

```bash
#
# Perform these steps as the root user (e.g., in a 'sudo bash' shell)
#

# Install build tools and Kerberos support.

apt install -y wget software-properties-common lsb-release gcc make python3 python3-pip python3-dev libsasl2-modules-gssapi-mit krb5-user


# Install the latest version of librdkafka:

wget -qO - https://packages.confluent.io/deb/7.0/archive.key | apt-key add -

add-apt-repository "deb https://packages.confluent.io/clients/deb $(lsb_release -cs) main"

apt update

apt install -y librdkafka-dev


#
# Now build and install confluent-kafka-python as your standard user
# (e.g., exit the root shell first).
#

python3 -m pip install --no-binary confluent-kafka confluent-kafka


# Verify that confluent_kafka is installed:

python3 -c 'import confluent_kafka; print(confluent_kafka.version())'
```


### Install from source on Mac OS X

```bash

# Install librdkafka from homebrew

brew install librdkafka


# Build and install confluent-kafka-python

python3 -m pip install --no-binary confluent-kafka confluent-kafka


# Verify that confluent_kafka is installed:

python3 -c 'import confluent_kafka; print(confluent_kafka.version())'

```
