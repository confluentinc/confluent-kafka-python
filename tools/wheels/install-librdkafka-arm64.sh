set -ex

VER="$1"
if [[ -f  /usr/local/include/librdkafka/rdkafka.h ]]; then
    echo "$0: librdkafka already installed in /usr/local/include/librdkafka"
    exit 0
fi

echo "$0: Installing librdkafka $VER to $PWD"
curl -L https://github.com/edenhill/librdkafka/archive/v$VER.tar.gz | tar xzf -
cd librdkafka-$VER
chmod 777 configure lds-gen.py
./configure
make
make install
