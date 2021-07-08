set -ex

VER="$1"
#DEST="$2"

#if [[ -z $DEST ]]; then
#    echo "Usage: $0 <librdkafka-redist-version> <destdir>"
#    exit 1
#fi

if [[ -f  /usr/local/include/librdkafka/rdkafka.h ]]; then
    echo "$0: librdkafka already installed in /usr/local/include/librdkafka"
    exit 0
fi

echo "$0: Installing librdkafka $VER to $PWD"
#[[ -d "$DEST" ]] || mkdir -p "$DEST"
#pushd "$DEST"

#pwd
#rm -rf librdkafka
#git clone https://github.com/edenhill/librdkafka
curl -L https://github.com/edenhill/librdkafka/archive/v$VER.tar.gz | tar xzf -
cd librdkafka-$VER
chmod 777 configure lds-gen.py
./configure
make
make install

#popd
ls -lrt
#echo "$0: Installing librdkafka $VER to $DEST"
#[[ -d "$DEST" ]] || mkdir -p "$DEST"
#pushd "$DEST"
#curl -L https://github.com/edenhill/librdkafka/archive/v0.9.2-RC1.tar.gz | tar xzf -
#cd librdkafka-0.9.2-RC1/
#./configure --prefix=/usr
#make -j
#sudo make install
#popd
