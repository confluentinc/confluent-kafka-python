#!/bin/bash
#
# Verify an installed free-threaded (cp3XXt) confluent-kafka wheel.
#
# Usage: tools/verify-free-threaded-wheel.sh [python]
#   python   interpreter to check with; defaults to "python", i.e. the activated 3.14t venv.
#
# Expects the wheel to be installed already and LIBRDKAFKA_VERSION (e.g. v2.15.0)
# to name the librdkafka it must bundle. Exits non-zero on the first failed check.

set -eu

PYTHON=${1:-python}
: "${LIBRDKAFKA_VERSION:?set LIBRDKAFKA_VERSION (e.g. v2.15.0) to the librdkafka the wheel must bundle}"

echo "# Interpreter must be a free-threaded build, otherwise the GIL check below is meaningless"
"$PYTHON" -c "import sys, sysconfig; assert sysconfig.get_config_var('Py_GIL_DISABLED'), sys.executable + ' is not a free-threaded build'"

echo "# Importing must not re-enable the GIL"
"$PYTHON" -W error::RuntimeWarning -c "import sys, confluent_kafka; assert not sys._is_gil_enabled(), 'import re-enabled the GIL'"

echo "# Loaded extension matches this interpreter's ABI (EXT_SUFFIX, since Windows names it .cp314t-win_amd64.pyd) and bundles the expected librdkafka"
"$PYTHON" -c "import os, sysconfig, confluent_kafka as ck; print(ck.version(), ck.libversion(), ck.cimpl.__file__); assert ck.cimpl.__file__.endswith(sysconfig.get_config_var('EXT_SUFFIX')), ck.cimpl.__file__; assert ck.libversion()[0] == os.environ['LIBRDKAFKA_VERSION'].lstrip('v'), ck.libversion()"

echo "# Bundled librdkafka was built with OpenSSL and the gzip, lz4, snappy and zstd codecs"
"$PYTHON" -c "import confluent_kafka; confluent_kafka.Producer({'ssl.cipher.suites':'DEFAULT'}); print('OK: OpenSSL')"
"$PYTHON" -c "import confluent_kafka; confluent_kafka.Producer({'compression.codec':'gzip'}); print('OK: gzip')"
"$PYTHON" -c "import confluent_kafka; confluent_kafka.Producer({'compression.codec':'lz4'}); print('OK: lz4')"
"$PYTHON" -c "import confluent_kafka; confluent_kafka.Producer({'compression.codec':'snappy'}); print('OK: snappy')"
"$PYTHON" -c "import confluent_kafka; confluent_kafka.Producer({'compression.codec':'zstd'}); print('OK: zstd')"

echo "# All free-threaded wheel verification checks passed"
