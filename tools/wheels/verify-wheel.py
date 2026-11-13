"""Check the installed Linux wheel's native library, ABI, and required features."""

import sys
import sysconfig

import confluent_kafka

expected_version = sys.argv[1].lstrip('v')
assert confluent_kafka.libversion()[0] == expected_version, confluent_kafka.libversion()
assert confluent_kafka.cimpl.__file__.endswith(sysconfig.get_config_var('EXT_SUFFIX'))
if sysconfig.get_config_var('Py_GIL_DISABLED'):
    assert not sys._is_gil_enabled(), 'Importing the wheel re-enabled the GIL'

confluent_kafka.Producer({'ssl.cipher.suites': 'DEFAULT', 'log_level': 0})
for codec in ('gzip', 'lz4', 'snappy', 'zstd'):
    confluent_kafka.Producer({'compression.codec': codec, 'log_level': 0})

print(
    'Verified Python',
    sys.version,
    confluent_kafka.version(),
    confluent_kafka.libversion(),
    'OpenSSL and gzip/lz4/snappy/zstd',
)
