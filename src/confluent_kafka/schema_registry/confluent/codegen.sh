#!/usr/bin/env bash
# Regenerates the Python bindings for the vendored confluent value types.
# Run from the repo root. Requires protoc 35.1 to match the checked-in headers.
#
# Two post-processing steps, both load-bearing:
#
#  1. The runtime-version guard is stripped. protoc emits an import of
#     google.protobuf.runtime_version plus a ValidateProtobufRuntimeVersion call, but
#     requirements-protobuf.txt pins no protobuf version, and that module does not exist on
#     older runtimes - so the guard would make these modules fail to import at all. Every
#     checked-in _pb2.py here has it stripped for that reason.
#  2. Relative imports are rewritten to fully-qualified ones. protoc emits
#     `from confluent.type import decimal_pb2`, which only resolves if `confluent` is a
#     top-level package; here it lives under confluent_kafka.schema_registry.
#
# confluent/types/decimal.proto is a public-import stub for the path confluent.type.Decimal
# used to occupy. Its generated module re-exports Decimal and registers the old file name, so
# code and descriptors built against the old path keep working.
set -euo pipefail

PKG=src/confluent_kafka/schema_registry
ABS=confluent_kafka.schema_registry

# confluent/meta.proto is deliberately not regenerated here: its checked-in module came from a
# much older protoc and this one rewrites the whole file, which is churn unrelated to the value
# types. Regenerate it on purpose, not as a side effect of touching decimal or variant.
cd "$PKG"
protoc -I. --python_out=. confluent/type/decimal.proto confluent/type/variant.proto \
  confluent/types/decimal.proto

for f in confluent/type/decimal_pb2.py confluent/type/variant_pb2.py \
         confluent/types/decimal_pb2.py; do
  python3 - "$f" "$ABS" <<'PY'
import re, sys
path, abs_pkg = sys.argv[1], sys.argv[2]
s = open(path).read()
s = s.replace('from google.protobuf import runtime_version as _runtime_version\n', '')
s = re.sub(r'_runtime_version\.ValidateProtobufRuntimeVersion\((?:[^)]*)\)\n', '', s)
# `from confluent.type import decimal_pb2 as X` -> `import <abs>.confluent.type.decimal_pb2 as X`
s = re.sub(r'^from (confluent[\w.]*) import (\w+) as (\w+)$',
           lambda m: 'import %s.%s.%s as %s' % (abs_pkg, m.group(1), m.group(2), m.group(3)),
           s, flags=re.M)
# `from confluent.type.decimal_pb2 import *` (public import re-export) -> fully qualified
s = re.sub(r'^from (confluent[\w.]*) import \*$',
           lambda m: 'from %s.%s import *' % (abs_pkg, m.group(1)), s, flags=re.M)
open(path, 'w').write(s)
PY
done
