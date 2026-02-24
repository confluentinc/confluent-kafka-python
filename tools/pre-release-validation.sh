#!/bin/bash
#
# Pre-release validation script.
#
# Checks that version strings are consistent across all source files
# that must be updated for a release (see RELEASE.md steps 3-4).
#
# Files checked:
#   - pyproject.toml                              (version)
#   - src/confluent_kafka/src/confluent_kafka.h    (CFL_VERSION_STR)
#   - tests/soak/setup_all_versions.py             (PYTHON_VERSIONS, LIBRDKAFKA_VERSIONS)
#   - CHANGELOG.md                                 (release section header)
#   - .semaphore/semaphore.yml                     (LIBRDKAFKA_VERSION, RC check)
#

set -e

errors=0
warnings=0

echo "=== Pre-release Validation ==="
echo ""

# --- Extract versions from source files ---

pyproject_version=$(python3 -c "
try:
    import tomllib
except ImportError:
    import tomli as tomllib
with open('pyproject.toml', 'rb') as f:
    print(tomllib.load(f)['project']['version'])
")

h_version=$(sed -n 's/^#define CFL_VERSION_STR "\(.*\)"/\1/p' \
    src/confluent_kafka/src/confluent_kafka.h)

semaphore_librdkafka=$(sed -n '/LIBRDKAFKA_VERSION/{n;s/.*value: *//p;}' \
    .semaphore/semaphore.yml | head -1)

echo "INFO: pyproject.toml        version = $pyproject_version"
echo "INFO: confluent_kafka.h     CFL_VERSION_STR = $h_version"
echo "INFO: semaphore.yml         LIBRDKAFKA_VERSION = $semaphore_librdkafka"
echo ""

# --- Consistency checks ---

# 1. pyproject.toml must match confluent_kafka.h
if [[ "$pyproject_version" != "$h_version" ]]; then
    echo "FAIL: Version mismatch: pyproject.toml ($pyproject_version) != confluent_kafka.h ($h_version)"
    errors=$((errors + 1))
else
    echo "OK:   pyproject.toml and confluent_kafka.h versions match"
fi

# 2. Version must appear in soak test version lists
if grep -q "'${pyproject_version}'" tests/soak/setup_all_versions.py; then
    echo "OK:   Version $pyproject_version found in tests/soak/setup_all_versions.py"
else
    echo "FAIL: Version $pyproject_version not found in tests/soak/setup_all_versions.py"
    errors=$((errors + 1))
fi

# 3. CHANGELOG.md should have a section for this version
if grep -q "^## v${pyproject_version}" CHANGELOG.md; then
    echo "OK:   CHANGELOG.md has entry for v${pyproject_version}"
else
    echo "WARN: CHANGELOG.md missing '## v${pyproject_version}' section"
    warnings=$((warnings + 1))
fi

# 4. For final release tags (no rc/dev suffix), librdkafka must not be an RC
if [[ -n "${SEMAPHORE_GIT_TAG_NAME:-}" ]]; then
    tag="$SEMAPHORE_GIT_TAG_NAME"
    if [[ ! "$tag" =~ rc && ! "$tag" =~ dev && "$semaphore_librdkafka" =~ RC ]]; then
        echo "FAIL: Final release tag ($tag) but LIBRDKAFKA_VERSION is still an RC ($semaphore_librdkafka)"
        errors=$((errors + 1))
    fi
fi

# --- Summary ---

echo ""
if [[ $errors -gt 0 ]]; then
    echo "VALIDATION FAILED: $errors error(s), $warnings warning(s)"
    exit 1
fi

echo "VALIDATION PASSED ($warnings warning(s))"