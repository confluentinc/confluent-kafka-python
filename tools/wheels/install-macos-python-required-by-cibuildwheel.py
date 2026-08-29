#!/usr/bin/env python3
#
#
# Get python versions required for cibuildwheel from their config and
# install them. This implementation is based on cibuildwheel 3.2.1
# version. Might need tweak if something changes in cibuildwheel.
#
# This was added as there is a permission issue when cibuildwheel
# tries to install these versions on its own.
#

import os
import platform
import re
import shutil
import sys
import tomllib
import urllib.request

cibuildwheel_version = sys.argv[1]
config_url = (
    "https://raw.githubusercontent.com/pypa/cibuildwheel/"
    + f"v{cibuildwheel_version}/cibuildwheel/resources/build-platforms.toml"
)
print(f"Config URL is '{config_url}'")

response = urllib.request.urlopen(config_url).read()

content = response.decode('utf-8')
d = tomllib.loads(content)
macos_config = d['macos']['python_configurations']

machine_arc = platform.machine()
print(f"Machine Architecture is '{machine_arc}'")
machine_arc_regex_string = f".*{machine_arc}"
machine_arc_regex = re.compile(machine_arc_regex_string)

skip_versions = os.environ['CIBW_SKIP']
print(f"Versions to skip are '{skip_versions}'")
skip_versions_list = skip_versions.split()
skip_versions_regex_string = ("|".join(skip_versions_list)).replace("*", ".*")
skip_versions_regex = re.compile(skip_versions_regex_string)

py_versions_info = []

for py_version_config in macos_config:
    identifier = py_version_config['identifier']
    if not skip_versions_regex.match(identifier) and machine_arc_regex.match(identifier):
        pkg_url = py_version_config['url']
        py_versions_info.append((identifier, pkg_url))

tmp_download_dir = "tmp_download_dir"
tmp_pkg_file_name = "Package.pkg"
this_file_path = os.getcwd()
script_dir = os.path.dirname(os.path.abspath(__file__))
print(f"CWD is: '{this_file_path}'")
tmp_download_dir_full_path = os.path.join(os.getcwd(), tmp_download_dir)
tmp_pkg_file_full_path = os.path.join(tmp_download_dir_full_path, tmp_pkg_file_name)
if os.path.exists(tmp_download_dir_full_path):
    shutil.rmtree(tmp_download_dir_full_path)
os.mkdir(tmp_download_dir)
os.chdir(tmp_download_dir)

for py_version_info in py_versions_info:
    identifier = py_version_info[0]
    pkg_url = py_version_info[1]
    print(f"Installing '{identifier}' from '{pkg_url}'")
    os.system(f"curl {pkg_url} --output {tmp_pkg_file_name}")

    install_args = ""
    cpython_tag = identifier.split('-')[0]
    if cpython_tag.endswith('t'):
        # Free-threaded identifiers (e.g. cp314t-macosx_arm64) share the
        # same installer .pkg as their regular counterpart (cp314-...).
        # The free-threaded framework is an opt-in installer choice that
        # is unchecked by default, so it must be explicitly selected via
        # -applyChoiceChangesXML, otherwise only the regular (GIL)
        # framework gets installed and the cpython314t interpreter is
        # never created. Mirrors cibuildwheel's own macOS installer logic.
        py_version_digits = cpython_tag[2:-1]
        choicechanges_xml = os.path.join(script_dir, f"free-threaded-enable-{py_version_digits}.xml")
        install_args = f"-applyChoiceChangesXML {choicechanges_xml} "
    os.system(f"sudo installer -pkg {tmp_pkg_file_name} {install_args}-target /")
    os.remove(tmp_pkg_file_full_path)

os.chdir(this_file_path)
shutil.rmtree(tmp_download_dir_full_path)
