#!/usr/bin/env python3
#
#
# Get python versions required for cibuildwheel from their config and
# install them. This implementation is based on cibuildwheel 2.12.0
# version. Might need tweak if something changes in cibuildwheel.
#
# This was added as there is a permission issue when cibuildwheel
# tries to install these versions on its own.
#

import platform
import sys
import os
import tomli
import urllib.request
import re
import shutil


cibuildwheel_version = sys.argv[1]
config_url = "https://raw.githubusercontent.com/pypa/cibuildwheel/" + \
    f"v{cibuildwheel_version}/cibuildwheel/resources/build-platforms.toml"
print(f"Config URL is '{config_url}'")

response = urllib.request.urlopen(config_url).read()

content = response.decode('utf-8')
d = tomli.loads(content)
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
print(f"CWD is: '{this_file_path}'")
tmp_download_dir_full_path = os.path.join(os.getcwd(), tmp_download_dir)
tmp_pkg_file_full_path = os.path.join(tmp_download_dir_full_path, tmp_pkg_file_name)
if os.path.exists(tmp_download_dir_full_path):
    shutil.rmtree(tmp_download_dir_full_path)
os.mkdir(tmp_download_dir)
os.chdir(tmp_download_dir)
install_command = f"sudo installer -pkg {tmp_pkg_file_name} -target /"

for py_version_info in py_versions_info:
    identifier = py_version_info[0]
    pkg_url = py_version_info[1]
    print(f"Installing '{identifier}' from '{pkg_url}'")
    os.system(f"curl {pkg_url} --output {tmp_pkg_file_name}")
    os.system(install_command)
    os.remove(tmp_pkg_file_full_path)

os.chdir(this_file_path)
shutil.rmtree(tmp_download_dir_full_path)
