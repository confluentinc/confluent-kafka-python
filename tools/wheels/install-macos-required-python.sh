#!/bin/bash

url=$1
tmp="tmp_dnld"
[[ -d $tmp ]] || mkdir -p "$tmp"
cd $tmp
curl $url --output Python.pkg
sudo installer -pkg Python.pkg -target /
cd ..
rm -rf $tmp