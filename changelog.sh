#!/bin/bash

data=`apt-cache policy $1 | awk '/\*\*\*/ {print $2} f{print $2;f=0} /\*\*\*/{f=1}'`
version=`echo $data | awk '{print $1}'`
version=`echo $version | sed -r s/^[0-9]+://`
URL=`echo $data | awk '{print $2}'`
if [ -z `echo $URL | grep ppa` ]; then
    echo "The package is not installed from PPA"
    exit
else
    user=`echo $URL | cut -d / -f 4`
    name=`echo $URL | cut -d / -f 5`
    wget  -q -O - https://launchpad.net/~$user/+archive/ubuntu/$name/+files/$1_${version}_source.changes | \
    awk '/Changes:/{f=1;next}/Checksums/{f=0}f' 
fi
