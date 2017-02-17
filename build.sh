#!/bin/bash

set -e

cd $(dirname $0)
imagename=$(pwd | awk -F/ '{print $NF}')
HASH=`git rev-parse HEAD`
HASH=${HASH:0:10}
docker build -t $imagename:$HASH . 
