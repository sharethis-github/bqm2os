#!/bin/bash

set -e
set -o nounset

cd $(dirname $0)

. env.sh

python /python/bqm2.py --varsFile global.vars --defaultProject $PROJECT --defaultDataset $DEFAULT_DATASET --dumpToFolder /tmp/ .
