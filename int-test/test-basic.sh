#!/bin/bash

set -e
set -o nounset

cd /
touch int-test/bq/*
python /python/bqm2.py --defaultDataset atest2 --dumpToFolder /tmp/ int-test/bq/ | sort > /tmp/debug
diff /tmp/debug /int-test/test.expected

python /python/bqm2.py --defaultDataset atest2 --execute int-test/bq/
