#!/bin/bash

set -e
set -o nounset

cd /
touch int-test/bq/*

dataset=int_test_$(date +%s)
python /python/bqm2.py --defaultDataset atest2 --dumpToFolder /tmp/ int-test/bq/ | sort | tee /tmp/debug
diff /tmp/debug /int-test/test.expected

echo Dataset for test is ${dataset}
python /python/bqm2.py --defaultDataset ${dataset} --execute int-test/bq/
