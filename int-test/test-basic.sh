#!/bin/bash

set -e
set -o nounset

gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS

cd /
touch int-test/bq/*

dataset=int_test_$(date +%s)
python /python/bqm2.py --defaultDataset atest2 --dumpToFolder /tmp/ int-test/bq/ | sort | tee /tmp/debug
diff /tmp/debug /int-test/test.expected

echo Dataset for test is ${dataset}
python /python/bqm2.py --defaultDataset ${dataset} --execute int-test/bq/

# check view recreation
touch int-test/bq/*.view
python /python/bqm2.py --defaultDataset ${dataset} --execute int-test/bq/


echo 'c	d	e' >> int-test/bq/test_local_json_data.localdata
python /python/bqm2.py --defaultDataset ${dataset} --execute int-test/bq/
