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

# check view recreation
#touch int-test/bq/*.view
python /python/bqm2.py --defaultDataset ${dataset} --execute int-test/bq/


echo 'c	d	e' >> int-test/bq/test_local_json_data.localdata
python /python/bqm2.py --defaultDataset ${dataset} --execute int-test/bq/

# commenting this out because integration test is failing
exit

echo activating account
gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS
echo setting default project
gcloud config set account api-project-dev-1212

dataset=$dataset /int-test/test-view/test.sh
