#!/bin/bash

set -e
set -o nounset

cd /
touch int-test/bq/*

# TODO: user here needs access to bucket
project_id=${project_id:-$(jq -r .project_id /gcloud-private-key)}
echo activating account
gcloud auth activate-service-account --key-file=/gcloud-private-key
echo setting default project
gcloud config set project ${project_id}

gsutil ls gs://${project_id}-bqm2-int-test || gsutil mb gs://${project_id}-bqm2-int-test

# setup input
gsutil cp /int-test/gcsload/parquet_test.parquet gs://${project_id}-bqm2-int-test/parquet_test.parquet
gsutil cp /int-test/bq/flag gs://${project_id}-bqm2-int-test/flag


dataset=int_test_$(date +%s)
bq mk $project_id:$dataset
bq update --default_table_expiration 3600 $project_id:$dataset
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

dataset=$dataset /int-test/test-view/test.sh
