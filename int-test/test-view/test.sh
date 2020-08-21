#!/bin/bash

set -e
set -o nounset

cd $(dirname $0)

python /python/bqm2.py --defaultProject api-project-dev-1212 --defaultDataset ${dataset} --execute .

echo calling bq show
bq show --project_id api-project-dev-1212 ${dataset}.test_view | grep VIEW > /dev/null 2>&1
