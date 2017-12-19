#!/bin/bash

set -e

cd $(dirname $0)

. env.sh

docker build -t $imagename:$current_commit .
docker run -e GOOGLE_APPLICATION_CREDENTIALS=/gcloud-private-key \
-v /mnt/bqm2-int-test/gcloud-private-key:/gcloud-private-key \
-v $(pwd)/int-test/:/int-test --rm $imagename:$current_commit /int-test/test-basic.sh
