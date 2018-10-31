#!/bin/bash

set -e

cd $(dirname $0)

. env.sh

docker build -t $imagename:$current_commit .

if [ -z $SRC_DIR ]; then
  echo "Must specify SRC_DIR. Path where a user's git repos are present."
  exit 1;
fi
echo "SRC: $SRC_DIR"


# get creds
mkdir -p /tmp/creds
docker run -ti -v ~/.aws/:/root/.aws -v /tmp/creds:/tmp/creds -e AWS_DEFAULT_PROFILE=prod -e AWS_DEFAULT_REGION=us-east-1 docker.io/stops/secrets-manager-v2:5b20bdefaa python /src/client.py k8app__bq-third-party__staging__bqm2-json__gcloud-private-key /tmp/creds/gcloud-private-key

docker run -e GOOGLE_APPLICATION_CREDENTIALS=/gcloud-private-key \
-v $SRC_DIR/config/templates/secrets/taxonomy-run:/mnt/run \
-v $SRC_DIR/dynamic-data-config/endpoints:/endpoints \
-v $SRC_DIR/dynamic-data-config/bigquery:/bigquery \
-v /tmp/creds/gcloud-private-key:/gcloud-private-key \
-v $SRC_DIR/taxonomy-mapping/:/mnt/templates \
-v ~/.vimrc:/root/.vimrc \
--name bqm2 -v ~/.config:/root/.config \
-v $(pwd)/python:/python \
-v $(pwd)/test:/test \
-v $(pwd)/int-test:/int-test \
-v ~/.aws:/root/.aws \
-ti --rm $imagename:$current_commit $@

rm -rf /tmp/creds/gcloud-private-key
