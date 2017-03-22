#!/bin/bash

set -e

cd $(dirname $0)

. env.sh

docker build -t $imagename:$current_commit .

docker run -e GOOGLE_APPLICATION_CREDENTIALS=/gcloud-private-key \
-v ~/src/config/templates/secrets/int/bqm2-json/bqm2-gcloud-private-key:/gcloud-private-key \
-v ~/src/taxonomy-mapping/:/mnt/templates \
-v ~/.vimrc:/root/.vimrc \
--name bqm2 -v ~/.config:/root/.config \
-v $(pwd)/python:/python \
-v $(pwd)/test:/test \
-v $(pwd)/int-test:/int-test \
-v ~/.aws:/root/.aws \
-ti --rm $imagename:$current_commit $@
