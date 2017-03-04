#!/bin/bash

set -e

cd $(dirname $0)
. ./build.sh

docker run -e GOOGLE_APPLICATION_CREDENTIALS=/gcloud-private-key \
-v ~/src/config/templates/secrets/int/gcloud-json/bqm2-gcloud-private-key:/gcloud-private-key \
-v ~/src/taxonomy-mapping/:/taxonomy-mapping/ \
-v ~/.vimrc:/root/.vimrc \
--name $imagename -v ~/.config:/root/.config -v $(pwd)/bq:/bq -v $(pwd)/python:/python -v ~/.aws:/root/.aws -ti --rm  $imagename:$HASH $@
