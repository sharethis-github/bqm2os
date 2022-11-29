#!/bin/bash

set -e
set -o nounset

cd $(dirname $0)

. env.sh

docker build --platform linux/amd64 -t $imagename:$current_commit .

if [[ ! -f ~/.vimrc ]]
then
    touch ~/.vimrc
fi

# create an empty .vimrc to prevent this getting mounted as a folder. Do us all a favor and set expandtab so we get no tabs.
touch ~/.vimrc

QUERIES=${QUERIES:-$(pwd)/queries}
MOUNT=${MOUNT:-/queries}
echo mounting ${QUERIES} to ${MOUNT}.  Set these yourself to override where your queries live and where they are mounted to in the countainer
docker run --platform linux/amd64 -e GOOGLE_APPLICATION_CREDENTIALS=/gcloud-private-key \
-e AWS_SHARED_CREDENTIALS_FILE=/root/.aws/mfa \
-v ${GOOGLE_APPLICATION_SERVICE_ACCOUNT}:/gcloud-private-key \
-v ~/.vimrc:/root/.vimrc \
-v ${QUERIES}:${MOUNT} \
--name bqm2 \
-v $(pwd)/python:/python \
-v $(pwd)/test:/test \
-v $(pwd)/int-test:/int-test \
-v ~/.aws:/root/.aws \
-ti --rm $imagename:$current_commit $@
