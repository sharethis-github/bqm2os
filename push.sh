#!/bin/bash

set -e
cd $(dirname $0)

. ./env.sh

docker tag  ${imagename} ${registryimage:?}:${current_commit}
docker push ${registryimage:?}:${current_commit}
