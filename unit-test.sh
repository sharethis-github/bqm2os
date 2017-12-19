#!/bin/bash

set -e

cd $(dirname $0)

. env.sh

docker build -t $imagename:$current_commit .
docker run -v $(pwd)/test/:/test --rm $imagename:$current_commit /test/test.sh
