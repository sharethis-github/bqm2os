#!/bin/bash

set -e

cd $(dirname $0)

. env.sh

docker build -t $imagename:$current_commit .
#test
docker run -v $(pwd)/test/:/test --rm $imagename:$current_commit /test/test.sh

docker run -v $(pwd)/int-test/:/int-test --rm $imagename:$current_commit /int-test/test-basic.sh
