#!/bin/bash

set -e

cd $(dirname $0)

. ./build.sh

#test
docker run -v $(pwd)/test/:/test --rm $imagename:$HASH /test/test.sh
