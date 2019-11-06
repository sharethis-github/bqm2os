#!/bin/bash

set -e

cd $(dirname $0)

. env.sh

echo Building container and testing within docker build
docker build -t $imagename:$current_commit .

echo Executing integration tests - $(date)
echo
./int-test.sh
