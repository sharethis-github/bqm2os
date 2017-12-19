#!/bin/bash

set -e

cd $(dirname $0)

. env.sh

docker build -t $imagename:$current_commit .

echo Executing unit tests
./unit-test.sh

echo Executing integration tests - $(date)
echo
./int-test.sh
