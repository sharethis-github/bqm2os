#!/bin/bash -x

set -e

cd $(dirname $0)

for file in $(find /python -name '*.py')
do
    pycodestyle $file
    testfile=$(echo $file | awk -F/ '{printf "/test/"; n=3; while (n < NF) { printf $n"/"; n++}; print "test_"$NF}')
    coverage run -a --source=/python $testfile
    coverage report -m
done
