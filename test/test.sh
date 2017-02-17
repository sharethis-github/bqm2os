#!/bin/bash

set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR
pep8 /python/*.py
pep8 /test/*.py

#export PYTHONPATH=$DIR/src
#coverage run -a --source=src/ test/test_run.py
#coverage run -a --source=src/ test/test_bq.py
#coverage run -a --source=src/ test/test_mongo.py
#coverage run -a --source=src/ test/test_sqs.py
#coverage run -a --source=src/ test/test_keyword_expansion.py
#coverage report -m
