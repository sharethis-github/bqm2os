# bqm2

2nd version of bq materializer - should understand dependencies of executions

# Supported File Suffixes

# Notes

## Cross Project support

Essentially, service accounts from project other than the project of the
tables be read from and written can be used to execute the dependency graph

### .localdata file load limitation
Due to an apparent limitation of the google python api used, the
job id allocated for data loads can only be provisioned against the
project of the dataset which contains the table being loaded to.

The implication of this is that the service account running the load
must have Bq Job Create permission on the project of the table being 
loaded to.


# Loader Types

.querytemplate

.view

.uniontable

.unionview

.localdata

.gcsdata

# Example Usage
# note: aws-login.sh before running
export SRC_DIR='/Users/xxx/src' # DO NOT USE "~", IT WILL NOT WORK
./run.sh bash
cd /mnt/templates/bq/domo2
python /python/bqm2.py --defaultProject sharethis.com:quixotic-spot-526 --defaultDataset domo2 --maxConcurrent 20 --maxRetry 100000 --execute .
python /python/bqm2.py --defaultProject sharethis.com:quixotic-spot-526 --defaultDataset sop --maxConcurrent 20 --maxRetry 100000 --execute .
python /python/bqm2.py  --varsFile=../thirdparty-mergelog-dynamic/global.vars --defaultProject sharethis.com:quixotic-spot-526 --defaultDataset domo2 --maxConcurrent 20 --maxRetry 100000 --execute .
