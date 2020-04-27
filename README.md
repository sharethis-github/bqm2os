# bqm2

2nd version of bq materializer - understands dependencies of executions, re-executes 
any queries which have changed or that have upstream queries and or tables which 
have changed

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
Within the folders you specify to /python/bqm2, the following "loader" types or 
file extensions are supported

.querytemplate
- allows you to write a query as a template and have it saved in a table.  by default it will be stored in a table of the same name as
the file and the default dataset specified on the command line of bqm2

- add a <filename>.querytemplate.vars to override table location

.view
- allows you to write a query as a template.  by default it will be stored in a table of the same name as
the file
- add a <filename>.querytemplate.vars to override table location 

.uniontable
- This behaves just like querytemplate EXCEPT it is expected that the variables
you specify in your .vars files will result in multiple and different queries 
being produced and unioned into a single table as specified by the table value in 
.vars file.

.unionview
- Just like uniontable, but for views.

.localdata
- allows you to upload csv and json files from disk.  requires a <filename>.localdata.schema 
file. schema can either be simple form i.e. col1:type1,col2:type2 or bigquery json schema form.
The code auto detects if your file is json or flat file. For flat files, tsv is all the is supported.

.gcsdata
- requires a .schema and optional .vars files
- allows you to load gs://.... data

# Example Usage
# note: aws-login.sh before running
export SRC_DIR='/Users/xxx/src' # DO NOT USE "~", IT WILL NOT WORK
./run.sh bash
cd /mnt/templates/bq/domo2
python /python/bqm2.py --varsFile=../hll_sketches/global.vars --defaultProject sharethis.com:quixotic-spot-526 --defaultDataset domo2 --maxConcurrent 20 --maxRetry 100000 --execute .
python /python/bqm2.py --defaultProject sharethis.com:quixotic-spot-526 --defaultDataset sop --maxConcurrent 20 --maxRetry 100000 --execute .
python /python/bqm2.py  --varsFile=../thirdparty-mergelog-dynamic/global.vars --defaultProject sharethis.com:quixotic-spot-526 --defaultDataset domo2 --maxConcurrent 20 --maxRetry 100000 --execute .
