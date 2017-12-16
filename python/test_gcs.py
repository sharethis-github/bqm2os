from google.cloud.bigquery import Client
import uuid
import resource
import loader

from oauth2client.client import GoogleCredentials
credentials = GoogleCredentials.get_application_default()

from google.cloud.bigquery.job import WriteDisposition, CopyJob, \
    QueryPriority, QueryJob, SourceFormat, \
    ExtractTableToStorageJob, Compression, \
    DestinationFormat

client = Client()
dataset_name="temporary"

dataset = client.dataset(dataset_name)
table_name="testname"
if not dataset.exists():
    dataset.create()

table = dataset.table(table_name)
job_id = str(uuid.uuid4())

uris="gs://sharethis-thirdparty-mergelog/v1/viglinkdata/2017121603/*.gz"
job = client.load_table_from_storage(job_id, table, uris)


loder = loader.BqDataFileLoader(client)

with open("/int-test/bq/mergelog_mini_sample.localdata.schema", "r") as f:
    schema = f.read()

schema=loder.loadSchemaFromString(schema)
job.schema = schema
job.ignore_unknown_values = True
job.source_format = SourceFormat.NEWLINE_DELIMITED_JSON
job.begin()
resource.wait_for_job(job)
