# reference
# - https://googlecloudplatform.github.io/google-cloud-python/stable/bigquery-client.html
# - https://cloud.google.com/bigquery/querying-data#asynchronous-queries
# Imports the Google Cloud client library
import uuid
import time

from google.cloud import bigquery

# Instantiates a client
from google.cloud.bigquery import table
from google.cloud.bigquery.job import WriteDisposition


def wait_for_job(job):
    while True:
        job.reload()  # Refreshes the state via a GET request.
        print("waiting for job", job)
        if job.state == 'DONE':
            if job.error_result:
                raise RuntimeError(job.errors)
            return
        time.sleep(1)

client = bigquery.Client()

# The name for the new dataset
dataset_name = 'my_new_dataset'

# Prepares the new dataset
dataset = client.dataset(dataset_name)

print('Dataset {} created.'.format(dataset.name))

jobid = str(uuid.uuid4())

query_job = client.run_async_query(jobid, """
    SELECT
        APPROX_TOP_COUNT(corpus, 10) as title,
        COUNT(*) as unique_words
    FROM `publicdata.samples.shakespeare`;""")

# Use standard SQL syntax for queries.
# See: https://cloud.google.com/bigquery/sql-reference/
query_job.use_legacy_sql = False
query_job.allow_large_results = True
query_job.destination = table.Table("test_table", dataset)
query_job.write_disposition = WriteDisposition.WRITE_TRUNCATE
query_job.begin()
wait_for_job(query_job);
