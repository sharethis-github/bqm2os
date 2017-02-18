import uuid

from google.cloud.bigquery import table
from google.cloud.bigquery.dataset import Dataset
from google.cloud.bigquery.job import WriteDisposition
import time

class Resource:
    def __init__(self):
        raise Exception("Please implement")

    def exists(self):
        raise Exception("Please implement")

    def updateTime(self):
        raise Exception("Please implement")

    def definitionTime(self):
        raise Exception("Please implement")

    def create(self):
        raise Exception("Please implement")

class BqQueryBackedTableResource(Resource):
    def __init__(self, query, dataset, tableName, definitionTime, bqClient):
        self.query = query
        self.dataset = dataset
        self.tableName = tableName
        self.bqClient = bqClient
        self.definitionTime = definitionTime

    def exists(self):
        return table.Table(self.tableName,
                           Dataset(self.dataset, self.bqClient))\
                           .exists(self.bqClient);

    def updateTime(self):
        raise Exception("Please implement")

    def definitionTime(self):
        raise Exception("Please implement")

    def create(self):
        jobid = "-".join(["create", self.dataset, self.tableName, str(uuid.uuid4())])
        query_job = self.bqClient.run_async_query(jobid, self.query)
        # Use standard SQL syntax for queries.
        # See: https://cloud.google.com/bigquery/sql-reference/
        query_job.use_legacy_sql = True
        query_job.allow_large_results = True
        query_job.destination = table.Table(self.tableName, Dataset(self.dataset, self.bqClient))
        query_job.write_disposition = WriteDisposition.WRITE_TRUNCATE
        query_job.begin()
        self.wait_for_job(query_job);

    def wait_for_job(self, job):
        while True:
            job.reload()  # Refreshes the state via a GET request.
            print("waiting for job", job)
            if job.state == 'DONE':
                if job.error_result:
                    raise RuntimeError(job.errors)
                return
            time.sleep(1)
