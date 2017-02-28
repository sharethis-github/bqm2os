import uuid
import re

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

    def key(self):
        raise Exception("Please implement")

    def dependsOn(self, resource):
        raise Exception("Please implement")


class BqJobs:
    def __init__(self, bqClient):
        self.bqClient = bqClient

    def jobs(self, state_filter='running'):
        return self.bqClient.list_jobs(state_filter=state_filter)

class BqViewBackedTableResource(Resource):
    def __init__(self, query, dataset,
                 tableName, defTime, bqClient,
                 ):
        self.query = query
        self.dataset = dataset
        self.viewName = tableName
        self.bqClient = bqClient
        self.defTime = defTime

    def exists(self):
        return table.Table(self.viewName,
                           Dataset(self.dataset, self.bqClient))\
                           .exists(self.bqClient)

    def updateTime(self):
        ### time in milliseconds.  None if not created """
        t = table.Table(self.viewName,
                        Dataset(self.dataset, self.bqClient))
        t.reload()
        createdTime = t.modified
        if createdTime: return int(createdTime.strftime("%s")) * 1000
        return None

    def definitionTime(self):
        """ Time in milliseconds """
        return self.defTime

    def create(self):
        jobid = "-".join(["create", self.dataset,
                          self.viewName, str(uuid.uuid4())])
        query_job = self.bqClient.run_async_query(jobid, self.query)
        # Use standard SQL syntax for queries.
        # See: https://cloud.google.com/bigquery/sql-reference/
        query_job.use_legacy_sql = True
        query_job.allow_large_results = True
        query_job.destination = table.Table(self.viewName,
                                            Dataset(self.dataset,
                                                    self.bqClient))
        query_job.write_disposition = WriteDisposition.WRITE_TRUNCATE
        query_job.begin()

    def key(self):
        return ".".join([self.dataset, self.viewName])

    def dependsOn(self, resource):
        filtered = re.sub('[^0-9a-zA-Z\._]+', ' ', self.query)
        return resource.key() in filtered

    def isRunning(self):
        jobs = BqJobs(self.bqClient).jobs(state_filter='running')
        return len([x.destination.name for x in jobs if
                    x.destination and x.destination.dataset_name == self.dataset
                    and x.destination.name == self.viewName]) != 0

    def __str__(self):
        return ".".join([self.dataset, self.viewName, self.query])

class BqQueryBackedTableResource(Resource):
    def __init__(self, query, dataset,
                 tableName, defTime, bqClient,
                 ):
        self.query = query
        self.dataset = dataset
        self.tableName = tableName
        self.bqClient = bqClient
        self.defTime = defTime

    def exists(self):
        return table.Table(self.tableName,
                           Dataset(self.dataset, self.bqClient))\
                           .exists(self.bqClient)

    def updateTime(self):
        ### time in milliseconds.  None if not created """
        t = table.Table(self.tableName,
                    Dataset(self.dataset, self.bqClient))
        t.reload()
        createdTime = t.modified
        if createdTime: return int(createdTime.strftime("%s")) * 1000
        return None

    def definitionTime(self):
        """ Time in milliseconds """
        return self.defTime

    def create(self):
        jobid = "-".join(["create", self.dataset,
                          self.tableName, str(uuid.uuid4())])
        query_job = self.bqClient.run_async_query(jobid, self.query)
        # Use standard SQL syntax for queries.
        # See: https://cloud.google.com/bigquery/sql-reference/
        query_job.use_legacy_sql = True
        query_job.allow_large_results = True
        query_job.destination = table.Table(self.tableName,
                                            Dataset(self.dataset,
                                                    self.bqClient))
        query_job.write_disposition = WriteDisposition.WRITE_TRUNCATE
        query_job.begin()
#        self.wait_for_job(query_job)

    def wait_for_job(self, job):
        while True:
            job.reload()  # Refreshes the state via a GET request.
            print("waiting for job", job)
            if job.state == 'DONE':
                if job.error_result:
                    raise RuntimeError(job.errors)
                return
            time.sleep(1)

    def key(self):
        return ".".join([self.dataset, self.tableName])

    def dependsOn(self, resource):
        filtered = re.sub('[^0-9a-zA-Z\._]+', ' ', self.query)
        return resource.key() in filtered

    def isRunning(self):
        jobs = BqJobs(self.bqClient).jobs(state_filter='running')
        return len([x.destination.name for x in jobs if
         x.destination and x.destination.dataset_name == self.dataset
         and x.destination.name == self.tableName]) != 0

    def __str__(self):
        return ".".join([self.dataset, self.tableName, self.query])