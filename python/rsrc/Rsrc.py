import uuid
import re

from google.cloud.bigquery.client import Client
from google.cloud.bigquery.dataset import Dataset
from google.cloud.bigquery.job import WriteDisposition, CopyJob, \
    QueryPriority, QueryJob
import time
from google.cloud.bigquery.table import Table


def wait_for_job(job):
    while True:
        job.reload()  # Refreshes the state via a GET request.
        print("waiting for job", job)
        if job.state == 'DONE':
            if job.error_result:
                raise RuntimeError(job.errors)
            return
        time.sleep(1)


class Resource:
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
    def __init__(self, bqClient: Client):
        self.bqClient = bqClient

    def jobs(self, state_filter='running'):
        return self.bqClient.list_jobs(state_filter=state_filter)


class BqDatasetBackedResource(Resource):
    """ Resource for ensuring existence of dataset """
    def __init__(self, dataset: Dataset,
                 defTime: int, bqClient: Client):
        self.dataset = dataset
        self.bqClient = bqClient
        self.defTime = defTime

    def exists(self):
        return self.dataset.exists()

    def updateTime(self):
        """ time in milliseconds.  None if not created """
        self.dataset.reload()
        createdTime = self.dataset.modified
        if createdTime:
            return int(createdTime.strftime("%s")) * 1000
        return None

    def definitionTime(self):
        """ Time in milliseconds """
        return self.defTime

    def create(self):
        t = Dataset(self.dataset, self.bqClient)
        t.create(self.bqClient)

    def key(self):
        return self.dataset.name

    def dependsOn(self, resource):
        return False

    def isRunning(self):
        return False

    def __str__(self):
        return ".".join([self.dataset])


class BqDataLoadTableResource(Resource):
    def __init__(self, file: str, table: Table,
                 schema: tuple, defTime: int,
                 bqClient: Client):
        """ """
        self.file = file
        self.table = table
        self.bqClient = bqClient
        self.schema = schema
        self.defTime = defTime

    def exists(self):
        return self.table.exists()

    def updateTime(self):
        """ time in milliseconds.  None if not created """
        self.table.reload()
        createdTime = self.table.modified
        if createdTime:
            return int(createdTime.strftime("%s")) * 1000
        return None

    def definitionTime(self):
        """ Time in milliseconds """
        return self.defTime

    def create(self):
        self.table.schema = self.schema
        # self.table.create()
        with open(self.file, 'rb') as readable:
            ret = self.table.upload_from_file(
                readable, source_format='CSV', field_delimiter='\t',
                write_disposition=WriteDisposition.WRITE_TRUNCATE)
            wait_for_job(ret)

    def key(self):
        return ".".join([self.table.dataset_name, self.table.name])

    def dependsOn(self, resource: Resource):
        return False

    def isRunning(self):
        return False

    def __str__(self):
        return ".".join([self.table.dataset_name, self.table.name])


def makeJobName(parts: list):
    return "-".join(parts + [str(uuid.uuid4())])


class BqViewBackedTableResource(Resource):
    def __init__(self, query: str, table: Table,
                 defTime: int, bqClient: Client):
        self.query = query
        self.table = table
        self.bqClient = bqClient
        self.defTime = defTime

    def exists(self):
        return self.table.exists()

    def updateTime(self):
        """ time in milliseconds.  None if not created """
        self.table.reload()
        createdTime = self.table.modified
        if createdTime:
            return int(createdTime.strftime("%s")) * 1000
        return None

    def definitionTime(self):
        """ Time in milliseconds """
        return self.defTime

    def create(self):
        self.table.view_query = self.query
        if (self.table.exists()):
            self.table.update()
        else:
            self.table.create()

    def key(self):
        return ".".join([self.table.dataset_name, self.table.name])

    def dependsOn(self, other: Resource):
        return legacyBqQueryDependsOn(self, other, self.query)

    def isRunning(self):
        return False

    def __str__(self):
        return ".".join([self.table.dataset_name, self.table.name,
                         "${query}"])


def legacyBqQueryDependsOn(me: Resource, other: Resource, query: str):
    if me == other:
        return False

    filtered = re.sub('[^0-9a-zA-Z\._]+', ' ', query)
    if strictSubstring(" ".join(["", other.key(), ""]), filtered):
        return True

    # we need a better way!
    # other may be simply a dataset in which case it will have not
        # .query field
    if isinstance(other, BqDatasetBackedResource) \
            and strictSubstring(other.key(), me.key()):
        return True
    return False

    # \ or strictSubstring(other.key(), me.key())


def strictSubstring(contained, container):
    """

    :rtype: bool
    """
    return contained in container and len(contained) < len(container)


class BqQueryBackedTableResource(Resource):
    def __init__(self, query: str, table: Table,
                 defTime: int, bqClient: Client,
                 queryJob: QueryJob):
        self.query = query
        self.table = table
        self.bqClient = bqClient
        self.defTime = defTime
        self.queryJob = queryJob

    def exists(self):
        return self.table.exists()

    def updateTime(self):
        """ time in milliseconds.  None if not created """
        self.table.reload()
        createdTime = self.table.modified
        if createdTime:
            return int(createdTime.strftime("%s")) * 1000
        return None

    def definitionTime(self):
        """ Time in milliseconds """
        return self.defTime

    def create(self):
        jobid = "-".join(["create", self.table.dataset_name,
                          self.table.name, str(uuid.uuid4())])
        query_job = self.bqClient.run_async_query(jobid, self.query)
        # todo - allow standard
        # Use standard SQL syntax for queries.
        # See: https://cloud.google.com/bigquery/sql-reference/
        query_job.use_legacy_sql = True
        query_job.allow_large_results = True
        query_job.destination = self.table
        query_job.priority = QueryPriority.BATCH
        query_job.write_disposition = WriteDisposition.WRITE_TRUNCATE
        query_job.begin()
        self.queryJob = query_job

    def key(self):
        return ".".join([self.table.dataset_name, self.table.name])

    def dependsOn(self, other):
        return legacyBqQueryDependsOn(self, other, self.query)

    def isRunning(self):
        if self.queryJob:
            self.queryJob.reload()
            print(self, "checking running status of ",
                  self.queryJob.name, self.queryJob.state,
                  self.queryJob.errors)
            return self.queryJob.state in ['RUNNING', 'PENDING']
        else:
            """
                find previous job if any
                todo: jobs searching needs to be moved out to loader
            """
            jobs = self.bqClient.list_jobs(max_results=1000,
                                           state_filter=None)
            for job in jobs:
                if job.destination and job.destination.dataset_name == \
                        self.table.dataset_name \
                        and job.destination.name == self.table.name:
                    self.queryJob = job
                    # recurse
                    return self.isRunning()
            return False

    # def hasFailed(self):
    #     jobs = BqJobs(self.bqClient).jobs(state_filter=None)
    #     for job in jobs:
    #         if job.destination and job.destination.dataset_name == \
    #                 self.table.dataset_name \
    #            and job.destination.name == self.table.name:
    #                 if job.state == 'RUNNING' or job.state == 'PENDING':
    #                     return False
    #                 elif job.state == 'DONE' and job.errors:
    #                     print ("failed job: ", job.name, job.error_result)
    #                     return True

    def __str__(self):
        return ".".join([self.table.dataset_name, self.table.name,
                         "${query}"])
