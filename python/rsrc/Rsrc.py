import uuid
import re

from google.cloud.bigquery import table
from google.cloud.bigquery.client import Client
from google.cloud.bigquery.dataset import Dataset
from google.cloud.bigquery.job import WriteDisposition
import time

from google.cloud.bigquery.schema import SchemaField
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
    def __init__(self, bqClient: Client):
        self.bqClient = bqClient


    def jobs(self, state_filter='running'):
        return self.bqClient.list_jobs(state_filter=state_filter)


class BqDatasetBackedResource(Resource):
    """ Resource for ensuring existence of dataset """
    def __init__(self, dataset: str,
                 defTime: int, bqClient: Client):
        self.dataset = dataset
        self.bqClient = bqClient
        self.defTime = defTime


    def exists(self):
        return Dataset(self.dataset, self.bqClient).exists(self.bqClient)


    def updateTime(self):
        ### time in milliseconds.  None if not created """
        t = Dataset(self.dataset, self.bqClient)
        t.reload()
        createdTime = t.modified
        if createdTime: return int(createdTime.strftime("%s")) * 1000
        return None


    def definitionTime(self):
        """ Time in milliseconds """
        return self.defTime


    def create(self):
        t = Dataset(self.dataset, self.bqClient)
        t.create(self.bqClient)


    def key(self):
        return self.dataset


    def dependsOn(self, resource):
        return False


    def isRunning(self):
        return False


    def __str__(self):
        return ".".join([self.dataset])


class BqDataLoadTableResource(Resource):
    def __init__(self, file: str, dataset: str,
                 tableName: str, schema: tuple, defTime: int,
                 bqClient: Client):
        """ """
        self.file = file
        self.dataset = dataset
        self.tableName = tableName
        self.bqClient = bqClient
        self.schema = schema
        self.defTime = defTime


    def exists(self):
        return table.Table(self.tableName,
                           Dataset(self.dataset, self.bqClient)) \
            .exists(self.bqClient)


    def updateTime(self):
        ### time in milliseconds.  None if not created """
        t = table.Table(self.tableName, self.bqClient.dataset(self.dataset))
        t.reload()
        createdTime = t.modified
        if createdTime: return int(createdTime.strftime("%s")) * 1000
        return None


    def definitionTime(self):
        """ Time in milliseconds """
        return self.defTime


    def create(self):
        # SCHEMA = [
        #     SchemaField('id', 'STRING', mode='required'),
        #     SchemaField('desc', 'STRING', mode='required')
        # ]
        #
        table = self.bqClient.dataset(self.dataset).table(self.tableName,
                                                            self.schema)
        if (table.exists()):
            table.delete()
        table.create()
        with open(self.file, 'rb') as readable:
            ret = table.upload_from_file(
                readable, source_format='CSV', field_delimiter='\t')
            wait_for_job(ret)

    def key(self):
        return ".".join([self.dataset, self.tableName])


    def dependsOn(self, resource: Resource):
        return False


    def isRunning(self):
        return False

    def __str__(self):
        return ".".join([self.dataset, self.tableName, self.query])

def makeJobName(parts: list):
    return "-".join(parts + [str(uuid.uuid4())])


class BqViewBackedTableResource(Resource):
    def __init__(self, query: str, dataset: str,
                 tableName:str, defTime: int, bqClient: Client,
                 ):

        self.query = query
        self.dataset = dataset
        self.viewName = tableName
        self.bqClient = bqClient
        self.defTime = defTime


    def exists(self):
        return table.Table(self.viewName,
                           Dataset(self.dataset, self.bqClient)) \
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
        t = table.Table(self.viewName,
                        Dataset(self.dataset, self.bqClient))
        t.view_query = self.query
        t.create(self.bqClient)


    def key(self):
        return ".".join([self.dataset, self.viewName])


    def dependsOn(self, resource):
        if self == resource: return False
        filtered = re.sub('[^0-9a-zA-Z\._]+', ' ', self.query)
        return resource.key() in filtered or (resource.key() in self.key())


    def isRunning(self):
        return False


    def __str__(self):
        return ".".join([self.dataset, self.viewName, self.query])


class BqQueryBackedTableResource(Resource):
    def __init__(self, query: str, dataset: str,
                 tableName: str, defTime: int, bqClient: Client,
                 ):
        self.query = query
        self.dataset = dataset
        self.tableName = tableName
        self.bqClient = bqClient
        self.defTime = defTime


    def exists(self):
        return table.Table(self.tableName,
                           Dataset(self.dataset, self.bqClient)) \
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
        # todo - allow standard
        # Use standard SQL syntax for queries.
        # See: https://cloud.google.com/bigquery/sql-reference/
        query_job.use_legacy_sql = True
        query_job.allow_large_results = True
        query_job.destination = table.Table(self.tableName,
                                            Dataset(self.dataset,
                                                    self.bqClient))
        query_job.write_disposition = WriteDisposition.WRITE_TRUNCATE
        query_job.begin()

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
