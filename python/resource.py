import json
import uuid
import re

from json.decoder import JSONDecodeError

from google.cloud import storage
from google.cloud.bigquery.client import Client
from google.cloud.bigquery.dataset import Dataset
from google.cloud.bigquery.job import WriteDisposition, CopyJob, \
    QueryPriority, QueryJob, SourceFormat, \
    ExtractTableToStorageJob, Compression, \
    DestinationFormat, LoadTableFromStorageJob
import time
from google.cloud.bigquery.table import Table
import logging


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

    def dump(self):
        return ""


class BqJobs:
    def __init__(self, bqClient: Client,
                 tableToJobMap: dict={},
                 pageSize: int=1000, page_limit: int=1):
        self.bqClient = bqClient
        self.tableToJobMap = tableToJobMap
        self.page_limit = page_limit
        self.pageSize = pageSize

    def jobs(self, state_filter=None):
        return self.bqClient.list_jobs(state_filter=state_filter)

    def __loadTableJobs__(self, state):
        logging.info("starting jobs load for ", state)
        """ scans through 5k previous jobs and puts into
        a map keyed by project/dataset/table/ the first job
        encountered for that any table.
        """
        iter = self.bqClient.list_jobs(max_results=self.pageSize,
                                       state_filter=state)
        while True:
            for t in iter:
                if t.destination:
                    tableKey = _buildDataSetTableKey_(t.destination)
                    print(tableKey + " was found in " + state)
                    print(str(t.destination.project))
                    if tableKey in self.tableToJobMap:
                        continue
                    self.tableToJobMap[tableKey] = t

            if not iter.next_page_token:
                break
            if iter.page_number > self.page_limit:
                break
            iter = self.bqClient.list_jobs(max_results=self.pageSize,
                                           page_token=iter.next_page_token,
                                           state_filter=state)
        print("finished jobs load for ", state)

    def loadTableJobs(self):
        [self.__loadTableJobs__(state) for state in ['running', 'pending']]

    def getJobForTable(self, table: Table):
        key = _buildDataSetTableKey_(table)
        if key in self.tableToJobMap:
            return self.tableToJobMap[key]
        return None


def _buildDataSetKey_(table: Table) -> str:
    """
    :param table: a bq table
    :return: colon concatenated project, dataset
    """
    return ":".join([table.dataset_name])


def _buildDataSetTableKey_(table: Table) -> str:
    """
    :param table:
    :return: colon concatenated project,  dataset, tablename
    """
    return ":".join([_buildDataSetKey_(table), table.name])


class BqTables:
    """ Basically a helper class whose purpose is
    to speed up the answer to questions such as
    does table x or view x exist and when was it updated.
    We lazily load the tables by waiting for a request for a table.
    Then we load that dataset of tables """
    def __init__(self, bqClient: Client):
        self.bqClient = bqClient
        self.datasetTableMap = {}  # a map to of tables

    def exists(self, bqTable: Table):
        return bqTable.exists()

    def _dsetkey_(self, table: Table) -> str:
        return _buildDataSetKey_(table)

    def _get_table_(self, table: Table):
        key = self.__dsetkey_(table)
        if key in self.datasetTableMap:
            dsetMap = self.datasetTableMap[key]
            if table.name in dsetMap:
                return dsetMap[table.name]
            else:
                return None
        else:  # load dataset
            iter = self.bqClient.dataset(table.dataset_name).list_tables()
            dsetMap = {}
            self.datasetTableMap[key] = dsetMap
            while True:
                for t in iter:
                    dsetMap[t.name] = t
                if not iter.next_page_token:
                    break
            if table.name in dsetMap:
                return table
            else:
                return None


class BqDatasetBackedResource(Resource):
    """ Resource for ensuring existence of dataset
     todo: maybe helpful to allow users to specify attributes
     of the dataset such as ttl of tables exist
    """
    def __init__(self, dataset: Dataset,
                 defTime: int, bqClient: Client):
        self.dataset = dataset
        self.bqClient = bqClient
        self.defTime = defTime
        self.existFlag = self.dataset.exists()
        if self.existFlag:
            self.dataset.reload()

    def exists(self):
        return self.dataset.exists()

    def updateTime(self):
        """ time in milliseconds.  None if not created """
        createdTime = self.dataset.modified
        if createdTime:
            return int(createdTime.strftime("%s")) * 1000
        return None

    def definitionTime(self):
        """ Time in milliseconds """
        return self.defTime

    def create(self):
        self.dataset.create()

    def key(self):
        return self.dataset.name

    def dependsOn(self, resource):
        return False

    def isRunning(self):
        return False

    def __str__(self):
        return ":".join([self.dataset.project, self.dataset.name])

class BqDataLoadTableResource(Resource):
    """ todo: currently we block during the creation of this
    table but we should probably treat this just like any table
    create and put it in the background
    """
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

        fieldDelimiter = '\t'
        with open(self.file, 'r') as readable:
            srcFormat = BqDataLoadTableResource.detectSourceFormat(
                                                readable.readline())
            if srcFormat != SourceFormat.CSV:
                fieldDelimiter = None

        with open(self.file, 'rb') as readable:
            ret = self.table.upload_from_file(
                readable, source_format=srcFormat,
                field_delimiter=fieldDelimiter,
                ignore_unknown_values=True,
                write_disposition=WriteDisposition.WRITE_TRUNCATE)
            wait_for_job(ret)

    def key(self):
        return ".".join([self.table.dataset_name, self.table.name])

    def dependsOn(self, resource: Resource):
        return self.table.dataset_name == resource.key()

    def isRunning(self):
        return False

    def __str__(self):
        return "localdata:" + ".".join([self.table.dataset_name,
                                        self.table.name])

    def detectSourceFormat(firstFileLine: str):
        try:
            json.loads(firstFileLine)
            return SourceFormat.NEWLINE_DELIMITED_JSON
        except JSONDecodeError:
            return SourceFormat.CSV


def makeJobName(parts: list):
    return "-".join(parts + [str(uuid.uuid4())])

# base resource class for all table back resources
class BqTableBasedResource(Resource):
    """ Base class of query based big query actions """
    def __init__(self, table: Table,
                 defTime: int, bqClient: Client):
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
        raise Exception("implement")

    def key(self):
        return ".".join([self.table.dataset_name,
                         self.table.name])

    def dependsOn(self, other: Resource):
        raise Exception("implement this function")

    def isRunning(self):
        raise Exception("implement this function")

class BqGcsTableLoadResource(BqTableBasedResource):
    # LoadTableFromStorageJob
    def __init__(self, table: Table,
                 defTime: int, bqClient: Client,
                 job: LoadTableFromStorageJob,
                 uris: tuple,
                 schema: tuple):
        super(BqGcsTableLoadResource, self)\
            .__init__(table, defTime, bqClient)
        self.job = job

    def isRunning(self):
        isJobRunning(self.job)

    def create(self):
        jobid = "-".join(["create", self.table.dataset_name,
                          self.table.name, str(uuid.uuid4())])
        self.job = LoadTableFromStorageJob(jobid, self.table,
                                           self.source_uris,
                                           self.client,
                                           self.schema)
        self.job.begin()



class BqQueryBasedResource(BqTableBasedResource):
    """ Base class of query based big query actions """
    def __init__(self, query: str, table: Table,
                 defTime: int, bqClient: Client):
        self.query = query
        self.table = table
        self.bqClient = bqClient
        self.defTime = defTime


    def updateTime(self):
        """ time in milliseconds.  None if not created """
        self.table.reload()
        createdTime = self.table.modified

        if createdTime:
            # hijack this step to update description
            if not self.table.description:
                self.table.description = "\n".join(["This table/view was " +
                                                    "created with the " +
                                                    "following query", "",
                                                    self.query, "",
                                                    "Edits will not be "
                                                    "saved"])
                self.table.update()
            return int(createdTime.strftime("%s")) * 1000
        return None

    def definitionTime(self):
        """ Time in milliseconds """
        return self.defTime

    def create(self):
        raise Exception("implement")

    def key(self):
        return ".".join([self.table.dataset_name,
                         self.table.name])

    def dependsOn(self, other: Resource):
        return self.legacyBqQueryDependsOn(other)

    def isRunning(self):
        raise Exception("implement this function")

    def legacyBqQueryDependsOn(self, other: Resource):
        if self == other:
            return False

        filtered = re.sub('[^0-9a-zA-Z\._]+', ' ', self.query)
        if strictSubstring("".join(["", other.key(), " "]), filtered):
            return True

            # we need a better way!
            # other may be simply a dataset in which case it will have not
            # .query field
        if isinstance(other, BqDatasetBackedResource) \
                and strictSubstring(other.key(), self.key()):
            return True
        return False


class BqViewBackedTableResource(BqQueryBasedResource):
    def create(self):
        self.table.view_query = self.query
        if (self.table.exists()):
            self.table.delete()
        self.table.schema = []
        self.table.create()

    def isRunning(self):
        return False

    def __str__(self):
        return "bqview:" + ".".join([self.table.dataset_name,
                                     self.table.name, "${query}"])

    def dump(self):
        return self.query


def strictSubstring(contained, container):
    """
    :rtype: bool
    """
    return contained in container and len(contained) < len(container)


class BqQueryBackedTableResource(BqQueryBasedResource):
    def __init__(self, query: str, table: Table,
                 defTime: int, bqClient: Client, queryJob: QueryJob):
        super(BqQueryBackedTableResource, self)\
            .__init__(query, table, defTime, bqClient)
        self.queryJob = queryJob

    def create(self):
        if self.table.exists():
            self.table.delete()

        jobid = "-".join(["create", self.table.dataset_name,
                          self.table.name, str(uuid.uuid4())])
        query_job = self.bqClient.run_async_query(jobid, self.query)
        query_job.allow_large_results = True
        query_job.flatten_results = False
        query_job.destination = self.table
        query_job.priority = QueryPriority.INTERACTIVE
        query_job.write_disposition = WriteDisposition.WRITE_TRUNCATE
        query_job.maximum_billing_tier = 2
        query_job.begin()
        self.queryJob = query_job

    # def key(self):
    #     return ".".join([self.table.dataset_name, self.table.name])

    def isRunning(self):
        if self.queryJob:
            self.queryJob.reload()
            print(self.queryJob.name,
                  self.queryJob.state, self.queryJob.errors)
            return self.queryJob.state in ['RUNNING', 'PENDING']
        else:
            return False

    def __str__(self):
        return "bqtable:" + ".".join([self.table.dataset_name,
                                     self.table.name, "${query}"])

    def dump(self):
        return self.query

class BqQueryBackedTableResource(BqQueryBasedResource):
    def __init__(self, query: str, table: Table,
                 defTime: int, bqClient: Client, queryJob: QueryJob):
        super(BqQueryBackedTableResource, self)\
            .__init__(query, table, defTime, bqClient)
        self.queryJob = queryJob

    def create(self):
        if self.table.exists():
            self.table.delete()

        jobid = "-".join(["create", self.table.dataset_name,
                          self.table.name, str(uuid.uuid4())])
        query_job = self.bqClient.run_async_query(jobid, self.query)
        query_job.allow_large_results = True
        query_job.flatten_results = False
        query_job.destination = self.table
        query_job.priority = QueryPriority.INTERACTIVE
        query_job.write_disposition = WriteDisposition.WRITE_TRUNCATE
        query_job.maximum_billing_tier = 2
        query_job.begin()
        self.queryJob = query_job

    def key(self):
        return ".".join([self.table.dataset_name, self.table.name])

    def isRunning(self):
        if self.queryJob:
            self.queryJob.reload()
            print(self.queryJob.name,
                  self.queryJob.state, self.queryJob.errors)
            return self.queryJob.state in ['RUNNING', 'PENDING']
        else:
            return False

    def __str__(self):
        return "bqtable:" + ".".join([self.table.dataset_name,
                                     self.table.name, "${query}"])

    def dump(self):
        return self.query

class BqExtractTableResource(Resource):
    def __init__(self,
                 table: Table,
                 defTime: int,
                 bqClient: Client,
                 gcsClient: storage.Client,
                 extractJob: ExtractTableToStorageJob,
                 uris: str,
                 compression=Compression.GZIP,
                 destinationFormat=DestinationFormat.NEWLINE_DELIMITED_JSON):

        self.extractJob = extractJob
        self.defTime = defTime
        self.table = table
        self.bqClient = bqClient
        self.gcsClient = gcsClient
        self.uris = uris
        # check uris
        (self.bucket, self.pathPrefix) = self.parseBucketAndPrefix(uris)
        self.compression = compression
        self.destinationFormat = destinationFormat

    def create(self):
        jobid = "-".join(["extract", self.table.name,
                          self.table.name, str(uuid.uuid4())])
        self.extractJob = self.bqClient.extract_table_to_storage(jobid,
                                                                 self.table,
                                                                 self.uris)
        self.extractJob.destination_format = self.destinationFormat
        self.extractJob.compression = self.compression
        self.extractJob.begin()

    def key(self):
        return ".".join(["extract", self.table.dataset_name,
                         self.table.name])

    def isRunning(self):
        if self.extractJob:
            self.extractJob.reload()
            print(self.extractJob.name,
                  self.extractJob.state, self.extractJob.errors)
            return self.extractJob.state in ['RUNNING', 'PENDING']
        else:
            return False

    def __str__(self):
        return "extract:" + ".".join([self.table.dataset_name,
                                     self.table.name])

    def exists(self):
        bucket = self.gcsClient.get_bucket(self.bucket)
        objs = [x for x in bucket.list_blobs(1, prefix=self.pathPrefix,
                                             delimiter="/")]
        return len(objs) > 0

    def dependsOn(self, other: Resource):
        return "extract." + other.key() == self.key()

    def dump(self):
        return ",".join(self.uris)

    def definitionTime(self):
        """ Time in milliseconds """
        return self.defTime

    def updateTime(self):
        objs = [int(o.updated.timestamp() * 1000) for o in
                self.gcsClient.bucket(self.bucket).list_blobs(
                prefix=self.pathPrefix)]

        if len(objs) == 0:
            return self.defTime

        return max(objs)

    def parseBucketAndPrefix(self, uris):
        bucket = uris.replace("gs://", "").split("/")[0]
        prefix = "/".join(uris.replace("gs://", "").split("/")[:-2])
        return (bucket, prefix)


def wait_for_job(job: QueryJob):
    while True:
        job.reload()  # Refreshes the state via a GET request.
        print("waiting for job", job.name)
        if job.state == 'DONE':
            if job.error_result:
                raise RuntimeError(job.errors)
            return
        time.sleep(1)


def export_data_to_gcs(dataset_name, table_name, destination):
    bigquery_client = Client()
    dataset = bigquery_client.dataset(dataset_name)
    table = dataset.table(table_name)
    job_name = str(uuid.uuid4())

    job = bigquery_client.extract_table_to_storage(
        job_name, table, destination)

    job.begin()
    job.result()  # Wait for job to complete

    print('Exported {}:{} to {}'.format(
        dataset_name, table_name, destination))

def isJobRunning(job):
    if job:
        job.reload()
        print(job.name,
              job.state, job.errors)
        return job.state in ['RUNNING', 'PENDING']
    else:
        return False
