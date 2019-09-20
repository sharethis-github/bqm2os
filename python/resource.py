import json
import uuid
import re
from enum import Enum

import hashlib
from json.decoder import JSONDecodeError

from google.api.core.exceptions import NotFound
from google.cloud import storage
from google.cloud.bigquery.client import Client
from google.cloud.bigquery.dataset import Dataset
from google.cloud.bigquery.job import WriteDisposition, \
    QueryPriority, QueryJob, SourceFormat, \
    ExtractTableToStorageJob, Compression, \
    DestinationFormat, LoadTableFromStorageJob, _AsyncJob
import time
from google.cloud.bigquery.table import Table
import logging


class Resource:
    def exists(self):
        raise Exception("Please implement")

    def updateTime(self):
        raise Exception("Please implement")

    def create(self):
        raise Exception("Please implement")

    def shouldUpdate(self):
        raise Exception("Please implement")

    def key(self):
        raise Exception("Please implement")

    def dependsOn(self, resource):
        raise Exception("Please implement")

    def dump(self):
        return ""

    def __eq__(self, other):
        raise Exception("Must implement __eq__")


class BqJobs:
    def __init__(self, bqClient: Client,
                 tableToJobMap: dict = {},
                 pageSize: int = 1000, page_limit: int = 1):
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
                 bqClient: Client):
        self.dataset = dataset
        self.bqClient = bqClient
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

    def create(self):
        self.dataset.create()

    def key(self):
        return self.dataset.name

    def dependsOn(self, resource):
        return False

    def isRunning(self):
        return False

    def shouldUpdate(self):
        return False

    def __str__(self):
        return ":".join([self.dataset.project, self.dataset.name])

    def __eq__(self, other):
        try:
            return self.key() == other.key() and \
                   self.dataset.project == other.dataset.project and \
                   self.dataset.name == other.dataset.name
        except Exception:
            return False


def makeJobName(parts: list):
    return "-".join(parts + [str(uuid.uuid4())])


# base resource class for all table back resources
class BqTableBasedResource(Resource):
    """ Base class of query based big query actions """
    def __init__(self, table: Table, bqClient: Client):
        self.table = table
        self.bqClient = bqClient

    def exists(self):
        return self.table.exists()

    def updateTime(self):
        """ time in milliseconds.  None if not created """
        self.table.reload()
        createdTime = self.table.modified

        if createdTime:
            return int(createdTime.strftime("%s")) * 1000
        return None

    def create(self):
        raise Exception("implement")

    def key(self):
        return ".".join([self.table.dataset_name,
                         self.table.name])

    def dependsOn(self, other: Resource):
        raise Exception("implement this function")

    def isRunning(self):
        raise Exception("implement this function")

    def __str__(self):
        return ".".join([self.table.dataset_name,
                         self.table.name, "${query}"])


def generate_file_md5(filename, blocksize=2**20):
    m = hashlib.md5()
    with open(filename, "rb") as f:
        while True:
            buf = f.read(blocksize)
            if not buf:
                break
            m.update(buf)
    return m.hexdigest()


class BqDataLoadTableResource(BqTableBasedResource):
    """ todo: currently we block during the creation of this
    table but we should probably treat this just like any table
    create and put it in the background
    """
    def __init__(self, file: str, table: Table,
                 schema: tuple, bqClient: Client,
                 job: _AsyncJob):
        """ """
        super(BqDataLoadTableResource, self).__init__(table, bqClient)
        self.file = file
        self.table = table
        self.bqClient = bqClient
        self.schema = schema
        self.job = job

    def exists(self):
        return self.table.exists()

    def makeHashTag(self):
        schemahash = generate_file_md5(self.file + ".schema")
        return "filehash:" + generate_file_md5(self.file) + ":" + schemahash

    def updateTime(self):
        """ time in milliseconds.  None if not created """
        self.table.reload()
        createdTime = self.table.modified

        hashtag = self.makeHashTag()

        if createdTime:
            # hijack this step to update description - ugh - debt supreme
            if not self.table.description:
                self.table.description = "\n".join(["Do not edit", hashtag])
                self.table.update()
            return int(createdTime.strftime("%s")) * 1000
        return None

    def create(self):
        self.table.schema = self.schema

        if self.exists():
            print("Table exists and we're wiping out the description")
            self.table.description = ""
            self.table.update()

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
            self.job = ret

    def key(self):
        return ".".join([self.table.dataset_name, self.table.name])

    def dependsOn(self, resource: Resource):
        return self.table.dataset_name == resource.key()

    def isRunning(self):
        return isJobRunning(self.job)

    def __str__(self):
        return "localdata:" + ".".join([self.table.dataset_name,
                                        self.table.name])

    def detectSourceFormat(firstFileLine: str):
        try:
            json.loads(firstFileLine)
            return SourceFormat.NEWLINE_DELIMITED_JSON
        except JSONDecodeError:
            return SourceFormat.CSV

    def __eq__(self, other):
        try:
            return self.file == other.file and self.key() == other.key()
        except Exception:
            return False

    def shouldUpdate(self):
        self.updateTime()
        if not self.makeHashTag() in self.table.description:
            return True
        return False


def processLoadTableOptions(options: dict, job: LoadTableFromStorageJob):
    """
    :param options: A dictionary of options matching fields available
     on LoadTableFromStorageJob
    :param job: An instance of LoadTableFromStorageJob
    :return: None - simply decorates the job
    """
    formats = {
        "AVRO": SourceFormat.AVRO,
        "NEWLINE_DELIMITED_JSON": SourceFormat.NEWLINE_DELIMITED_JSON,
        "CSV": SourceFormat.CSV,
        "DATASTORE_BACKUP": SourceFormat.DATASTORE_BACKUP
    }

    if "source_format" in options:
        value = options['source_format']
        if value not in formats:
            raise KeyError("Please use only one of the following: "
                           + ",".join(formats.keys()))
        job.source_format = formats[value]

    if "max_bad_records" in options:
        job.max_bad_records = int(options['max_bad_records'])

    if 'ignore_unknown_values' in options:
        job.ignore_unknown_values = bool(options['ignore_unknown_values'])

    write_disp = {
        "WRITE_APPEND": WriteDisposition.WRITE_APPEND,
        "WRITE_EMPTY": WriteDisposition.WRITE_EMPTY,
        "WRITE_TRUNCATE": WriteDisposition.WRITE_TRUNCATE
    }

    if 'write_disposition' in options:
        value = options['write_disposition']
        if value not in write_disp:
            raise KeyError("Please use only one of the following: "
                           + ",".join(write_disp.keys()))
        job.write_disposition = write_disp[options['write_disposition']]

    if 'field_delimiter' in options:
        job.field_delimiter = options['field_delimiter']

    if 'skip_leading_rows' in options:
        job.skip_leading_rows = int(options["skip_leading_rows"])


class BqGcsTableLoadResource(BqTableBasedResource):
    # LoadTableFromStorageJob
    def __init__(self, table: Table,
                 bqClient: Client,
                 gcsClient: storage.Client,
                 job: LoadTableFromStorageJob,
                 uris: tuple,
                 schema: tuple,
                 options: dict):
        super(BqGcsTableLoadResource, self)\
            .__init__(table, bqClient)
        self.job = job
        self.gcsClient = gcsClient,
        self.uris = uris
        self.schema = schema
        self.options = options

    def isRunning(self):
        return isJobRunning(self.job)

    def create(self):
        jobid = "-".join(["create", self.table.dataset_name,
                          self.table.name, str(uuid.uuid4())])
        self.job = LoadTableFromStorageJob(jobid, self.table,
                                           self.uris,
                                           self.bqClient,
                                           self.schema)
        self.job.source_format = DestinationFormat.NEWLINE_DELIMITED_JSON
        self.job.ignore_unknown_values = True
        self.job.max_bad_records = 1000
        self.job.write_disposition = WriteDisposition.WRITE_TRUNCATE
        processLoadTableOptions(self.options, self.job)
        self.job.begin()

    def dependsOn(self, other: Resource):
        if self == other:
            return False

        if not isinstance(other, BqExtractTableResource):
            return False
        # TODO: this is pretty janky but works (mostly) for now
        me = set([self.uris[i] for i in range(len(self.uris))])
        them = set(other.uris.split(","))
        return len(me.intersection(them)) > 0

    # we'll come back to this - Doug
    # def updateTime(self):
    #     objs = []
    #     for uri in self.uris:
    #         bucket, prefix = parseBucketAndPrefix(uri)
    #         staridx = prefix.index(prefix, "*")
    #         if staridx != -1:
    #             prefix = prefix[:staridx]
    #
    #         files = [int(o.updated.timestamp() * 1000) for o in
    #                 self.gcsClient.bucket(bucket).list_blobs(
    #                 prefix=prefix)]
    #         objs.append(files)
    #
    #
    #     if not len(objs):
    #         self.table.reload()
    #         createdTime = self.table.modified
    #         return int(createdTime.strftime("%s")) * 1000
    #
    #     return max(objs)

    def shouldUpdate(self):
        return False

    def key(self):
        return ".".join([self.table.dataset_name,
                         self.table.name])

    def __eq__(self, other):
        try:
            return self.key() == other.key() and self.uris == other.uris
        except Exception:
            return False


class BqQueryBasedResource(BqTableBasedResource):
    """ Base class of query based big query actions """
    def __init__(self, queries: list, table: Table,
                 bqClient: Client):
        self.queries = queries
        self.table = table
        self.bqClient = bqClient

        if not isinstance(self.queries, list):
            raise Exception("queries must be of type list")

    def __eq__(self, other):
        try:
            return other.key() == self.key() \
                   and self.makeFinalQuery() == other.makeFinalQuery()
        except Exception:
            return False

    def makeQueryHashTag(self):
        finalQuery = self.makeFinalQuery()
        md5hash = "queryhash:" + hashlib.md5(
            finalQuery.encode("utf-8")).hexdigest()
        return md5hash

    def updateTime(self):
        """ time in milliseconds.  None if not created """
        self.table.reload()
        createdTime = self.table.modified

        if createdTime:
            # getting even more debt ridden
            finalQuery = self.makeFinalQuery()
            # hijack this step to update description - ugh - debt supreme
            if not self.table.description:

                msg = ["This table/view was created with the " +
                       "following query", "", "/**", finalQuery, "*/",
                       "Edits to this description will not be saved",
                       "Do not edit", "",
                       self.makeQueryHashTag()]
                self.table.description = "\n".join(msg)
                self.table.update()
            return int(createdTime.strftime("%s")) * 1000
        return None

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

        filtered = getFiltered(self.makeFinalQuery())
        if strictSubstring("".join(["", other.key(), " "]), filtered):
            return True

            # we need a better way!
            # other may be simply a dataset in which case it will have not
            # .query field
        if isinstance(other, BqDatasetBackedResource) \
                and strictSubstring(other.key(), self.key()):
            return True
        return False

    def addQuery(self, query):
        s = set(self.queries)
        if query not in s:
            self.queries.append(query)

    def makeFinalQuery(self):
        return "\nunion all\n".join(self.queries)

    def shouldUpdate(self):
        self.updateTime()

        if not self.makeQueryHashTag() in self.table.description:
            print("updating because query hash is not in the description")
            return True

        return False


class BqViewBackedTableResource(BqQueryBasedResource):
    def create(self):
        try:
            if (self.table.exists()):
                self.table.delete()
                self.table = Table(self.table.name,
                                   self.bqClient.dataset(
                                       self.table.dataset_name,
                                       self.table.project))

            self.table.view_query = self.makeFinalQuery()
            self.table.schema = []
            self.table.create()
        except NotFound:
            # fail loading - exists will fail
            # and we'll retry and after
            # a few times app will exit
            pass

    def isRunning(self):
        return False

    def dump(self):
        return self.makeFinalQuery()


def getFiltered(query):
    return re.sub('[^0-9a-zA-Z._]+', ' ', query)


def strictSubstring(contained, container):
    """
    :rtype: bool
    """
    return contained in container and len(contained) < len(container)


class BqQueryBackedTableResource(BqQueryBasedResource):
    def __init__(self, queries: list, table: Table,
                 bqClient: Client, queryJob: QueryJob):
        super(BqQueryBackedTableResource, self)\
            .__init__(queries, table, None, bqClient)
        self.queryJob = queryJob

    def create(self):
        if self.table.exists():
            self.table.delete()

        jobid = "-".join(["create", self.table.dataset_name,
                          self.table.name, str(uuid.uuid4())])
        query_job = self.bqClient.run_async_query(jobid, self.makeFinalQuery())

        # TODO: this should probably all be options
        query_job.allow_large_results = True
        query_job.flatten_results = False
        query_job.destination = self.table
        query_job.priority = QueryPriority.INTERACTIVE
        query_job.write_disposition = WriteDisposition.WRITE_TRUNCATE
        query_job.maximum_billing_tier = 2
        query_job.begin()
        self.queryJob = query_job

    def isRunning(self):
        if self.queryJob:
            self.queryJob.reload()
            print(self.queryJob.name,
                  self.queryJob.state, self.queryJob.errors)
            return self.queryJob.state in ['RUNNING', 'PENDING']
        else:
            return False

    def dump(self):
        return self.makeFinalQuery()


class BqQueryBackedTableResource(BqQueryBasedResource):
    def __init__(self, query: str, table: Table,
                 bqClient: Client, queryJob: QueryJob):
        super(BqQueryBackedTableResource, self)\
            .__init__(query, table, bqClient)
        self.queryJob = queryJob

    def create(self):
        if self.table.exists():
            self.table.delete()

        jobid = "-".join(["create", self.table.dataset_name,
                          self.table.name, str(uuid.uuid4())])
        query_job = self.bqClient.run_async_query(jobid, self.makeFinalQuery())
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

    def dump(self):
        return self.makeFinalQuery()


def processExtractTableOptions(options: dict,
                               job: ExtractTableToStorageJob):
    compressions = {
        "GZIP": Compression.GZIP,
        "NONE": Compression.NONE,
    }

    if "compression" in options:
        value = options["compression"]
        if value not in compressions:
            raise KeyError("Please specify only one of " +
                           compressions.keys())
        job.compression = compressions[value]

    formats = {
        "NEWLINE_DELIMITED_JSON": DestinationFormat.NEWLINE_DELIMITED_JSON,
        "CSV": DestinationFormat.CSV,
        "AVRO": DestinationFormat.AVRO
    }

    if "destination_format" in options:
        value = options['destination_format']
        if value not in formats:
            raise KeyError("Please specify only one of " + formats.keys())
        job.destination_format = formats[value]

    if "field_delimiter" in options:
        job.field_delimiter = options['field_delimiter']

    if "print_header" in options:
        job.field_delimiter = bool(options['print_header'])


class BqExtractTableResource(Resource):
    def __init__(self,
                 table: Table,
                 bqClient: Client,
                 gcsClient: storage.Client,
                 extractJob: ExtractTableToStorageJob,
                 uris: str,
                 options: dict):

        self.extractJob = extractJob
        self.table = table
        self.bqClient = bqClient
        self.gcsClient = gcsClient
        self.uris = uris
        # check uris
        (self.bucket, self.pathPrefix) = self.parseBucketAndPrefix(uris)
        self.options = options

    def create(self):
        jobid = "-".join(["extract", self.table.name,
                          self.table.name, str(uuid.uuid4())])
        self.extractJob = self.bqClient.extract_table_to_storage(jobid,
                                                                 self.table,
                                                                 self.uris)
        processExtractTableOptions(self.options, self.extractJob)
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
        return gcsExists(self.gcsClient, self.uris)

    def dependsOn(self, other: Resource):
        return "extract." + other.key() == self.key()

    def dump(self):
        return ",".join(self.uris)

    def parseBucketAndPrefix(self, uris):
        bucket = uris.replace("gs://", "").split("/")[0]
        prefix = "/".join(uris.replace("gs://", "").split("/")[:-2])
        return (bucket, prefix)

    def updateTime(self):
        objs = [int(o.updated.timestamp() * 1000) for o in
                gcsUris(self.gcsClient, self.uris)]

        if len(objs) == 0:
            # basically i've never be extracted
            print("returning 0")
            return 0

        return max(objs)

    def shouldUpdate(self):
        self.table.reload()
        createdTime = self.table.modified
        if not createdTime:
            return False

        return self.updateTime() < int(createdTime.strftime("%s")) * 1000


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


def parseBucketAndPrefix(uris):
    bucket = uris.replace("gs://", "").split("/")[0]
    prefix = "/".join(uris.replace("gs://", "").split("/")[1:])
    return (bucket, prefix)


def gcsExists(gcsClient, uris):
    return len(gcsUris(gcsClient, uris)) > 0


def gcsUris(gcsClient, uris):
    (bucket, prefix) = parseBucketAndPrefix(uris)
    prefix = prefix.replace("*.gz", "")
    bucket = gcsClient.get_bucket(bucket)
    objs = [x for x in bucket.list_blobs(prefix=prefix,
                                         delimiter="/")]

    return objs
