import hashlib
import json
import logging
import re
import subprocess
import uuid
from datetime import datetime, timedelta
from json.decoder import JSONDecodeError

from google.cloud import bigquery
from google.cloud import storage
from google.cloud.bigquery import ExternalConfig
from google.cloud.bigquery.client import Client
from google.cloud.bigquery.dataset import Dataset
from google.cloud.bigquery.job import WriteDisposition, \
    QueryPriority, QueryJob, SourceFormat, \
    Compression, DestinationFormat, _AsyncJob, LoadJob, ExtractJob
from google.cloud.bigquery.table import Table, TableReference
from google.cloud.exceptions import NotFound

# max length of description allowed by biquery
# https://cloud.google.com/bigquery/quotas - found this by updating
# a single table description.
# We take 150 off the max
MAX_DESCRIPTION_LEN = 16384


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


def _buildFullyQualifiedTableName_(table: Table) -> str:
    return "{}.{}.{}".format(table.project, table.dataset_id, table.table_id)


def _buildDataSetKey_(table: Table) -> str:
    """
    :param table: a bq table
    :return: colon concatenated project, dataset
    """
    return ":".join([table.dataset_id])


def _buildDataSetTableKey_(table: TableReference) -> str:
    """
    :param table:
    :return: colon concatenated project,  dataset, tablename
    """
    return ":".join([_buildDataSetKey_(table), table.table_id])


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
            if table.table_id in dsetMap:
                return dsetMap[table.table_id]
            else:
                return None
        else:  # load dataset
            iter = self.bqClient.dataset(table.dataset_id).list_tables()
            dsetMap = {}
            self.datasetTableMap[key] = dsetMap
            while True:
                for t in iter:
                    dsetMap[t.name] = t
                if not iter.next_page_token:
                    break
            if table.friendly_name in dsetMap:
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
        self.bqClient = bqClient
        self.existFlag = False
        self.dataset = None
        try:
            self.dataset = bqClient.get_dataset(dataset)
            self.existFlag = True
        except NotFound:
            pass

    def exists(self):
        return self.dataset is not None

    def updateTime(self):
        """ time in milliseconds.  None if not created """
        createdTime = self.dataset.modified
        if createdTime:
            # replaced %s with %S to avoid "invalid format"
            # calling createdTime.strftime on windows
            # return int(createdTime.strftime("%S")) * 1000
            return int(createdTime.strftime("%s")) * 1000

        return None

    def create(self):
        self.dataset = self.bqClient.create_dataset(self.datasetReference)

    def key(self):
        return self.dataset.dataset_id

    def dependsOn(self, resource):
        return False

    def isRunning(self):
        return False

    def shouldUpdate(self):
        return False

    def __str__(self):
        return ":".join([self.dataset.project, self.dataset.dataset_id])

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
        try:
            self.bqClient.get_table(self.table)
            return True
        except NotFound:
            return False

    def updateTime(self):
        """ time in milliseconds.  None if not created """
        self.table = self.bqClient.get_table(self.table)
        createdTime = self.table.modified

        if createdTime:
            return int(createdTime.strftime("%s")) * 1000
        return None

    def create(self):
        raise Exception("implement")

    def key(self):
        return ".".join([self.table.dataset_id,
                         self.table.table_id])

    def dependsOn(self, other: Resource):
        raise Exception("implement this function")

    def isRunning(self):
        raise Exception("implement this function")

    def __str__(self):
        return ".".join([self.table.dataset_id,
                         self.table.table_id, "${query}"])


def generate_file_md5(filename, blocksize=2**20):
    m = hashlib.md5()
    with open(filename, "rb") as f:
        while True:
            buf = f.read(blocksize)
            if not buf:
                break
            m.update(buf)
    return m.hexdigest()


class BqProcessTableResource(BqTableBasedResource):
    """ todo: currently we block during the creation of this
    table but we should probably treat this just like any table
    create and put it in the background
    """
    def __init__(self, query: str, table: Table,
                 schema: tuple, bqClient: Client,
                 job: _AsyncJob):
        """ """
        super(BqProcessTableResource, self).__init__(table, bqClient)
        self.query = query
        self.table = table
        self.bqClient = bqClient
        self.schema = schema
        self.job = job

    def exists(self):
        try:
            self.bqClient.get_table(self.table)
            return True
        except NotFound:
            return False

    def dependsOn(self, other: Resource):
        return self.legacyBqQueryDependsOn(other)

    def legacyBqQueryDependsOn(self, other: Resource):
        if self == other:
            return False

        filtered = getFiltered(self.query)
        if strictSubstring("".join(["", other.key(), " "]), filtered):
            return True

            # we need a better way!
            # other may be simply a dataset in which case it will have not
            # .query field
        if isinstance(other, BqDatasetBackedResource) \
                and strictSubstring(other.key(), self.key()):
            return True
        return False

    def makeHashTag(self):

        m = hashlib.md5()
        m.update(self.query.encode("utf-8"))
        return m.hexdigest()

        return "filehash:" + generate_file_md5(self.file) + ":" + schemahash

    def updateTime(self):
        """ time in milliseconds.  None if not created """
        # self.table.reload() # reload was pre-sdk update
        self.table = self.bqClient.get_table(self.table)

        print("created time is ", str(self.table.modified))
        createdTime = self.table.modified
        hashtag = self.makeHashTag()

        if createdTime:

            print("description is ", self.table.description)
            # hijack this step to update description - ugh - debt supreme
            if not self.table.description:
                self.table.description = "\n".join(["Do not edit", hashtag])
                self.bqClient.update_table(self.table, ["description"])
            return int(createdTime.strftime("%s")) * 1000
        return None

    def create(self):
        self.table.schema = self.schema

        if self.exists():
            print("Table exists and we're wiping out the description")
            self.table.description = ""
            self.bqClient.update_table(self.table, ["description"])

        # we exec
        import os

        # pump the script into a file
        # script name
        script = "/tmp/" + _buildDataSetTableKey_(table=self.table)
        with open(script, 'wb') as of:
            of.write(bytearray(self.query, 'utf-8'))

        os.chmod(script, 0o744)

        datascript = script + ".data"
        with open(datascript, 'wb') as writable:
            with open(datascript + ".error", 'w') as errors:
                try:
                    fHandle = subprocess.Popen(script, stdout=writable,
                                               stderr=errors)
                except OSError as ose:
                    logging.error(ose)
                    return None

                fHandle.wait()

        if fHandle.returncode != 0:
            err = open(datascript + ".error").read()
            print("exit status != 0, got " + str(fHandle.returncode)
                  + "error:" + err)
            return None

        # todo - allow caller to specify file delimiter
        fieldDelimiter = '\t'
        with open(datascript, 'r') as readable:
            srcFormat = BqDataLoadTableResource.detectSourceFormat(
                                                readable.readline())
            if srcFormat != SourceFormat.CSV:
                fieldDelimiter = None

        job_config = bigquery.LoadJobConfig(
            source_format=srcFormat,
            field_delimiter=fieldDelimiter, ignore_unknown_values=True,
            write_disposition=WriteDisposition.WRITE_TRUNCATE,
            schema=self.schema)

        with open(datascript, "rb") as source_file:
            self.job \
                = self.bqClient.load_table_from_file(source_file,
                                                     self.table,
                                                     job_config=job_config)

    def key(self):
        return ".".join([self.table.dataset_id, self.table.table_id])

    def isRunning(self):
        return isJobRunning(self.job)

    def __str__(self):
        return "localdata:" + ".".join([self.table.dataset_id,
                                        self.table.table_id])

    def detectSourceFormat(firstFileLine: str):
        try:
            json.loads(firstFileLine)
            return SourceFormat.NEWLINE_DELIMITED_JSON
        except JSONDecodeError:
            return SourceFormat.CSV

    def __eq__(self, other):
        try:
            return self.query == other.query and self.key() == other.key()
        except Exception:
            return False

    def shouldUpdate(self):
        self.updateTime()
        if not self.makeHashTag() in self.table.description:
            return True
        return False

    def dump(self):
        return self.query


class BqDataLoadTableResource(BqTableBasedResource):
    """
        script for loading local data
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
        try:
            self.bqClient.get_table(self.table)
            return True
        except NotFound:
            return False

    def makeHashTag(self):
        schemahash = generate_file_md5(self.file + ".schema")
        return "filehash:" + generate_file_md5(self.file) + ":" + schemahash

    def updateTime(self):
        """ time in milliseconds.  None if not created """
        self.table = self.bqClient.get_table(self.table)
        createdTime = self.table.modified

        hashtag = self.makeHashTag()

        if createdTime:
            # hijack this step to update description - ugh - debt supreme
            if not self.table.description:
                self.table.description = "\n".join(["Do not edit", hashtag])
                self.bqClient.update_table(self.table, ["description"])
            return int(createdTime.strftime("%s")) * 1000
        return None

    def create(self):
        self.table.schema = self.schema

        if self.exists():
            self.table.description = ""
            self.bqClient.update_table(self.table, ["description"])

        fieldDelimiter = '\t'
        with open(self.file, 'r') as readable:
            srcFormat = BqDataLoadTableResource.detectSourceFormat(
                                                readable.readline())

            if srcFormat != SourceFormat.CSV:
                fieldDelimiter = None

        job_config = bigquery.LoadJobConfig()
        job_config.source_format = srcFormat
        job_config.schema = self.table.schema
        if srcFormat == SourceFormat.CSV:
            job_config.skip_leading_rows = 1
        job_config.autodetect = True
        job_config.write_disposition = WriteDisposition.WRITE_TRUNCATE

        with open(self.file, 'rb') as readable:
            job = self.bqClient.load_table_from_file(
                readable,
                self.table,
                job_config=job_config
                )
        self.job = job

    def key(self):
        return ".".join([self.table.dataset_id, self.table.table_id])

    def dependsOn(self, resource: Resource):
        return self.table.dataset_id == resource.key()

    def isRunning(self):
        return isJobRunning(self.job)

    def __str__(self):
        return "localdata:" + ".".join([self.table.dataset_id,
                                        self.table.table_id])

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


def processLoadTableOptions(options: dict):
    """
    :param options: A dictionary of options matching fields available
     on LoadTableFromStorageJob
    :param job: An instance of LoadTableFromStorageJob
    :return: None - simply decorates the job
    """
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = DestinationFormat.NEWLINE_DELIMITED_JSON
    job_config.ignore_unknown_values = True
    job_config.write_disposition = WriteDisposition.WRITE_TRUNCATE

    if "schema" in options:
        job_config.schema = options["schema"]

    formats = {
        "AVRO": SourceFormat.AVRO,
        "NEWLINE_DELIMITED_JSON": SourceFormat.NEWLINE_DELIMITED_JSON,
        "CSV": SourceFormat.CSV,
        "DATASTORE_BACKUP": SourceFormat.DATASTORE_BACKUP,
        "PARQUET": bigquery.SourceFormat.PARQUET,
        "ORC": bigquery.SourceFormat.ORC
    }

    if "source_format" in options:
        value = options['source_format']
        if value not in formats:
            raise KeyError("Please use only one of the following: "
                           + ",".join(formats.keys()))
        if value != "PARQUET":
            job_config.max_bad_records = 1000
        job_config.source_format = formats[value]

    if "max_bad_records" in options:
        job_config.max_bad_records = int(options['max_bad_records'])

    if 'ignore_unknown_values' in options:
        job_config.ignore_unknown_values = \
                bool(options['ignore_unknown_values'])

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
        job_config.write_disposition = write_disp[options['write_disposition']]

    if 'field_delimiter' in options:
        job_config.field_delimiter = options['field_delimiter']

    if 'skip_leading_rows' in options:
        job_config.skip_leading_rows = int(options["skip_leading_rows"])

    if 'allow_quoted_newlines' in options:
        job_config.allow_quoted_newlines = options["allow_quoted_newlines"]

    if 'encoding' in options:
        job_config.encoding = options["encoding"]

    if 'quote_character' in options:
        job_config.quote_character = options["quote_character"]

    if 'null_marker' in options:
        job_config.null_marker = options["null_marker"]

    if 'destination_table_description' in options:
        job_config.destination_table_description = \
            options["destination_table_description"]

    return job_config


class BqGcsTableLoadResource(BqTableBasedResource):
    # LoadTableFromStorageJob
    def __init__(self, table: Table,
                 bqClient: Client,
                 gcsClient: storage.Client,
                 job: LoadJob,
                 query: str,
                 schema: tuple,
                 options: dict):
        super(BqGcsTableLoadResource, self).__init__(table, bqClient)
        self.job = job
        self.gcsClient = gcsClient
        self.query = query
        self.schema = schema
        self.options = options
        self.uris = tuple([uri for uri in self.query.split("\n") if
                          uri.startswith("gs://")])
        self.expiration = None
        self.require_exists = None

        if "require_exists" in self.options:
            self.require_exists = self.options['require_exists']

        if "expiration" in self.options:
            try:
                self.expiration = int(self.options["expiration"])
            except Exception:
                raise Exception("expiration must be an integer: load: ",
                                self.table.table_id)

    def isRunning(self):
        return isJobRunning(self.job)

    def dump(self):
        return str(self.uris)

    def create(self):
        if self.require_exists is not None and \
                not gcsBlobExists(self.gcsClient, self.require_exists):
            print(
                self.require_exists +
                " required file does not exist. Unable to load: ",
                self.key()
            )
            return

        jobid = "-".join(
            [
                "create",
                self.table.dataset_id,
                self.table.table_id,
                str(uuid.uuid4())
            ]
        )
        self.job = self.bqClient.load_table_from_uri(
            self.uris,
            self.table,
            jobid,
            job_config=processLoadTableOptions(self.options)
            )

    def exists(self):
        try:
            self.table = self.bqClient.get_table(self.table)
            # update expiration if not set
            if self.expiration is not None and self.table.expires is None:
                self.table.expires = datetime.now() + timedelta(
                    days=self.expiration)
                self.bqClient.update_table(self.table, ['expires'])

            return True
        except NotFound:
            return False

    def dependsOn(self, other: Resource):
        if self == other:
            return False

        if isinstance(other, BqDatasetBackedResource) \
                and strictSubstring(other.key(), self.key()):
            return True

        if not isinstance(other, BqExtractTableResource):
            depends = self.legacyBqQueryDependsOn(other)
            if depends:
                return True

        if isinstance(other, BqExtractTableResource):
            # TODO: this is pretty janky but works (mostly) for now
            me = set([self.uris[i] for i in range(len(self.uris))])
            them = set(other.uris.split(","))
            return len(me.intersection(them)) > 0

        return False

    def shouldUpdate(self):
        return False

    def key(self):
        return ".".join([self.table.dataset_id,
                         self.table.table_id])

    def __eq__(self, other):
        try:
            return self.key() == other.key() and self.uris == other.uris
        except Exception:
            return False

    def legacyBqQueryDependsOn(self, other: Resource):
        if self == other:
            return False

        gcsremoved = re.sub('^gs:.*$', "\n", self.query)
        filtered = getFiltered(gcsremoved)

        if strictSubstring("".join(["", other.key(), " "]), filtered):
            return True

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
        self.table = self.bqClient.get_table(self.table)

        createdTime = self.table.modified

        if createdTime:
            # getting even more debt ridden
            final_query = self.makeFinalQuery()
            # hijack this step to update description
            if not self.table.description:
                # we use a create time + a missing description
                # as a queue to update description with the state
                # necessary to know if we should update / re-run next
                # time.
                padding = 300
                truncated = len(final_query) > MAX_DESCRIPTION_LEN - padding \
                    and "(truncated due to size)" or ""

                msg = [f"This table/view was created with "
                       f"the following {truncated} query", "/**",
                       f"{final_query[:MAX_DESCRIPTION_LEN-padding]}",
                       "*/",
                       "Edits to this description will not be saved",
                       "Do not edit", "",
                       self.makeQueryHashTag()]

                self.table.description = "\n".join(msg)
                self.table = self.bqClient.update_table(
                        self.table,
                        ["description"]
                        )
            return int(createdTime.strftime("%s")) * 1000
        return None

    def create(self):
        raise Exception("implement")

    def key(self):
        return ".".join([self.table.dataset_id,
                         self.table.table_id])

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

    def tableExists(self):
        try:
            self.bqClient.get_table(self.table)
            return True
        except NotFound:
            return False

    def create(self):
        try:
            table_id = _buildFullyQualifiedTableName_(self.table)

            if (self.tableExists()):
                self.bqClient.delete_table(table_id, not_found_ok=True)

            self.table = Table(table_id)
            self.table.view_query = self.makeFinalQuery()
            self.table.schema = None
            self.table = self.bqClient.create_table(self.table)
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
    def __init__(self, query: str, table: Table,
                 bqClient: Client, queryJob: QueryJob, expiration: None):
        super(BqQueryBackedTableResource, self)\
            .__init__(query, table, bqClient)
        self.queryJob = queryJob
        self.expiration = expiration

    def tableExists(self):
        try:
            self.bqClient.get_table(self.table)
            return True
        except NotFound:
            return False

    def create(self):
        if self.tableExists():
            table_id = _buildFullyQualifiedTableName_(self.table)
            self.bqClient.delete_table(table_id, not_found_ok=True)
        jobid = "-".join(["create", self.table.dataset_id,
                          self.table.table_id, str(uuid.uuid4())])
        use_legacy_sql = "#standardsql" not in self.makeFinalQuery().lower()
        job_config = bigquery.QueryJobConfig()
        job_config.allow_large_results = True
        job_config.flatten_results = False
        job_config.use_legacy_sql = use_legacy_sql
        job_config.destination = self.table
        job_config.priority = QueryPriority.INTERACTIVE
        job_config.write_disposition = WriteDisposition.WRITE_TRUNCATE
        job_config.maximum_billing_tier = 2

        self.queryJob = self.bqClient.query(
            self.makeFinalQuery(),
            job_config=job_config,
            job_id=jobid
        )

        if self.expiration is not None:
            def done_callback(future):
                table_path = ".".join([self.table.project,
                                      self.table.dataset_id,
                                      self.table.table_id])
                table = self.bqClient.get_table(table_path)
                table.expires = datetime.now() + timedelta(
                                      days=self.expiration)
                self.bqClient.update_table(table, ['expires'])

            self.queryJob.add_done_callback(done_callback)

    def key(self):
        return ".".join([self.table.dataset_id, self.table.table_id])

    def isRunning(self):
        return isJobRunning(self.queryJob)

    def dump(self):
        return self.makeFinalQuery()


def processExtractTableOptions(options: dict):
    compressions = {
        "GZIP": Compression.GZIP,
        "NONE": Compression.NONE,
        "SNAPPY": Compression.SNAPPY,
        "DEFLATE": Compression.DEFLATE
    }

    job_config = bigquery.job.ExtractJobConfig()

    if "compression" in options:
        value = options["compression"]
        if value not in compressions:
            raise KeyError("Please specify only one of " +
                           compressions.keys())
        job_config.compression = compressions[value]

    formats = {
        "NEWLINE_DELIMITED_JSON": DestinationFormat.NEWLINE_DELIMITED_JSON,
        "CSV": DestinationFormat.CSV,
        "AVRO": DestinationFormat.AVRO,
        "PARQUET": "PARQUET"
    }

    if "destination_format" in options:
        value = options['destination_format']
        if value not in formats:
            raise KeyError("Please specify only one of " + formats.keys())
        job_config.destination_format = formats[value]

    if "field_delimiter" in options:
        job_config.field_delimiter = options['field_delimiter']

    if "print_header" in options:
        if type(options['print_header']) == 'str':
            raise Exception("print_header value must be a json boolean")
        job_config.print_header = bool(options['print_header'])

    return job_config


class BqExtractTableResource(Resource):
    def __init__(self,
                 table: Table,
                 bqClient: Client,
                 gcsClient: storage.Client,
                 extractJob: ExtractJob,
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
        jobid = "-".join(["extract", self.table.table_id,
                          self.table.table_id, str(uuid.uuid4())])
        self.extractJob = self.bqClient.extract_table(
                self.table,
                self.uris,
                jobid,
                job_config=processExtractTableOptions(self.options)
                )

    def key(self):
        return ".".join(["extract", self.table.dataset_id,
                         self.table.table_id])

    def isRunning(self):
        return isJobRunning(self.extractJob)

    def __str__(self):
        return "extract:" + ".".join([self.table.dataset_id,
                                     self.table.table_id])

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
            # basically i've never been extracted
            return 0

        return max(objs)

    def shouldUpdate(self):
        self.table = self.bqClient.get_table(self.table)
        createdTime = self.table.modified
        if not createdTime:
            return False

        return self.updateTime() < int(createdTime.strftime("%s")) * 1000


def export_data_to_gcs(dataset_name, table_name, destination):
    bigquery_client = Client()
    dataset = bigquery_client.dataset(dataset_name)
    table = dataset.table(table_name)
    job_name = str(uuid.uuid4())

    job = bigquery_client.extract_table_to_storage(
        job_name, table, destination)

    job.result()  # Wait for job to complete

    print('Exported {}:{} to {}'.format(
        dataset_name, table_name, destination))


def isJobRunning(job):
    if not job:
        return False

    job.reload()
    print(job.job_id, job.state, job.errors)
    return job.running()


def parseBucketAndPrefix(uris):
    bucket = uris.replace("gs://", "").split("/")[0]
    prefix = "/".join(uris.replace("gs://", "").split("/")[1:])
    return (bucket, prefix)


def gcsBlobExists(gcsclient, gcsUri):
    bucket_name, blob_path = parseBucketAndBlobPath(gcsUri)
    bucket = gcsclient.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    return blob.exists()


def parseBucketAndBlobPath(uri):
    bucket_name = uri.split("/")[2]
    blob_path = "/".join(uri.split("/")[3:])
    return (bucket_name, blob_path)


def gcsExists(gcsClient, uris):
    return len(gcsUris(gcsClient, uris)) > 0


def gcsUris(gcsClient, uris):
    (bucket, prefix) = parseBucketAndPrefix(uris)
    parts = prefix.split('*')
    if len(parts) > 2:
        raise Exception(f"The extract url must only contain " +
                        "a single * char and provide file " +
                        "suffix info: {str(uris)}")

    args = {'prefix': parts[0], 'delimiter': '/'}

    bucket = gcsClient.get_bucket(bucket)
    objs = [x for x in bucket.list_blobs(**args)
            if len(parts) == 1 or x.name.endswith(parts[1])]

    return objs


# deliberately class level
def legacyBqQueryDependsOn(self, other: Resource):
    if self == other:
        return False

    if 'query' in dir(self):
        filtered = getFiltered(self.query)
        if strictSubstring("".join(["", other.key(), " "]), filtered):
            return True

        # we need a better way!
        # other may be simply a dataset in which case it will have not
        # .query field
    if isinstance(other, BqDatasetBackedResource) \
            and strictSubstring(other.key(), self.key()):
        return True
    return False


# base resource class for all table back resources
class BqExternalTableBasedResource(BqTableBasedResource):
    """ Base class of query based big query actions """
    def __init__(self, bqclient: Client, table: Table,
                 external_config: ExternalConfig):
        self.table = table
        self.bqClient = bqclient
        self.external_config = external_config
        self.table.external_data_configuration = external_config

        # assert if autodetect that there's no schema
        obj = external_config.to_api_repr()
        autodetect = obj.get("autodetect", None)
        if autodetect and table.schema:
            raise Exception("if autodetect is true, then you " +
                            "must not specify a schema")
        if not autodetect and not table.schema:
            raise Exception("you must not specify a schema in a .schema file")

    def exists(self):
        try:
            self.bqClient.get_table(self.table)
            return True
        except NotFound:
            return False

    def create(self):
        self.bqClient.delete_table(self.table, not_found_ok=True)
        self.table = self.bqClient.create_table(self.table)
        self.table.description = self.make_description()
        # update description - for some reason this can't be done
        # on create???
        self.bqClient.update_table(self.table, ["description"])

    def key(self):
        return ".".join([self.table.dataset_id,
                         self.table.table_id])

    def dependsOn(self, resource: Resource):
        if self.table.dataset_id == resource.key():
            return True
        return legacyBqQueryDependsOn(self, resource)

    def isRunning(self):
        # this is not an async operation
        return False

    def shouldUpdate(self):
        current_description = self.bqClient.get_table(self.table).description
        if not current_description:
            return True
        if not self.makeHashTag() in current_description:
            return True
        return False

    def makeHashTag(self):
        m = hashlib.md5()
        s = json.dumps(self.external_config.to_api_repr(),
                       sort_keys=True).encode()
        m.update(s)
        return m.hexdigest()

    def __eq__(self, other):
        return self.key() == other.key()

    def make_description(self):
        ret = f"""
The following config was used to create this external table.

{json.dumps(self.external_config.to_api_repr(), sort_keys=True, indent=2)}

Do not edit this confighash: {self.makeHashTag()}
        """
        return ret

    def __str__(self):
        return ".".join([self.table.dataset_id,
                         self.table.table_id]) + "-external-table"
