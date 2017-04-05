import json
from json.decoder import JSONDecodeError

from google.cloud.bigquery.client import Client
from google.cloud.bigquery.schema import SchemaField
from google.cloud.bigquery.table import Table
from os.path import getmtime
from enum import Enum

import tmplhelper
from resource import Resource, _buildDataSetKey_, BqDatasetBackedResource, \
    BqJobs, BqQueryBackedTableResource, _buildDataSetTableKey_, \
    BqViewBackedTableResource, BqDataLoadTableResource
from tmplhelper import evalTmplRecurse, explodeTemplate


class FileLoader:
    def __init__(self):
        pass

    def load(self, file) -> Resource:
        """ The resource loader will attempt to load the resources
        which it handles from the file arg """
        pass

    def handles(self, file) -> bool:
        """ Given a file, the resource should answer true or false
        whether or not this loader can handle loading that file
        """
        pass


class DelegatingFileSuffixLoader(FileLoader):
    """ Manages a map of loader keyed by file suffix """

    def __init__(self, **kwargs):
        if not len(kwargs):
            raise ValueError("Please specify one or more FileLoader")
        for key in kwargs:
            if not issubclass(kwargs[key].__class__, FileLoader):
                raise ValueError("args must be subclass of FileLoader")
            self.loaders = kwargs

    def load(self, file):
        suffixParts = file.split("/")[-1].split(".")
        if len(suffixParts) == 1:
            raise ValueError(file +
                             " must have suffix and be from one of " +
                             str(self.loaders.keys()) + " to be processed"
                             )
        try:
            return self.loaders[suffixParts[-1]].load(file)
        except KeyError:
            raise ValueError("No loader associated with suffix: " +
                             suffixParts[-1])

    def handles(self, file):
        return self.suffix(file) in self.loaders.keys()

    def suffix(self, file):
        try:
            return file.split("/")[-1].split(".")[-1]
        except:
            raise ValueError(
                "Invalid file for loading: " + file + ". No suffix")


def parseDatasetTable(filePath, defaultDataset: str, bqClient: Client) \
        -> Table:
    tokens = filePath.split("/")[-1].split(".")
    if len(tokens) == 3:
        return bqClient.dataset(tokens[0]).table(tokens[1])
    elif len(tokens) == 2:  # use default dataset
        if not defaultDataset:
            raise ValueError("Must specify a default dataset")
        return bqClient.dataset(defaultDataset).table(tokens[0])
    elif len(tokens) > 3:
        raise ValueError("Invalid filename: " + filePath +
                         ". File names "
                         "must be of form "
                         "dataset.table.suffix or table.suffix")


def parseDataset(filePath):
    """ Takes a file path and parses to dataset string"""
    tokens = filePath.split("/")[-1].split(".")
    if len(tokens) == 2:
        return tokens[0]
    else:
        raise ValueError("Invalid filename: " + filePath +
                         ". File names for datasets "
                         "must be of form "
                         "dataset.suffix")


def cacheDataSet(bqClient: Client, bqTable: Table, datasets: dict):
    """

    :param bqClient: The client to big query
    :param bqTable: The table whose dataset dependency will be generated
    :param datasets: a place where the dataset will be stuffed
    :return: BqDatasetBackedResource instance, either new or from cache
    """
    dsetKey = _buildDataSetKey_(bqTable)
    if dsetKey not in datasets:
        dataset = bqClient.dataset(bqTable.dataset_name)
        datasets[dsetKey] = BqDatasetBackedResource(dataset, 0,
                                                    bqClient)
    return datasets[dsetKey]


class TableType(Enum):
    VIEW = 1
    TABLE = 2


class BqQueryTemplatingFileLoader(FileLoader):
    """
    Query Template loader

    Requires the following
    1. A query file passed to the load method with 0 or more string.format
    simple
    template expressions.
    2. An additional file of the same + .vars, which must be a json array
    of simple key value pairs

    Within each object, there MUST be a 'table' key value.  This maybe a
    string format expression string as well.

    Within each object, there MAY be a 'dataset' key value.  It may also
    be a string.format expression value

    Files passed to the load method MUST have a suffix

    todo: add support for project although this is pretty limiting in
    reality
    """

    def __init__(self, bqClient: Client, bqJobs: BqJobs, tableType:
                 TableType, defaultDataset=None):
        """

        :param bqClient: The big query client to use
        :param bqJobs: An initialized BqJobs
        :param tableType Either TABLE or VIEW
        :param defaultDataset: A default dataset to use in templates
        """
        self.bqClient = bqClient
        self.defaultDataset = defaultDataset
        self.bqJobs = bqJobs
        self.datasets = {}
        self.tableType = tableType
        if not self.tableType or self.tableType not in TableType:
            raise Exception("TableType must be set")

    def explodeTemplateVarsArray(rawTemplates: list,
                                 folder: str,
                                 filename: str,
                                 defaultDataset: str):
        ret = []
        for t in rawTemplates:
            copy = t.copy()
            copy['folder'] = folder
            copy['filename'] = filename
            if 'dataset' not in copy:
                copy['dataset'] = defaultDataset
            if 'table' not in copy:
                copy['table'] = filename

            ret += [evalTmplRecurse(t) for t in explodeTemplate(copy)]

        return ret

    def processTemplateVar(self, templateVars: dict, template: str,
                           filePath: str, mtime: int, out: dict):
        """

        :param templateVars: These are the variables which will be used
        to format the query and generate the table name
        :param template: This is the query which will be templatized
        :param filePath: The local file path where the query exists
        :param mtime: The modification time to give the resource we're
        building
        :param out: a dictionary where any resources which are built
        during method execution will be stored.  Duplicate tables
        generated is considered an error and will raise Exception.
        Datasets are ok.
        :return: void
        """
        if 'dataset' not in templateVars:
            raise Exception("Missing dataset in template vars for " +
                            filePath + ".vars")
        dataset = templateVars['dataset']
        needed = tmplhelper.keysOfTemplate(template)
        if not needed.issubset(templateVars.keys()):
            missing = str(needed - templateVars.keys())
            raise Exception("Please define values for " +
                            missing + " in a file: ",
                            filePath + ".vars")
        query = template.format(**templateVars)
        # print("formatting query: ", query)
        table = templateVars['table']

        bqTable = self.bqClient.dataset(dataset).table(table)
        key = _buildDataSetTableKey_(bqTable)
        if key in out:
            raise Exception("Templating generated duplicate "
                            "tables outputs for " + filePath)

        if self.tableType == TableType.TABLE:
            jT = self.bqJobs.getJobForTable(bqTable)
            arsrc = BqQueryBackedTableResource(query, bqTable,
                                               int(mtime * 1000),
                                               self.bqClient,
                                               queryJob=jT)
            out[key] = arsrc
        elif self.tableType == TableType.VIEW:
            arsrc = BqViewBackedTableResource(query, bqTable,
                                              int(mtime * 1000),
                                              self.bqClient)
            out[key] = arsrc

        dsetKey = _buildDataSetKey_(bqTable)
        if dsetKey not in out:
            out[dsetKey] = cacheDataSet(self.bqClient, bqTable,
                                        self.datasets)

    def load(self, filePath):
        mtime = getmtime(filePath)
        ret = {}
        with open(filePath) as f:
            template = f.read()
            try:
                filename = filePath.split("/")[-1].split(".")[-2]
                folder = filePath.split("/")[-2]
                templateVars = \
                    BqQueryTemplatingFileLoader.explodeTemplateVarsArray(
                        self.loadTemplateVars(
                            filePath + ".vars"), folder, filename,
                        self.defaultDataset)

            except FileNotFoundError:
                raise Exception("Please define template vars in a file "
                                "called " + filePath + ".vars")

            for v in templateVars:
                self.processTemplateVar(v, template, filePath, mtime, ret)
        return ret.values()

    def loadTemplateVars(self, filePath) -> list:
        try:
            with open(filePath) as f:
                templateVarsList = json.loads(f.read())
                if not isinstance(templateVarsList, list):
                    raise Exception(
                        "Must be json list of objects in " + filePath)
                for definition in templateVarsList:
                    if not isinstance(definition, dict):
                        raise Exception(
                            "Must be json list of objects in " + filePath)
                return templateVarsList
        except FileNotFoundError:
            return [{}]
        except JSONDecodeError:
            raise Exception("Problem reading json var list from file: ",
                            filePath)


class BqQueryFileLoader(FileLoader):
    def __init__(self, bqClient: Client, bqJobs: BqJobs,
                 defaultDataset=None):
        self.bqClient = bqClient
        self.defaultDataset = defaultDataset
        self.bqJobs = bqJobs
        self.datasets = {}

    def load(self, filePath):
        mtime = getmtime(filePath)
        bqTable = parseDatasetTable(filePath, self.defaultDataset,
                                    self.bqClient)
        ret = []
        with open(filePath) as f:
            query = f.read().format(dataset=self.defaultDataset)
            jobForTable = self.bqJobs.getJobForTable(bqTable)
            ret.append(BqQueryBackedTableResource(query, bqTable,
                                                  int(mtime * 1000),
                                                  self.bqClient,
                                                  queryJob=jobForTable))
            ret.append(cacheDataSet(self.bqClient, bqTable,
                                    self.datasets))
        return ret


class BqViewFileLoader(FileLoader):
    def __init__(self, bqClient: Client, defaultDataset=None):
        self.bqClient = bqClient
        self.defaultDataset = defaultDataset
        self.datasets = {}

    def load(self, filePath):
        mtime = getmtime(filePath)
        bqTable = parseDatasetTable(filePath, self.defaultDataset,
                                    self.bqClient)

        ret = []
        with open(filePath) as f:
            query = f.read().format(dataset=self.defaultDataset)
            ret.append(BqViewBackedTableResource(query, bqTable,
                                                 int(mtime * 1000),
                                                 self.bqClient))
            ret.append(cacheDataSet(self.bqClient, bqTable,
                                    self.datasets))
            return ret


class BqDataFileLoader(FileLoader):
    def __init__(self, bqClient: Client, defaultDataset=None):
        self.bqClient = bqClient
        self.defaultDataset = defaultDataset
        self.datasets = {}

    def load(self, filePath):
        mtime = getmtime(filePath)
        schemaFilePath = filePath + ".schema"
        mtime_schema = getmtime(schemaFilePath)
        mtime = max([mtime, mtime_schema])
        bqTable = parseDatasetTable(filePath, self.defaultDataset,
                                    self.bqClient)

        with open(schemaFilePath) as schemaFile:
            schema = self.loadSchemaFromString(schemaFile.read().strip())

        ret = []
        ret.append(BqDataLoadTableResource(filePath, bqTable, schema,
                                           int(mtime * 1000),
                                           self.bqClient))
        ret.append(cacheDataSet(self.bqClient, bqTable,
                                self.datasets))
        return ret

    def loadSchemaFromString(self, schema: str):
        """ only support simple schema for i.e. not json just cmd line
        like format """

        try:
            ret = []
            for s in schema.split(","):
                (col, type) = s.split(":", maxsplit=2)
                ret.append(SchemaField(col, type))
            return ret
        except ValueError:
            raise Exception("Schema string should follow format "
                            "col:type," +
                            "col2:type...json schema not supported at "
                            "the moment")
