import optparse
from genericpath import isfile
from os import listdir
import re
from time import sleep

import time

from google.cloud.bigquery.client import Client
from google.cloud.bigquery.schema import SchemaField
from googleapiclient.http import MediaFileUpload

from rsrc.Rsrc import BqQueryBackedTableResource, \
    BqViewBackedTableResource, BqJobs, BqDatasetBackedResource, \
    BqDataLoadTableResource
from os.path import getmtime
from google.cloud import bigquery


class FileLoader:
    def __init__(self):
        pass


    def load(self, file):
        pass


    def handles(self, file):
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


def parseDatasetTable(filePath, defaultDataset=None):
    tokens = filePath.split("/")[-1].split(".")
    if len(tokens) == 3:
        return (tokens[0], tokens[1])
    elif len(tokens) == 2:  # use default dataset
        if not defaultDataset:
            raise ValueError("Must specify a default dataset")
        return (defaultDataset, tokens[0])
    elif len(tokens) > 3:
        raise ValueError("Invalid filename: " + filePath + ". File names "
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

class BqQueryFileLoader(FileLoader):
    def __init__(self, bqClient: Client, defaultDataset=None):
        self.bqClient = bqClient
        self.defaultDataset = defaultDataset


    def load(self, filePath):
        mtime = getmtime(filePath)
        (dataset, table) = parseDatasetTable(filePath, self.defaultDataset)

        with open(filePath) as f:
            query = f.read().format(dataset=self.defaultDataset)
            bqTable = self.bqClient.dataset(dataset).table(table)
            return BqQueryBackedTableResource(query, bqTable,
                                              int(mtime * 1000),
                                              self.bqClient, queryJob=None)


class BqDatasetFileLoader(FileLoader):
    def __init__(self, bqClient, defaultDataset=None):
        self.bqClient = bqClient
        self.defaultDataset = defaultDataset


    def load(self, filePath):
        mtime = getmtime(filePath)
        dataset = parseDataset(filePath, self.defaultDataset)
        return BqDatasetBackedResource(dataset,
                                       int(mtime * 1000),
                                       self.bqClient)


class BqViewFileLoader(FileLoader):
    def __init__(self, bqClient: Client, defaultDataset=None):
        self.bqClient = bqClient
        self.defaultDataset = defaultDataset

    def load(self, filePath):
        mtime = getmtime(filePath)
        (dataset, table) = parseDatasetTable(filePath, self.defaultDataset)

        with open(filePath) as f:
            query = f.read().format(dataset=self.defaultDataset)
            bqTable = self.bqClient.dataset(dataset).table(table)
            return BqViewBackedTableResource(query, bqTable,
                                             int(mtime * 1000),
                                             self.bqClient)


class BqDataFileLoader(FileLoader):
    def __init__(self, bqClient: Client, defaultDataset=None):
        self.bqClient = bqClient
        self.defaultDataset = defaultDataset

    def load(self, filePath):
        mtime = getmtime(filePath)
        (dataset, table) = parseDatasetTable(filePath, self.defaultDataset)

        with open(filePath+".schema") as schemaFile:
            schema = self.loadSchemaFromString(schemaFile.read().strip())

        bqTable = self.bqClient.dataset(dataset).table(table)
        return BqDataLoadTableResource(filePath, bqTable, schema,
                                       int(mtime * 1000), self.bqClient)
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

class DependencyBuilder:
    def __init__(self, loader):
        self.loader = loader


    def buildDepend(self, folders):
        """ folders arg is an array of strings which should point
        at folders containing reseource descriptions loadable by
        self.loader """
        resources = {}
        resourceDependencies = {}
        for folder in folders:
            folder = re.sub("/$", "", folder)
            for name in listdir(folder):
                file = "/".join([folder, name])
                if isfile(file) and self.loader.handles(file):
                    rsrc = self.loader.load(file)
                    resources[rsrc.key()] = rsrc

            for rsrc in resources.values():
                resourceDependencies[rsrc.key()] = set([])

            for rsrc in resources.values():
                for osrc in resources.values():
                    if rsrc.dependsOn(osrc):
                        resourceDependencies[rsrc.key()].add(osrc.key())

        return (resources, resourceDependencies)


class DependencyExecutor:
    """ """

    def __init__(self, resources, dependencies):
        self.resources = resources
        self.dependencies = dependencies


    def show(self):
        for (k, s) in sorted(self.dependencies.items()):
            if len(s):
                msg = " ".join([x for x in sorted(s)])
            else:
                msg = "nothing"

            print (k, "depends on", msg)

        while len(dependencies):
            todel = set([])
            for n in sorted(dependencies.keys()):
                if not len(dependencies[n]):
                    todel.add(n)
                    print("would execute", n)
                    del dependencies[n]

            for n in sorted(dependencies.keys()):
                torm = set([])
                for k in dependencies[n]:
                    if k not in dependencies:
                        torm.add(k)

                dependencies[n] = dependencies[n] - torm


    def execute(self, checkFrequency=10):
        while len(dependencies):
            todel = set([])
            for n in sorted(dependencies.keys()):
                if not len(dependencies[n]):
                    todel.add(n)

            for n in sorted(todel):
                if (resources[n].isRunning()):
                    print("not executing: because we're running already",
                          n)
                    continue
                if not resources[n].exists():
                    print("executing: because it doesn't exist ", n)
                    resources[n].create()
                elif resources[n].definitionTime() > resources[
                    n].updateTime():
                    print("executing: because its definition is newer "
                          "than last created ",
                          n, resources[n])
                    if resources[n].hasFailed():
                        print ("job failed before")
                    resources[n].create()
                else:
                    print (resources[n], " resource exists and is up to "
                                         "date")
                    del dependencies[n]

            for n in sorted(dependencies.keys()):
                torm = set([])
                for k in dependencies[n]:
                    if k not in dependencies:
                        torm.add(k)

                dependencies[n] = dependencies[n] - torm
            if len(dependencies):
                sleep(checkFrequency)

if __name__ == "__main__":
    parser = optparse.OptionParser("[options] folder[ folder2[...]]")
    parser.add_option("--execute", dest="execute",
                      action="store_true", default=False,
                      help="Execute the dependencies found in the resources")
    parser.add_option("--show", dest="show",
                      action="store_true", default=False,
                      help="Show the dependency tree")
    parser.add_option("--showJobs", dest="showJobs",
                      action="store_true", default=False,
                      help="Show the jobs")
    parser.add_option("--defaultDataset", dest="defaultDataset",
                      help="The default dataset which will be used if "
                           "file definitions don't specify one")
    parser.add_option("--checkFrequency", dest="checkFrequency", type=int,
                      default=10,
                      help="The loop interval between dependency tree"
                           " evaluation runs")

    (options, args) = parser.parse_args()

    kwargs = {"defaultDataset": options.defaultDataset}

    builder = DependencyBuilder(
        DelegatingFileSuffixLoader(
            query=BqQueryFileLoader(bigquery.Client(), **kwargs),
            view=BqViewFileLoader(bigquery.Client(), **kwargs),
            dataset=BqDatasetFileLoader(bigquery.Client(), **kwargs),
            localdata=BqDataFileLoader(bigquery.Client(), **kwargs)

    ))

    (resources, dependencies) = builder.buildDepend(args)
    executor = DependencyExecutor(resources, dependencies)
    if options.execute:
        executor.execute()
    elif options.show:
        executor.show()
    elif options.showJobs:
        for j in BqJobs(bigquery.Client()).jobs():
            print(j)
    else:
        parser.print_help()
