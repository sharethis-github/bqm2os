#!/usr/bin/env python

import json
import logging
import optparse
from genericpath import isfile
from os import listdir
import re
from time import sleep

from google.cloud.bigquery.client import Client
from google.cloud.bigquery.schema import SchemaField
from google.cloud.bigquery.table import Table

import tmplhelper
from resource import BqQueryBackedTableResource, \
    BqViewBackedTableResource, BqJobs, BqDatasetBackedResource, \
    BqDataLoadTableResource, _buildDataSetKey_, \
    Resource
from os.path import getmtime
from google.cloud import bigquery

from tmplhelper import explodeTemplate, evalTmplRecurse


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

    def __init__(self, bqClient: Client, bqJobs: BqJobs,
                 defaultDataset=None):
        """

        :param bqClient: The big query client to use
        :param bqJobs: An initialized BqJobs
        :param defaultDataset: A default dataset to use in templates
        """
        self.bqClient = bqClient
        self.defaultDataset = defaultDataset
        self.bqJobs = bqJobs
        self.datasets = {}

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

    def load(self, filePath):
        mtime = getmtime(filePath)
        ret = []
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
                # print ("formatting: ", v)
                dataset = v['dataset']

                try:
                    needed = tmplhelper.keysOfTemplate(template)
                    if not needed.issubset(v.keys()):
                        missing = str(needed - v.keys())
                        raise Exception("Please define values for " +
                                        missing + " in a file: ",
                                        filePath + ".vars")
                    query = template.format(**v)
                    # print("formatting query: ", query)
                    table = v['table']

                    bqTable = self.bqClient.dataset(dataset).table(table)
                    jT = self.bqJobs.getJobForTable(bqTable)
                    ret.append(BqQueryBackedTableResource(query, bqTable,
                                                          int(
                                                              mtime * 1000),
                                                          self.bqClient,
                                                          queryJob=jT))

                    ret.append(cacheDataSet(self.bqClient, bqTable,
                                            self.datasets))

                except ValueError:
                    raise Exception("Problem in file: " + filePath +
                                    "with template")
        return ret

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
            print("template file: ", filePath, " not found")
            return [{}]


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


class DependencyBuilder:
    """
    Dependency builder loads resources from the folders specified.
    """

    def __init__(self, loader):
        self.loader = loader

    def buildDepend(self, folders) -> tuple:
        """ folders arg is an array of strings which should point
        at folders containing resource descriptions loadable by
        self.loader """
        resources = {}
        resourceDependencies = {}
        for folder in folders:
            folder = re.sub("/$", "", folder)
            for name in listdir(folder):
                file = "/".join([folder, name])
                if isfile(file) and self.loader.handles(file):
                    l = self.loader.load(file)
                    for rsrc in self.loader.load(file):
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

    def dump(self, folder):
        """ dump expanded templates to a folder """
        for (k, s) in sorted(self.dependencies.items()):
            if len(s):
                msg = " ".join([x for x in sorted(s)])
            else:
                msg = "nothing"

            print(k, "depends on", msg)

        while len(dependencies):
            todel = set([])
            for n in sorted(dependencies.keys()):
                if not len(dependencies[n]):
                    todel.add(n)
                    with open(folder + "/" + resources[n].key() +
                              ".debug", "w") as f:
                        f.write(resources[n].dump())
                        f.close()
                    del dependencies[n]

            for n in sorted(dependencies.keys()):
                torm = set([])
                for k in dependencies[n]:
                    if k not in dependencies:
                        torm.add(k)

                dependencies[n] = dependencies[n] - torm

    def show(self):
        for (k, s) in sorted(self.dependencies.items()):
            if len(s):
                msg = " ".join([x for x in sorted(s)])
            else:
                msg = "nothing"

            print(k, "depends on", msg)

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

            """ flag to capture if anything was running.  If so,
            we will pause before looping again """
            running = False
            for n in sorted(todel):
                if (resources[n].isRunning()):
                    print(resources[n], "already running")
                    running = True
                    continue
                if not resources[n].exists():
                    print("executing: because it doesn't exist ", n)
                    resources[n].create()
                    running = True
                elif resources[n].definitionTime() \
                        > resources[n].updateTime():
                    print("executing: because its definition is newer "
                          "than last created ",
                          n, resources[n])
                    resources[n].create()
                    running = True
                else:
                    print(resources[n], " resource exists and is up to "
                                        "date")
                    del dependencies[n]

            for n in sorted(dependencies.keys()):
                torm = set([])
                for k in dependencies[n]:
                    if k in torm:
                        continue
                    if k not in dependencies:
                        kDateTime = resources[k].updateTime()
                        if kDateTime > resources[n].definitionTime():
                            resources[n].defTime = kDateTime
                        torm.add(k)

                dependencies[n] = dependencies[n] - torm

            if len(dependencies):
                if running:
                    sleep(checkFrequency)


if __name__ == "__main__":
    parser = optparse.OptionParser("[options] folder[ folder2[...]]")
    parser.add_option("--execute", dest="execute",
                      action="store_true", default=False,
                      help="Execute the dependencies found in the resources")
    parser.add_option("--show", dest="show",
                      action="store_true", default=False,
                      help="Show the dependency tree")
    parser.add_option("--dumpToFolder", dest="dumpToFolder",
                      default=None,
                      help="Dump expanded templates to disk to "
                           "folder/file using the key of resource and "
                           "content of template")
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

    FORMAT = '%(asctime)-15s %(clientip)s %(user)-8s %(message)s'
    logging.basicConfig(format=FORMAT)

    kwargs = {"defaultDataset": options.defaultDataset}

    client = Client()
    bqJobs = BqJobs(client)
    if options.execute:
        bqJobs.loadTableJobs()

    builder = DependencyBuilder(
        DelegatingFileSuffixLoader(
            query=BqQueryFileLoader(bigquery.Client(), bqJobs, **kwargs),
            querytemplate=BqQueryTemplatingFileLoader(bigquery.Client(),
                                                      bqJobs,
                                                      **kwargs),
            view=BqViewFileLoader(bigquery.Client(), **kwargs),
            localdata=BqDataFileLoader(bigquery.Client(), **kwargs)))

    (resources, dependencies) = builder.buildDepend(args)
    executor = DependencyExecutor(resources, dependencies)
    if options.execute:
        executor.execute(checkFrequency=options.checkFrequency)
    elif options.show:
        executor.show()
    elif options.dumpToFolder:
        executor.dump(options.dumpToFolder)
    elif options.showJobs:
        for j in BqJobs(bigquery.Client()).jobs():
            if j.state in set(['RUNNING', 'PENDING']):
                print(j.name, j.state, j.errors)
    else:
        parser.print_help()
