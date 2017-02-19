import optparse
from os import listdir
import sys
import re
from rsrc.Rsrc import BqQueryBackedTableResource
from os.path import getmtime
from google.cloud import bigquery


class FileLoader:
    def __init__(self):
        pass

    def load(self, file):
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
                             " must have suffix from one of " +
                             str(self.loaders.keys())
                             )

        try:
            return self.loaders[suffixParts[-1]].load(file)
        except KeyError:
            raise ValueError("No loader associated with suffix: " +
                             suffixParts[-1])


class BqQueryFileLoader(FileLoader):

    def __init__(self, bqClient):
        self.bqClient = bqClient

    def load(self, filePath):
        mtime = getmtime(filePath)
        (dataset, table) = filePath.split("/")[-1].split(".")[:-1]

        with open(filePath) as f:
            return BqQueryBackedTableResource(f.read(),
                                              dataset, table,
                                              mtime, self.bqClient)

class DependencyBuilder:
    def __init__(self, loader):
        self.loader = loader

    def buildDepend(self, folders):
        """ folders arg is an array of strings which should point
        at folders containing reseource descriptions loadable by
        self.loader """
        queryMap = {}
        queryDependencies = {}
        for folder in folders:
            folder = re.sub("/$", "", folder)
            queryFiles = ["/".join([folder, x]) for x in listdir(folder)]
            for q in queryFiles:
                queryMap[q] = self.loader.load(q)

            for q in queryMap.keys():
                queryDependencies[q] = set([])

            for q in queryMap.keys():
                dependant = re.sub(".sql", "", q)
                tables = ["".join([" ", dependant, x, " "]) for x in ["", "_"]]
                for table in tables:
                    for dependant in queryMap.keys():
                        filtered = re.sub('[^0-9a-zA-Z\._]+', ' ',
                                          queryMap[dependant].query)
                        if table in filtered:
                            queryDependencies[dependant].add(q)

        return (queryMap, queryDependencies)

class DependencyExecutor:
    """ """
    def __init__(self, resources, dependencies):
        self.resources = resources
        self.dependencies = dependencies

    def execute(self):
        while len(dependencies):
            todel = set([])
            for n in dependencies.keys():
                if not len(dependencies[n]):
                    todel.add(n)
            for n in todel:
                print("executing: ", n, resources[n])
                resources[n].create()
                del dependencies[n]

            for n in dependencies.keys():
                torm = set([])
                for k in dependencies[n]:
                    if k not in dependencies:
                        torm.add(k)
                        dependencies[n] = dependencies[n] - torm


if __name__ == "__main__":

    builder = DependencyBuilder(DelegatingFileSuffixLoader(query=BqQueryFileLoader(bigquery.Client())))
    (resources, dependencies) = builder.buildDepend(sys.argv[1:])
    executor = DependencyExecutor(resources, dependencies)
    executor.execute()