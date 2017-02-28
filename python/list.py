import optparse
from genericpath import isfile
from os import listdir
import re
from time import sleep

from rsrc.Rsrc import BqQueryBackedTableResource, BqViewBackedTableResource, BqJobs
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
                             " must have suffix from one of " +
                             str(self.loaders.keys())
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
            raise ValueError("Invalid file for loading: " + file + ". No suffix")



class BqQueryFileLoader(FileLoader):

    def __init__(self, bqClient):
        self.bqClient = bqClient

    def load(self, filePath):
        mtime = getmtime(filePath)
        (dataset, table) = filePath.split("/")[-1].split(".")[:-1]

        with open(filePath) as f:
            return BqQueryBackedTableResource(f.read(),
                                              dataset, table,
                                              int(mtime*1000), self.bqClient)

class BqViewFileLoader(FileLoader):

    def __init__(self, bqClient):
        self.bqClient = bqClient

    def load(self, filePath):
        mtime = getmtime(filePath)
        (dataset, table) = filePath.split("/")[-1].split(".")[:-1]

        with open(filePath) as f:
            return BqViewBackedTableResource(f.read(),
                                              dataset, table,
                                              int(mtime*1000), self.bqClient)

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
            queryFile = []
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
        print(str(self.dependencies))

    def execute(self):
        while len(dependencies):
            todel = set([])
            for n in dependencies.keys():
                if not len(dependencies[n]):
                    todel.add(n)
            for n in todel:
                if (resources[n].isRunning()):
                    print("not executing: because we're running already", n)
                    continue
                if not resources[n].exists():
                    print("executing: because it doesn't exist ", n)
                    resources[n].create()
                elif resources[n].definitionTime() > resources[n].updateTime():
                    print("executing: because its definition is newer than last created ",
                          n, resources[n])

                ## can't delete dependency if we failed!
                while resources[n].isRunning():
                    print ("waiting for ",  n, "to finish")
                    sleep(10)


                del dependencies[n]

            for n in dependencies.keys():
                torm = set([])
                for k in dependencies[n]:
                    if k not in dependencies:
                        torm.add(k)
                        dependencies[n] = dependencies[n] - torm


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

    (options, args) = parser.parse_args()

    builder = DependencyBuilder(
        DelegatingFileSuffixLoader(
            query=BqQueryFileLoader(bigquery.Client()),
            view=BqViewFileLoader(bigquery.Client())))
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