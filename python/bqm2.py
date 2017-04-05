#!/usr/bin/env python

import json
import logging
import optparse
from genericpath import isfile
from os import listdir
import re
from time import sleep

from google.cloud.bigquery.client import Client

from loader import DelegatingFileSuffixLoader, BqQueryFileLoader, \
    BqQueryTemplatingFileLoader, BqViewFileLoader, BqDataFileLoader, \
    TableType
from resource import BqJobs
from google.cloud import bigquery


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
                                                      TableType.TABLE,
                                                      **kwargs),
            view=BqQueryTemplatingFileLoader(bigquery.Client(),
                                             bqJobs,
                                             TableType.VIEW,
                                             **kwargs),
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
