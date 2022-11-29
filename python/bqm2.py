#!/usr/bin/env python

import json
import logging
import optparse
from genericpath import isfile
from os import listdir
import re
from time import sleep

from collections import defaultdict

import sys
from google.cloud import storage
from google.cloud.bigquery.client import Client
from google.cloud.bigquery.job import QueryJobConfig

from loader import DelegatingFileSuffixLoader, \
    BqQueryTemplatingFileLoader, BqDataFileLoader, \
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

    def __init__(self, resources, dependencies, maxRetry=2):
        self.resources = resources
        self.dependencies = dependencies
        self.maxRetry = maxRetry

    def dump(self, folder):
        """ dump expanded templates to a folder """
        for (k, s) in sorted(self.dependencies.items()):
            if len(s):
                msg = " ".join([x for x in sorted(s)])
            else:
                msg = "nothing"

            print(k, "depends on", msg)

        while len(self.dependencies):
            todel = set([])
            for n in sorted(self.dependencies.keys()):
                if not len(self.dependencies[n]):
                    todel.add(n)
                    toWrite = folder + "/" \
                        + self.resources[n].key().replace("/", "_") \
                        + ".debug"
                    with open(toWrite, "w") as f:
                        f.write(self.resources[n].dump())
                        f.close()
                    del self.dependencies[n]

            for n in sorted(self.dependencies.keys()):
                torm = set([])
                for k in self.dependencies[n]:
                    if k not in self.dependencies:
                        torm.add(k)

                    self.dependencies[n] = self.dependencies[n] - torm

    def show(self):
        for (k, s) in sorted(self.dependencies.items()):
            if len(s):
                msg = " ".join([x for x in sorted(s)])
            else:
                msg = "nothing"

            print(k, "depends on", msg)

        while len(self.dependencies):
            todel = set([])
            for n in sorted(self.dependencies.keys()):
                if not len(self.dependencies[n]):
                    todel.add(n)
                    print("would execute", n)
                    del self.dependencies[n]

            for n in sorted(self.dependencies.keys()):
                torm = set([])
                for k in self.dependencies[n]:
                    if k not in self.dependencies:
                        torm.add(k)

                    self.dependencies[n] = self.dependencies[n] - torm

    def dotml(self):
        print("digraph g {\n")
        for (k, s) in sorted(self.dependencies.items()):
            if not len(s):
                continue
            for n in s:
                # if len(str(n).split(".")) == 1:
                #     continue
                print("".join(['"', k, '"']), "->", "".join(['"', n, '"']))
        print("}")

    def handleRetries(self, retries, rsrcKey):
        retries[rsrcKey] -= 1
        if retries[rsrcKey] < 0:
            raise Exception("Maximum retries hit for resource",
                            rsrcKey)

    def execute(self, checkFrequency=10, maxConcurrent=10):
        running = set([])
        retries = defaultdict(lambda: self.maxRetry)

        # def update times is a dict of maximum of the update
        # times of the dependencies of a resource

        depUpdateTimes = defaultdict(lambda: 0)
        while len(self.dependencies):
            todel = set([])
            for n in sorted(self.dependencies.keys()):
                if not len(self.dependencies[n]):
                    todel.add(n)

            """ flag to capture if anything was running.  If so,
            we will pause before looping again """
            for n in sorted(todel):
                if (self.resources[n].isRunning()):
                    print(self.resources[n], "already running")
                    running.add(n)
                    continue
                else:
                    running.discard(n)
                if not self.resources[n].exists():
                    if len(running) >= maxConcurrent:
                        print("max concurrent running already")
                        continue
                    self.handleRetries(retries, n)
                    print("executing: because it doesn't exist ", n)
                    self.resources[n].create()
                    if (self.resources[n].isRunning()):
                        running.add(n)
                elif self.resources[n].shouldUpdate():
                    if len(running) >= maxConcurrent:
                        print("max concurrent running already")
                        continue
                    self.handleRetries(retries, n)
                    print("executing: because our definition has changed",
                          n, self.resources[n])
                    self.resources[n].create()
                    running.add(n)
                elif self.resources[n].updateTime() < depUpdateTimes[n]:
                    if len(running) >= maxConcurrent:
                        print("max concurrent running already")
                        continue
                    self.handleRetries(retries, n)
                    print("executing: because our dependencies have "
                          "changed since we last ran",
                          n, self.resources[n])
                    self.resources[n].create()
                    running.add(n)
                else:
                    print(self.resources[n],
                          " resource exists and is up to date")
                    del self.dependencies[n]
                    if n in running:
                        running.remove(n)

                if len(running) >= maxConcurrent:
                    print("max concurrent running already")
                    break

            # n-squared
            for n in sorted(self.dependencies.keys()):
                torm = set([])
                for k in self.dependencies[n]:
                    if k in torm:
                        continue
                    if k not in self.dependencies:
                        kDateTime = self.resources[k].updateTime()
                        depUpdateTimes[n] = max(depUpdateTimes[n], kDateTime)
                        torm.add(k)

                self.dependencies[n] = self.dependencies[n] - torm

            if len(self.dependencies):
                sleep(checkFrequency)


if __name__ == "__main__":
    parser = optparse.OptionParser("[options] folder[ folder2[...]]")
    parser.add_option("--execute", dest="execute",
                      action="store_true", default=False,
                      help="'execute' mode.  Accepts a list of folders "
                           "- folder[ folder2[...]]."
                      "Renders the templates found in those folders "
                      "and executes them in proper order "
                           "of their dependencies")
    parser.add_option("--dotml", dest="dotml",
                      action="store_true", default=False,
                      help="Generate dot ml graph of dag of execution")
    parser.add_option("--show", dest="show",
                      action="store_true", default=False,
                      help="Show the dependency tree")
    parser.add_option("--dumpToFolder", dest="dumpToFolder",
                      default=None,
                      help="Dump expanded templates to disk to the "
                           "folder as files using the key of resource and "
                           "content of template.  ")
    parser.add_option("--showJobs", dest="showJobs",
                      action="store_true", default=False,
                      help="Show the jobs")
    parser.add_option("--defaultDataset", dest="defaultDataset",
                      help="The default dataset which will be used if "
                           "file definitions don't specify one.  This "
                           "will be automatically created during "
                           "'execute' mode")
    parser.add_option("--maxConcurrent", dest="maxConcurrent", type=int,
                      default=10, help="The maximum number of bq or "
                                       "other jobs to run in parallel.")
    parser.add_option("--defaultProject", dest="defaultProject",
                      help="The default project which will be used if "
                           "file definitions don't specify one")
    parser.add_option("--checkFrequency", dest="checkFrequency", type=int,
                      default=10,
                      help="The loop interval between dependency tree"
                           " evaluation runs")

    parser.add_option("--maxRetry", dest="maxRetry", type=int,
                      default=2,
                      help="Relevant to 'execute' mode. The maximum "
                           "retries for any single resource "
                           "creation. Once this number is hit, "
                           "the program will exit non-zero")

    parser.add_option("--varsFile", dest="varsFile", type=str,
                      help="A json file whose data can be refered to in "
                           "view and query templates.  Must be a simple "
                           "dictionary whose values are string, integers, "
                           "or arrays of strings and integers")

    parser.add_option("--bqClientLocation", type=str,
                      help="The location where datasets will be "
                           "created. i.e. us-east1, us-central1, etc",
                      default="US")

    (options, args) = parser.parse_args()

    FORMAT = '%(asctime)-15s %(clientip)s %(user)-8s %(message)s'
    logging.basicConfig(format=FORMAT)

    additional_args = {'location': options.bqClientLocation}
    kwargs = {"dataset": options.defaultDataset}
    if options.varsFile:
        with open(options.varsFile) as f:
            varJson = json.load(f)
            for (k, v) in varJson.items():
                kwargs[k] = v

    client = Client(**additional_args)
    if options.defaultProject:
        client = Client(options.defaultProject, **additional_args)
        kwargs["project"] = options.defaultProject
    else:
        kwargs["project"] = client.project

    loadClient = Client(project=kwargs["project"], **additional_args)
    gcsClient = storage.Client(project=kwargs["project"])

    bqJobs = BqJobs(client)
    if options.execute:
        bqJobs.loadTableJobs()

    builder = DependencyBuilder(
        DelegatingFileSuffixLoader(
            uniontable=BqQueryTemplatingFileLoader(client, gcsClient,
                                                   bqJobs,
                                                   TableType.UNION_TABLE,
                                                   kwargs),
            unionview=BqQueryTemplatingFileLoader(client, gcsClient,
                                                  bqJobs,
                                                  TableType.UNION_VIEW,
                                                  kwargs),
            querytemplate=BqQueryTemplatingFileLoader(client, gcsClient,
                                                      bqJobs,
                                                      TableType.TABLE,
                                                      kwargs),
            view=BqQueryTemplatingFileLoader(client, gcsClient,
                                             bqJobs,
                                             TableType.VIEW,
                                             kwargs),
            localdata=BqDataFileLoader(loadClient,
                                       kwargs['dataset'],
                                       kwargs['project'],
                                       bqJobs),
            gcsdata=BqQueryTemplatingFileLoader(client, gcsClient,
                                                bqJobs,
                                                TableType.TABLE_GCS_LOAD,
                                                kwargs),
            bashtemplate=BqQueryTemplatingFileLoader(loadClient, gcsClient,
                                                     bqJobs,
                                                     TableType.BASH_TABLE,
                                                     kwargs),
            externaltable=BqQueryTemplatingFileLoader(loadClient, gcsClient,
                                                      bqJobs,
                                                      TableType.EXTERNAL_TABLE,
                                                      kwargs))
    )

    (resources, dependencies) = builder.buildDepend(args)
    executor = DependencyExecutor(resources, dependencies,
                                  maxRetry=options.maxRetry)
    if options.execute:
        executor.execute(checkFrequency=options.checkFrequency,
                         maxConcurrent=options.maxConcurrent)
    elif options.show:
        executor.show()
    elif options.dotml:
        executor.dotml()
    elif options.dumpToFolder:
        executor.dump(options.dumpToFolder)
    elif options.showJobs:
        for j in BqJobs(bigquery.Client(**additional_args)).jobs():
            if j.state in set(['RUNNING', 'PENDING']):
                print(j.name, j.state, j.errors)
    else:
        parser.print_help()
