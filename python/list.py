import optparse
from os import listdir
import sys
import re
from rsrc.Rsrc import BqQueryBackedTableResource
from os.path import getmtime
from google.cloud import bigquery

def depTree(folder, bqClient):
    queryFiles = listdir(folder)
    queryMap = {}
    for q in queryFiles:
        fullName = "/".join([folder, q]);
        mtime = getmtime(fullName)
        with open(fullName) as f:
            (dataset, table) = f.name.split("/")[-1].split(".")
            queryMap[q] = \
                BqQueryBackedTableResource(f.read(), dataset, table, mtime, bqClient)

    queryDependencies = {}
    for q in queryMap.keys():
        queryDependencies[q] = set([])

    for q in queryMap.keys():
        dependant = re.sub(".sql", "", q)
        tables = ["".join([" ", dependant, x, " "]) for x in ["", "_"]]
        for table in tables:
            for dependant in queryMap.keys():
                filtered = re.sub('[^0-9a-zA-Z\._]+', ' ', queryMap[dependant].query)
                if table in filtered:
                    queryDependencies[dependant].add(q)

    return (queryMap, queryDependencies)

if __name__ == "__main__":

    (resources, dependencies) = depTree(sys.argv[1], bigquery.Client())
    print(dependencies)

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

    print(dependencies)
