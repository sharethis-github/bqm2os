import optparse
from os import listdir
from os.path import isfile, join
import sys
import re

def main():
    queryFiles = listdir("bq")
    queryMap = {}
    for q in queryFiles:
        with open("bq/" + q) as file:
            queryMap[q] = file.read()
        
    queryDependencies = {}
    for q in queryMap.keys():
        queryDependencies[q] = set([])
        
    for q in queryMap.keys():
        tables = ["".join([" ", re.sub(".sql", "", q), x, " "]) for x in ["","_"]]
        for table in tables:
            for dependant in queryMap.keys():
                filtered = re.sub('[^0-9a-zA-Z\._]+', ' ', queryMap[dependant])
                if table in filtered:
                    queryDependencies[dependant].add(q)

    for k in sorted(queryDependencies): print (" ".join([k, str(queryDependencies[k])]))

if __name__ == "__main__":
    main()
