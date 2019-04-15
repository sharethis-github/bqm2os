#!/bin/bash


current_commit=$(git rev-parse HEAD | cut -c 1-10)
imagename=bqm2
registry=${registry:-docker.io/stops}
registryimage=${registry}/${imagename}
