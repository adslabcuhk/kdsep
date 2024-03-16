#!/bin/bash

DIR=$(dirname $(realpath $0))

cd ${DIR}/KDSep
scripts/buildRelease.sh
#scripts/buildDebug.sh
cd ${DIR}/Benchmark/YCSB-C
make clean
make
