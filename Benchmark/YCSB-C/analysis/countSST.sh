#!/bin/bash

DIR="$(realpath $(dirname $0))/../"
cd $DIR

if [ $1 == "clean" ]; then
    rm -rf countSSTInfo
    rm -rf countSSTInfoLevel
#    rm -rf countSST/*log
    exit
fi

if [ $1 == "make" ]; then
    if [ ! -f $DIR/countSSTInfo ]; then
        g++ -o countSSTInfo countSSTInfo.cpp
    fi

    if [ ! -f countSSTInfoLevel ]; then
        g++ -o countSSTInfoLevel countSSTInfoLevel.cpp
    fi
    exit
fi

targetAnalysisPath=$1
runningCount=$2

if [[ ! -d $runningCount/tmp ]]; then
    mkdir -p $runningCount/tmp/
fi

manifestFile=$(ls $targetAnalysisPath/MANIFEST-*)
../../RocksDB/ldb manifest_dump --path=$manifestFile > ${runningCount}/manifest.log

sstablesSet=$(ls $targetAnalysisPath/*.sst)

echo "Find SSTable files: "
echo $sstablesSet

for SSTable in ${sstablesSet[@]}; do
    SSTFileName=${SSTable:0-10:6}
    ../../RocksDB/sst_dump --file=$SSTable --output_hex --command=scan >> ${runningCount}/tmp/$SSTFileName.log
    echo "SST ID = "$SSTFileName >> ${runningCount}/SSTablesAnalysis.log
    ./countSSTInfo ${runningCount}/tmp/$SSTFileName.log >> ${runningCount}/SSTablesAnalysis.log
    rm -rf ${runningCount}/tmp/$SSTFileName.log
done

./countSSTInfoLevel ${runningCount}/manifest.log ${runningCount}/SSTablesAnalysis.log >${runningCount}/levelBasedCount.log
