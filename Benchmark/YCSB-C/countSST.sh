#!/bin/bash

if [ $1 == "clean" ]; then
    rm -rf countSSTInfo
    rm -rf countSSTInfoLevel
    rm -rf *log
    exit
fi

if [ $1 == "make" ]; then
    if [ ! -f countSSTInfo ]; then
        g++ -o countSSTInfo countSSTInfo.cpp
    fi

    if [ ! -f countSSTInfoLevel ]; then
        g++ -o countSSTInfoLevel countSSTInfoLevel.cpp
    fi
    exit
fi

targetAnalysisPath=$1

sstablesSet=$(ls $targetAnalysisPath/*.sst)

echo "Find SSTable files: "
echo $sstablesSet

if [[ ! -f sst_dump ]]; then
    cp ../RocksDB/sst_dump .
    if [[ ! -f sst_dump ]]; then
        echo "Can't find sst_dump"
        exit
    fi
fi

if [[ ! -f ldb ]]; then
    cp ../RocksDB/ldb .
    if [[ ! -f ldb ]]; then
        echo "Can't find ldb"
        exit
    fi
fi

for SSTable in ${sstablesSet[@]}; do
    SSTFileName=${SSTable:0-10:6}
    ./sst_dump --file=$SSTable --output_hex --command=scan >>$SSTFileName.log
    echo "SST ID = "$SSTFileName >>SSTablesAnalysis.log
    ./countSSTInfo $SSTFileName.log >>SSTablesAnalysis.log
    rm -rf $SSTFileName.log
done

manifestFile=$(ls $targetAnalysisPath/MANIFEST-*)

./ldb manifest_dump --path=$manifestFile >manifest.log
./countSSTInfoLevel manifest.log SSTablesAnalysis.log >levelBasedCount.log
