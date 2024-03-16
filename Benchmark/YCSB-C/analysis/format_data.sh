#!/bin/bash

echo "readRatio valueSize scheme throughput readLatency mergeLatency"

## For Wiki
for file in $*; do
    thpt=`grep "rocksdb.*workload" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t;}'` 
    if [[ "$thpt" == "0" ]]; then
        loadtime=`grep "Load time" $file | awk 'BEGIN {t=0;} {t=$(NF-1);} END {print t;}'`
        records=`grep "Loading records" $file | awk 'BEGIN {t=0;} {t=$NF;} END {print t;}'`
        thpt=`echo "$loadtime $records" | awk '{print $2/($1+0.000001);}'`
    fi

    readRatio=`echo $file | awk -F"_|/|-" '{for (i=1; i<NF;i++) {if ($i == "Rd" || $i == "Read") print $(i+1);}}'`
    flength=`echo $file | awk -F"_|/|-" '{for (i=1; i<NF;i++) {if ($i ~ "fl") print $i "0";}}' | sed "s/fl//g"`
    scheme=`echo $file | awk -F"_|/|-" '{for (i=1; i<NF;i++) {if ($i ~ "Result") print $(i+1);}}' | sed "s/fl//g"`
    readLatency=`grep "per read" $file | awk '{print $(NF-1) * 1000;}'`
    mergeLatency=`grep "per update" $file | awk '{print $(NF-1) * 1000;}'`
    if [[ $(echo "$mergeLatency" | grep "nan" | wc -l) -ne 0 ]]; then
        mergeLatency=`grep "per R-M-W" $file | awk '{print $(NF-1) * 1000;}'`
    fi

    echo $readRatio $flength $scheme $thpt $readLatency $mergeLatency
done

## For motivation

#listfile="$(dirname $0)/list.txt"

echo "separation      readRatio       dataClass      dataOperation   operationTime"
for file in $*; do
    readRatio=`echo $file | awk -F"_|/|-" '{for (i=1; i<NF;i++) {if ($i == "Rd" || $i == "Read") print $(i+1);}}'`
    approach=`echo $file | awk -F"_|/|-" '{for (i=1; i<NF;i++) {if ($i == "Ud" || $i == "Update") print "Merge"; else if ($i == "RMW") print "GetPut";}}'`
    flength=`echo $file | awk -F"_|/|-" '{for (i=1; i<NF;i++) {if ($i ~ "fl") print $i "0";}}' | sed "s/fl//g"`
    scheme=`echo $file | awk -F"_|/|-" '{for (i=1; i<NF;i++) {if ($i ~ "Result") print $(i+1);}}' | sed "s/fl//g"`
    readLatency=`grep "per read" $file | awk '{print $(NF-1) * 1000;}'`
    mergeLatency=`grep "per update" $file | awk '{print $(NF-1) * 1000;}'`
    if [[ $(echo "$mergeLatency" | grep "nan" | wc -l) -ne 0 ]]; then
        mergeLatency=`grep "per R-M-W" $file | awk '{print $(NF-1) * 1000;}'`
    fi

    echo $scheme $readRatio $approach read $readLatency 
    echo $scheme $readRatio $approach update $mergeLatency 
done

echo "separation      metric  dataClass       dataOperation   operationTime"
for file in $*; do
    readRatio=`echo $file | awk -F"_|/|-" '{for (i=1; i<NF;i++) {if ($i == "Rd" || $i == "Read") print $(i+1);}}'`
    approach=`echo $file | awk -F"_|/|-" '{for (i=1; i<NF;i++) {if ($i == "Ud" || $i == "Update") print "Merge"; else if ($i == "RMW") print "GetPut";}}'`
    flength=`echo $file | awk -F"_|/|-" '{for (i=1; i<NF;i++) {if ($i ~ "fl") print $i "0";}}' | sed "s/fl//g"`
    scheme=`echo $file | awk -F"_|/|-" '{for (i=1; i<NF;i++) {if ($i ~ "Result") print $(i+1);}}' | sed "s/fl//g"`
    readLatency=`grep "per read" $file | awk '{print $(NF-1) * 1000;}'`
    mergeLatency=`grep "per update" $file | awk '{print $(NF-1) * 1000;}'`
    if [[ $(echo "$mergeLatency" | grep "nan" | wc -l) -ne 0 ]]; then
        mergeLatency=`grep "per R-M-W" $file | awk '{print $(NF-1) * 1000;}'`
    fi

    echo $scheme Avg $approach Read $readLatency 
    echo $scheme Avg $approach RMW $mergeLatency 
done


## For Wiki - Total time

echo "readRatio scheme readTime rmwTime"
for file in $*; do
    readRatio=`echo $file | awk -F"_|/|-" '{for (i=1; i<NF;i++) {if ($i == "Rd" || $i == "Read") print $(i+1);}}'`
    scheme=`echo $file | awk -F"_|/|-" '{for (i=1; i<NF;i++) {if ($i ~ "Result") print $(i+1);}}' | sed "s/fl//g"`
    readTime=`grep "Total read time" $file | awk '{print $(NF-1);}'`
    rmwTime=`grep "Total update time" $file | awk '{print $(NF-1);}'`
    if [[ "$rmwTime" == "0" ]]; then
        rmwTime=`grep "Total R-M-W time" $file | awk '{print $(NF-1);}'`
    fi

    echo $readRatio $scheme $readTime $rmwTime
done
